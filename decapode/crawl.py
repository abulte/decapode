import json
import logging
import os
import sys
import time

from collections import defaultdict
from datetime import datetime, timedelta
from urllib.parse import urlparse

import aiohttp
import asyncio

from humanfriendly import parse_timespan

from decapode import config
from decapode import context

log = logging.getLogger("decapode")

results = defaultdict(int)

STATUS_OK = 'ok'
STATUS_TIMEOUT = 'timeout'
STATUS_ERROR = 'error'


async def insert_check(data):
    if 'headers' in data:
        data['headers'] = json.dumps(data['headers'])
    columns = ','.join(data.keys())
    # $1, $2...
    placeholders = ','.join([f'${x + 1}' for x in range(len(data.values()))])
    q = f'''
        INSERT INTO checks ({columns})
        VALUES ({placeholders})
        RETURNING id
    '''
    pool = await context.pool()
    async with pool.acquire() as connection:
        last_check = await connection.fetchrow(q, *data.values())
        q = '''UPDATE catalog SET last_check = $1 WHERE url = $2'''
        await connection.execute(q, last_check['id'], data['url'])
    return last_check['id']


async def is_backoff(domain):
    no_backoff = [f"'{d}'" for d in config.NO_BACKOFF_DOMAINS]
    no_backoff = f"({','.join(no_backoff)})"
    since = datetime.utcnow() - timedelta(seconds=config.BACKOFF_PERIOD)
    pool = await context.pool()
    async with pool.acquire() as connection:
        res = await connection.fetchrow(f'''
            SELECT COUNT(*) FROM checks
            WHERE domain = $1
            AND created_at >= $2
            AND domain NOT IN {no_backoff}
        ''', domain, since)
        return res['count'] >= config.BACKOFF_NB_REQ, res['count']


def fix_surrogates(value):
    """FIX Unicode low surrogate must follow a high surrogate.
    eg in 'TREMI_2017-R\xe9sultats enqu\xeate bruts.csv'
    """
    return value.encode('utf-8', 'surrogateescape')\
                .decode('utf-8', 'replace')


def convert_headers(headers):
    """
    Convert headers from CIMultiDict to dict

    :warning: this will only take the first value for a given header
    key but multidict is not json serializable
    """
    if not headers:
        return {}
    _headers = {}
    for k in headers.keys():
        value = fix_surrogates(headers[k])
        _headers[k.lower()] = value
    return _headers


async def check_url(row, session, sleep=0, method='head'):
    log.debug(f"check {row['url']}, sleep {sleep}")

    if sleep:
        await asyncio.sleep(sleep)

    url_parsed = urlparse(row['url'])
    domain = url_parsed.netloc
    if not domain:
        log.warning(f"[warning] not netloc in url, skipping {row['url']}")
        await insert_check({
            'url': row['url'],
            'error': 'Not netloc in url',
            'timeout': False,
        })
        return STATUS_ERROR

    if domain in config.GET_DOMAINS:
        method = 'get'

    should_backoff, nb_req = await is_backoff(domain)
    if should_backoff:
        log.info(f'backoff {domain} ({nb_req})')
        context.monitor().add_backoff(domain, nb_req)
        # TODO: maybe just skip this url, it should come back in the next batch anyway
        # but won't it accumulate too many backoffs in the end? is this a problem?
        return await check_url(row, session,
                               sleep=config.BACKOFF_PERIOD / config.BACKOFF_NB_REQ)
    else:
        context.monitor().remove_backoff(domain)

    try:
        start = time.time()
        timeout = aiohttp.ClientTimeout(total=5)
        _method = getattr(session, method)
        async with _method(row['url'], timeout=timeout, allow_redirects=True) as resp:
            end = time.time()
            if resp.status == 501 and method != 'get':
                return await check_url(row, session, method='get')
            resp.raise_for_status()
            await insert_check({
                'url': row['url'],
                'domain': domain,
                'status': resp.status,
                'headers': convert_headers(resp.headers),
                'timeout': False,
                'response_time': end - start,
            })
            return STATUS_OK
    # TODO: debug AssertionError, should be caught in DB now
    # File "[...]aiohttp/connector.py", line 991, in _create_direct_connection
    # assert port is not None
    # UnicodeError: encoding with 'idna' codec failed (UnicodeError: label too long)
    # eg http://%20Localisation%20des%20acc%C3%A8s%20des%20offices%20de%20tourisme
    except (aiohttp.client_exceptions.ClientError, AssertionError, UnicodeError) as e:
        error = getattr(e, 'message', None) or str(e)
        await insert_check({
            'url': row['url'],
            'domain': domain,
            'timeout': False,
            'error': fix_surrogates(error),
            'headers': convert_headers(getattr(e, 'headers', {})),
            'status': getattr(e, 'status', None),
        })
        log.error(f"{row['url']}, {e}")
        return STATUS_ERROR
    except asyncio.exceptions.TimeoutError:
        await insert_check({
            'url': row['url'],
            'domain': domain,
            'timeout': True,
        })
        return STATUS_TIMEOUT


async def crawl_urls(to_parse):
    context.monitor().set_status('Crawling urls...')
    tasks = []
    async with aiohttp.ClientSession(timeout=None) as session:
        for row in to_parse:
            tasks.append(check_url(row, session))
        for task in asyncio.as_completed(tasks):
            result = await task
            results[result] += 1
            context.monitor().refresh(results)


def get_excluded_clause():
    return ' AND '.join([f"catalog.url NOT LIKE '{p}'" for p in config.EXCLUDED_PATTERNS])


async def crawl_batch():
    """Crawl a batch from the catalog"""
    context.monitor().set_status('Getting a batch from catalog...')
    pool = await context.pool()
    async with pool.acquire() as connection:
        excluded = get_excluded_clause()
        # urls without checks first
        q = f'''
            SELECT * FROM (
                SELECT DISTINCT(catalog.url)
                FROM catalog
                WHERE catalog.last_check IS NULL
                AND {excluded}
                AND deleted = False
            ) s
            ORDER BY random() LIMIT {config.BATCH_SIZE};
        '''
        to_check = await connection.fetch(q)
        # if not enough for our batch size, handle outdated checks
        if len(to_check) < config.BATCH_SIZE:
            since = parse_timespan(config.SINCE)  # in seconds
            since = datetime.utcnow() - timedelta(seconds=since)
            limit = config.BATCH_SIZE - len(to_check)
            q = f'''
            SELECT * FROM (
                SELECT DISTINCT(catalog.url)
                FROM catalog, checks
                WHERE catalog.last_check IS NOT NULL
                AND {excluded}
                AND catalog.last_check = checks.id
                AND checks.created_at <= $1
                AND catalog.deleted = False
            ) s
            ORDER BY random() LIMIT {limit};
            '''
            to_check += await connection.fetch(q, since)

    if len(to_check):
        await crawl_urls(to_check)
    else:
        context.monitor().set_status('Nothing to crawl for now.')
        await asyncio.sleep(config.SLEEP_BETWEEN_BATCHES)


async def crawl(iterations=-1):
    """Launch crawl batches

    :iterations: for testing purposes (break infinite loop)
    """
    try:
        context.monitor().init(
            SINCE=config.SINCE, BATCH_SIZE=config.BATCH_SIZE,
            BACKOFF_NB_REQ=config.BACKOFF_NB_REQ, BACKOFF_PERIOD=config.BACKOFF_PERIOD
        )
        while iterations != 0:
            await crawl_batch()
            iterations -= 1
    finally:
        pool = await context.pool()
        await pool.close()


def setup_logging():
    file_handler = os.getenv('DECAPODE_CURSES_ENABLED', False) == 'True'
    if file_handler:
        handler = logging.FileHandler('crawl.log')
    else:
        handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)


def run():
    """Main function

    :iterations: for testing purposes (break infinite loop)
    """
    setup_logging()
    try:
        asyncio.get_event_loop().run_until_complete(crawl())
    except KeyboardInterrupt:
        pass
    finally:
        context.monitor().teardown()


if __name__ == "__main__":
    run()
