import json
import os
import time

from collections import defaultdict
from datetime import datetime, timedelta
from urllib.parse import urlparse

import aiohttp
import asyncio
import asyncpg

from humanfriendly import parse_timespan

from monitor import Monitor

context = {}
results = defaultdict(int)

STATUS_OK = 'ok'
STATUS_TIMEOUT = 'timeout'
STATUS_ERROR = 'error'

# TODO: move to config
DATABASE_URL = os.getenv('DATABASE_URL', 'postgres://postgres:postgres@localhost:5432/postgres')
# max number of _completed_ requests per domain per period
BACKOFF_NB_REQ = 1800
BACKOFF_PERIOD = 3600  # in seconds


async def insert_check(data):
    columns = ','.join(data.keys())
    # $1, $2...
    placeholders = ','.join([f'${x + 1}' for x in range(len(data.values()))])
    q = f'''
        INSERT INTO checks ({columns})
        VALUES ({placeholders})
        RETURNING id
    '''
    async with context['pool'].acquire() as connection:
        last_check = await connection.fetchrow(q, *data.values())
        q = '''UPDATE catalog SET last_check = $1 WHERE url = $2'''
        await connection.execute(q, last_check['id'], data['url'])


async def is_backoff(domain):
    since = datetime.utcnow() - timedelta(seconds=BACKOFF_PERIOD)
    async with context['pool'].acquire() as connection:
        res = await connection.fetch('''
            SELECT COUNT(*) FROM checks
            WHERE domain = $1
            AND created_at >= $2
        ''', domain, since)
        return res[0]['count'] >= BACKOFF_NB_REQ


async def check_url(row, session, sleep=0):

    if sleep:
        await asyncio.sleep(sleep)

    url_parsed = urlparse(row['url'])
    domain = url_parsed.netloc
    if not domain:
        print(f"[warning] not netloc in url, skipping {row['url']}")
        return

    if await is_backoff(domain):
        # TODO: move to curses, it does _not_ like printed stuff while in curses window
        print(f'backoff {domain}')
        return await check_url(row, session, sleep=BACKOFF_PERIOD / BACKOFF_NB_REQ)

    try:
        start = time.time()
        timeout = aiohttp.ClientTimeout(total=5)
        async with session.head(row['url'], timeout=timeout, allow_redirects=True) as resp:
            end = time.time()
            # /!\ this will only take the first value for a given header key
            # but multidict is not json serializable
            headers = {}
            for k in resp.headers.keys():
                # FIX Unicode low surrogate must follow a high surrogate.
                # eg in 'TREMI_2017-R\xe9sultats enqu\xeate bruts.csv'
                value = resp.headers[k]\
                        .encode('utf-8', 'surrogateescape')\
                        .decode('utf-8', 'replace')
                headers[k.lower()] = value
            await insert_check({
                'url': row['url'],
                'domain': domain,
                'status': resp.status,
                'headers': json.dumps(headers),
                'timeout': False,
                'response_time': end - start,
            })
            return STATUS_OK
    except aiohttp.client_exceptions.ClientError as e:
        await insert_check({
            'url': row['url'],
            'domain': domain,
            'timeout': False,
            'error': str(e)
        })
        return STATUS_ERROR
    except asyncio.exceptions.TimeoutError:
        await insert_check({
            'url': row['url'],
            'domain': domain,
            'timeout': True,
        })
        return STATUS_TIMEOUT


async def crawl_urls(to_parse):
    context['monitor'].set_status('Crawling urls...')
    tasks = []
    async with aiohttp.ClientSession(timeout=None) as session:
        for row in to_parse:
            tasks.append(check_url(row, session))
        for task in asyncio.as_completed(tasks):
            result = await task
            results[result] += 1
            context['monitor'].refresh(results)


# TODO:
# - domain exclude list
async def crawl(since='1w'):
    """Crawl the catalog"""
    BATCH_SIZE = 100

    context['monitor'].init(BATCH_SIZE)
    context['pool'] = await asyncpg.create_pool(dsn=DATABASE_URL)

    context['monitor'].set_status('Getting a batch from catalog...')
    async with context['pool'].acquire() as connection:
        # urls without checks first
        q = f'''
            SELECT * FROM (
                SELECT DISTINCT(catalog.url)
                FROM catalog
                WHERE catalog.last_check IS NULL
            ) s
            ORDER BY random() LIMIT {BATCH_SIZE};
        '''
        to_check = await connection.fetch(q)
        # if not enough for our batch size, handle outdated checks
        if len(to_check) < BATCH_SIZE:
            since = parse_timespan(since)  # in seconds
            since = datetime.utcnow() - timedelta(seconds=since)
            limit = BATCH_SIZE - len(to_check)
            q = f'''
            SELECT * FROM (
                SELECT DISTINCT(catalog.url)
                FROM catalog, checks
                WHERE catalog.last_check IS NOT NULL
                AND catalog.last_check = checks.id
                AND checks.created_at <= $1
            ) s
            ORDER BY random() LIMIT {limit};
            '''
            to_check += await connection.fetch(q, since)

    await crawl_urls(to_check)
    context['monitor'].set_status('Crawling done.')


def run():
    try:
        monitor = Monitor()
        context['monitor'] = monitor
        # TODO: this will get ugly in memory, only aggregated stuff in results
        # maybe make it a "global" var, might display if interrupted
        # use a log file to output complete run (csv? but don't duplicate the DB!)
        asyncio.get_event_loop().run_until_complete(crawl())
        # FIXME: prevents screen from disappearing
        monitor.listen()
    except KeyboardInterrupt:
        pass
    finally:
        if 'monitor' in locals():
            monitor.teardown()


if __name__ == "__main__":
    run()
