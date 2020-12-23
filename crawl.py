import curses
import json
import os
import time

from datetime import datetime, timedelta
from urllib.parse import urlparse

import aiohttp
import asyncio
import asyncpg

from humanfriendly import parse_timespan
from tabulate import tabulate

context = {}

DATABASE_URL = os.getenv('DATABASE_URL', 'postgres://postgres:postgres@localhost:5432/postgres')
# max number of _completed_ requests per domain per period
BACKOFF_NB_REQ = 3600 * 2
BACKOFF_PERIOD = 3600  # in seconds


async def insert_check(data):
    columns = ','.join(data.keys())
    # $1, $2...
    placeholders = ','.join([f'${x + 1}' for x in range(len(data.values()))])
    q = f'''
        INSERT INTO checks ({columns})
        VALUES ({placeholders})
    '''
    async with context['pool'].acquire() as connection:
        await connection.execute(q, *data.values())


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

    # TODO: make this a (data)class?
    def return_structure(status, url, details=None):
        return {
            'status': status,
            'url': url,
            'details': details,
        }

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
            return return_structure('ok', row['url'], None)
    except aiohttp.client_exceptions.ClientError as e:
        # TODO: store results
        return return_structure('error', row['url'], e)
    except asyncio.exceptions.TimeoutError:
        await insert_check({
            'url': row['url'],
            'timeout': True,
        })
        return return_structure('timeout', row['url'], None)


# TODO: track more stuff on screen
def refresh_summary(results):
    screen = context['screen']

    screen.addstr(0, 0, "🦀 decapode crawling...", curses.A_BOLD)
    screen.addstr(1, 0, "")

    for i, status in enumerate(['ok', 'timeout', 'error']):
        nb = len([r for r in results if r['status'] == status])
        screen.addstr(i + 2, 0, f'{status}:')
        screen.addstr(i + 2, 10, f'{nb}')

    screen.addstr(5, 0, "")
    screen.addstr(6, 0, "Errors", curses.A_DIM)
    for i, r in enumerate([r for r in results if r['status'] == 'error'][-5:]):
        screen.addstr(i + 7, 0, r['url'])

    screen.addstr(13, 0, "")
    screen.addstr(14, 0, "Timeouts", curses.A_DIM)
    for i, r in enumerate([r for r in results if r['status'] == 'timeout'][-5:]):
        screen.addstr(i + 15, 0, r['url'])

    screen.refresh()


def print_summary(results):
    # TODO: print summary
    # - total checked, aggregated by domain?
    # - total left to check from catalog (because limit)
    # - print on errors too (finally:)
    oks = [r for r in results if r['status'] == 'ok']
    timeouts = [r for r in results if r['status'] == 'timeout']
    errors = [r for r in results if r['status'] == 'error']
    table = [
        ['checked urls', len(results)],
        ['oks', len(oks)],
        ['timeouts', len(timeouts)],
        ['errors', len(errors)],
    ]
    print(f'\n{tabulate(table)}')
    if errors:
        print('\nErrors:')
        for r in errors:
            print('-' * 7)
            print(r['url'])
            print(r['details'])


async def crawl_urls(to_parse, results):
    tasks = []
    async with aiohttp.ClientSession(timeout=None) as session:
        for row in to_parse:
            tasks.append(check_url(row, session))
        for task in asyncio.as_completed(tasks):
            result = await task
            results.append(result)
            refresh_summary(results)


# TODO:
# - domain exclude list
async def crawl(since='4w'):
    """Crawl the catalog"""
    context['pool'] = await asyncpg.create_pool(dsn=DATABASE_URL)

    since = parse_timespan(since)  # in seconds
    since = datetime.now() - timedelta(seconds=since)
    async with context['pool'].acquire() as connection:
        to_parse = await connection.fetch(
            '''
            SELECT url FROM catalog
            -- WHERE url LIKE $1
            -- this is way too slow... index?
            -- AND url NOT IN (SELECT url FROM checks WHERE created_at >= $2)
            ORDER BY random()
            LIMIT 1000
            ''',
            # 'https://static.data.gouv.fr%',
            # since,
        )
    results = []
    await crawl_urls(to_parse, results)
    return results


def run():
    try:
        stdscr = curses.initscr()
        curses.noecho()
        curses.cbreak()
        stdscr.keypad(1)
        curses.curs_set(0)
        try:
            curses.start_color()
        except:  # noqa
            pass
        context['screen'] = stdscr
        # TODO: this will get ugly in memory, only aggregated stuff in results
        # maybe make it a "global" var, might display if interrupted
        # use a log file to output complete run (csv? but don't duplicate the DB!)
        results = asyncio.get_event_loop().run_until_complete(crawl())
        # FIXME: prevents screen from disappearing
        stdscr.getch()
    except KeyboardInterrupt:
        pass
    finally:
        # Set everything back to normal
        if 'stdscr' in locals():
            stdscr.keypad(0)
            curses.echo()
            curses.nocbreak()
            curses.endwin()
        if 'results' in locals():
            print_summary(results)


if __name__ == "__main__":
    run()
