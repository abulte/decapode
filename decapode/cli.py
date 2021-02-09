import csv
import os
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile

import aiohttp
import asyncpg
from minicli import cli, run, wrap
from humanfriendly import parse_timespan, parse_size
from progressist import ProgressBar

from decapode import config

CATALOG_URL = 'https://www.data.gouv.fr/fr/datasets/r/4babf5f2-6a9c-45b5-9144-ca5eae6a7a6d'

context = {}


@cli
async def init_db(drop=False, table=None, index=False, reindex=False):
    """Create the DB structure"""
    print('Initializing database...')
    if drop:
        if table == 'catalog' or not table:
            await context['conn'].execute('DROP TABLE IF EXISTS catalog')
        if table == 'checks' or not table:
            await context['conn'].execute('DROP TABLE IF EXISTS checks')
    await context['conn'].execute('''
        CREATE TABLE IF NOT EXISTS catalog(
            id serial PRIMARY KEY,
            dataset_id VARCHAR(24),
            resource_id UUID,
            url VARCHAR,
            deleted BOOLEAN NOT NULL,
            last_check INT,
            UNIQUE(dataset_id, resource_id, url)
        )
    ''')
    await context['conn'].execute('''
        CREATE TABLE IF NOT EXISTS checks(
            id serial PRIMARY KEY,
            url VARCHAR,
            domain VARCHAR,
            created_at TIMESTAMP DEFAULT NOW(),
            status INT,
            headers JSONB,
            timeout BOOLEAN NOT NULL,
            response_time FLOAT,
            error VARCHAR
        )
    ''')
    if reindex:
        await context['conn'].execute('''
            DROP INDEX IF EXISTS url_idx;
            DROP INDEX IF EXISTS domain_idx;
        ''')
    if index or reindex:
        await context['conn'].execute('''
            CREATE INDEX IF NOT EXISTS url_idx ON checks (url);
            CREATE INDEX IF NOT EXISTS domain_idx ON checks (domain);
        ''')


async def download_file(url, fd):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            while True:
                chunk = await resp.content.read(1024)
                if not chunk:
                    break
                fd.write(chunk)


@cli
async def load_catalog(url=CATALOG_URL):
    """Load the catalog into DB from CSV file"""
    try:
        print(f'Downloading catalog from {url}...')
        with NamedTemporaryFile(delete=False) as fd:
            await download_file(url, fd)
        print('Upserting catalog in database...')
        # consider everything deleted, deleted will be updated when loading new catalog
        await context['conn'].execute('UPDATE catalog SET deleted = TRUE')
        with open(fd.name) as fd:
            reader = csv.DictReader(fd, delimiter=';')
            rows = list(reader)
            bar = ProgressBar(total=len(rows))
            for row in bar.iter(rows):
                await context['conn'].execute('''
                    INSERT INTO catalog (dataset_id, resource_id, url, deleted)
                    VALUES ($1, $2, $3, FALSE)
                    ON CONFLICT (dataset_id, resource_id, url) DO UPDATE SET deleted = FALSE
                ''', row['dataset.id'], row['id'], row['url'])
        print('Done!')
    except Exception as e:
        raise e
    finally:
        fd.close()
        os.unlink(fd.name)


@cli
async def summary():
    from tabulate import tabulate
    from crawl import get_excluded_clause
    q = f'''
        SELECT
            SUM(CASE WHEN last_check IS NULL THEN 1 ELSE 0 END) AS count_left,
            SUM(CASE WHEN last_check IS NOT NULL THEN 1 ELSE 0 END) AS count_checked
        FROM catalog
        WHERE {get_excluded_clause()}
        AND catalog.deleted = False
    '''
    stats_catalog = await context['conn'].fetchrow(q)

    since = parse_timespan(config.SINCE)
    since = datetime.utcnow() - timedelta(seconds=since)
    q = f'''
        SELECT
            SUM(CASE WHEN checks.created_at <= $1 THEN 1 ELSE 0 END) AS count_outdated
            --, SUM(CASE WHEN checks.created_at > $1 THEN 1 ELSE 0 END) AS count_fresh
        FROM catalog, checks
        WHERE {get_excluded_clause()}
        AND catalog.last_check = checks.id
        AND catalog.deleted = False
    '''
    stats_checks = await context['conn'].fetchrow(q, since)

    count_left = stats_catalog['count_left'] + stats_checks['count_outdated']
    # all w/ a check, minus those with an outdated checked
    count_checked = stats_catalog['count_checked'] - stats_checks['count_outdated']
    total = stats_catalog['count_left'] + stats_catalog['count_checked']
    rate_checked = round(stats_catalog['count_checked'] / total * 100, 1)
    rate_checked_fresh = round(count_checked / total * 100, 1)

    print(tabulate([
        ['Pending check', count_left],
        ['Checked (once)', stats_catalog['count_checked'], f"{rate_checked}%"],
        ['Checked (fresh)', count_checked, f"{rate_checked_fresh}%"],
    ]))

    q = f'''
        SELECT
            SUM(CASE WHEN error IS NULL AND timeout = False THEN 1 ELSE 0 END) AS count_ok,
            SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END) AS count_error,
            SUM(CASE WHEN timeout = True THEN 1 ELSE 0 END) AS count_timeout
        FROM catalog, checks
        WHERE {get_excluded_clause()}
        AND catalog.last_check = checks.id
        AND catalog.deleted = False
    '''
    stats_status = await context['conn'].fetchrow(q)

    def cmp_rate(key):
        return f"{round(stats_status[key] / stats_catalog['count_checked'] * 100, 1)}%"

    print()
    print(tabulate([
        ['Errors', stats_status['count_error'], cmp_rate('count_error')],
        ['Timeouts', stats_status['count_timeout'], cmp_rate('count_timeout')],
        ['Replied', stats_status['count_ok'], cmp_rate('count_ok')],
    ]))

    q = '''
        SELECT status, count(*) as count FROM checks, catalog
        WHERE catalog.last_check = checks.id
        AND status IS NOT NULL
        GROUP BY status
        ORDER BY count DESC;
    '''
    print()
    print(tabulate(await context['conn'].fetch(q), headers=['HTTP code', 'Count']))


@cli
async def csv_sample(size=1000, download=False, max_size='100M'):
    """Get a csv sample from latest checks

    :size: Size of the sample (how many files to query)
    :download: Download files or just list them
    :max_size: Maximum size for one file (from headers)
    """
    max_size = parse_size(max_size)
    start_q = f'''
        SELECT catalog.resource_id, catalog.dataset_id, checks.url,
            checks.headers->>'content-type' as content_type,
            checks.headers->>'content-length' as content_length
        FROM checks, catalog
        WHERE catalog.last_check = checks.id
        AND checks.headers->>'content-type' LIKE '%csv%'
        AND checks.status >= 200 and checks.status < 400
        AND CAST(checks.headers->>'content-length' AS INTEGER) <= {max_size}
    '''
    end_q = f'''
        ORDER BY RANDOM()
        LIMIT {size / 2}
    '''
    # get remote stuff for half the sample
    q = f'''{start_q}
        -- ignore ODS, they're correctly formated from a datastore
        AND checks.url NOT LIKE '%/explore/dataset/%'
        AND checks.url NOT LIKE '%/api/datasets/1.0/%'
        -- ignore ours
        AND checks.domain <> 'static.data.gouv.fr'
        {end_q}
    '''
    res = await context['conn'].fetch(q)
    # and from us for the rest
    q = f'''{start_q}
        AND checks.domain = 'static.data.gouv.fr'
        {end_q}
    '''
    res += await context['conn'].fetch(q)

    data_path = Path('./data')
    dl_path = data_path / 'downloaded'
    dl_path.mkdir(exist_ok=True, parents=True)
    if download:
        print('Cleaning up...')
        (data_path / '_index.csv').unlink(missing_ok=True)
        [p.unlink() for p in Path(dl_path).glob('*.csv')]

    lines = []
    bar = ProgressBar(total=len(res))
    for r in bar.iter(res):
        line = dict(r)
        line['resource_id'] = str(line['resource_id'])
        filename = dl_path / f"{r['dataset_id']}_{r['resource_id']}.csv"
        line['filename'] = filename.__str__()
        lines.append(line)
        if not download:
            continue
        await download_file(r['url'], filename.open('wb'))
        with os.popen(f'file {filename} -b --mime-type') as proc:
            line['magic_mime'] = proc.read().lower().strip()
        line['real_size'] = filename.stat().st_size

    with (data_path / '_index.csv').open('w') as ofile:
        writer = csv.DictWriter(ofile, fieldnames=lines[0].keys())
        writer.writeheader()
        writer.writerows(lines)


@wrap
async def cli_wrapper():
    dsn = os.getenv('DATABASE_URL', 'postgres://postgres:postgres@localhost:5432/postgres')
    context['conn'] = await asyncpg.connect(dsn=dsn)
    yield
    await context['conn'].close()


if __name__ == '__main__':
    run()
