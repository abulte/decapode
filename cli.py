import csv
import os
from tempfile import NamedTemporaryFile

import aiohttp
import asyncpg
from minicli import cli, run, wrap
from progressist import ProgressBar

DATABASE_URL = os.getenv('DATABASE_URL', 'postgres://postgres:postgres@localhost:5432/postgres')
CATALOG_URL = 'https://www.data.gouv.fr/fr/datasets/r/4babf5f2-6a9c-45b5-9144-ca5eae6a7a6d'

context = {}


@cli
async def init_db(drop=False, table=None, index=False):
    """Create the DB structure"""
    if drop:
        if table == 'catalog' or not table:
            await context['conn'].execute('DROP TABLE catalog')
        if table == 'checks' or not table:
            await context['conn'].execute('DROP TABLE checks')
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
    if index:
        await context['conn'].execute('''
            DROP INDEX IF EXISTS url_idx;
            CREATE INDEX url_idx ON checks (url);
            DROP INDEX IF EXISTS domain_idx;
            CREATE INDEX domain_idx ON checks (domain);
        ''')


@cli
async def load_catalog(url=CATALOG_URL):
    """Load the catalog into DB from CSV file"""
    try:
        print('Downloading catalog...')
        fd = NamedTemporaryFile(delete=False)
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                while True:
                    chunk = await resp.content.read(1024)
                    if not chunk:
                        break
                    fd.write(chunk)
        fd.close()
        print('Creating catalog in database...')
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


# TODO: account for `SINCE`, not only last_check (2nd query...)
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
    total = (stats_catalog['count_left'] + stats_catalog['count_checked'])
    rate = round(stats_catalog['count_checked'] / total * 100, 1)

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
    stats_checks = await context['conn'].fetchrow(q)

    def cmp_rate(key):
        return f"{round(stats_checks[key] / stats_catalog['count_checked'] * 100, 1)}%"

    print(tabulate([
        ['Left to check', stats_catalog['count_left']],
        ['Checked', stats_catalog['count_checked'], f"{rate}%"],
        ['Errors', stats_checks['count_error'], cmp_rate('count_error')],
        ['Timeouts', stats_checks['count_timeout'], cmp_rate('count_timeout')],
        ['Replied', stats_checks['count_ok'], cmp_rate('count_ok')],
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


@wrap
async def cli_wrapper():
    context['conn'] = await asyncpg.connect(dsn=DATABASE_URL)
    yield
    await context['conn'].close()


if __name__ == '__main__':
    run()
