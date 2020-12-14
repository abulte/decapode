import os
import csv
import json
import time

import aiohttp
import asyncio
import asyncpg

from tempfile import NamedTemporaryFile
from urllib.parse import urlparse

from minicli import cli, run, wrap
from progressist import ProgressBar

DATABASE_URL = os.getenv('DATABASE_URL', 'postgres://postgres:postgres@localhost:5432/postgres')
CATALOG_URL = 'https://www.data.gouv.fr/fr/datasets/r/4babf5f2-6a9c-45b5-9144-ca5eae6a7a6d'

context = {}


@cli
async def init_db(drop=False, table=None):
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
            response_time FLOAT
        )
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
                await context['conn'].execute(f'''
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


async def check_url(row, session, timeout):
    url_parsed = urlparse(row['url'])
    if not url_parsed.netloc:
        print(f"[warning] not netloc in url, skipping {row['url']}")
        return
    try:
        # TODO: store response time
        start = time.time()
        async with session.head(row['url'], timeout=timeout, allow_redirects=True) as resp:
            end = time.time()
            # /!\ this will only take the first value for a given header key
            headers = {}
            for k in resp.headers.keys():
                # FIX Unicode low surrogate must follow a high surrogate.
                # eg in 'TREMI_2017-R\xe9sultats enqu\xeate bruts.csv'
                value = resp.headers[k].encode('utf-8', 'surrogateescape').decode('utf-8', 'replace')
                headers[k.lower()] = value
            async with context['pool'].acquire() as connection:
                await connection.execute('''
                    INSERT INTO checks (url, domain, status, headers, timeout, response_time)
                    VALUES ($1, $2, $3, $4, $5, $6)
                ''', row['url'], url_parsed.netloc, resp.status, json.dumps(headers), False, end - start)
    except aiohttp.client_exceptions.ClientConnectorError as e:
        print(f"[error] {row['url']} {e}")
    except asyncio.exceptions.TimeoutError:
        print(f"[timeout] {row['url']}")


# TODO: backoff on domain (no more than XX calls on the same domain for YY time)
@cli
async def crawl():
    res = await context['conn'].fetch('''
        SELECT * FROM catalog
        WHERE url LIKE $1
        LIMIT 1000
    ''', 'https://static.data.gouv.fr%')
    timeout = aiohttp.ClientTimeout(total=5)
    tasks = []
    async with aiohttp.ClientSession(timeout=None) as session:
        for row in res:
            tasks.append(check_url(row, session, timeout))
        bar = ProgressBar(total=len(res))
        for task in asyncio.as_completed(tasks):
            await task
            bar.update()


@wrap
async def wrapper():
    context['pool'] = await asyncpg.create_pool(dsn=DATABASE_URL)
    context['conn'] = await asyncpg.connect(dsn=DATABASE_URL)
    yield
    await context['conn'].close()


if __name__ == '__main__':
    run()
