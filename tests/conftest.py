import os

from unittest import mock

from aioresponses import aioresponses
import asyncpg
import pytest

from minicli import run

import decapode.cli  # noqa - this register the cli cmds
from decapode.crawl import insert_check

DATABASE_URL = "postgresql://postgres:postgres@localhost:5433/postgres"
pytestmark = pytest.mark.asyncio


# this really really really should run first (or "prod" db will get erased)
@pytest.fixture(autouse=True, scope="session")
def setup():
    with mock.patch.dict(os.environ, {"DATABASE_URL": DATABASE_URL}):
        yield


@pytest.fixture(autouse=True)
async def mock_pool(mocker):
    """This avoids having different pools attached to different event loops"""
    m = mocker.patch("decapode.context.pool")
    pool = await asyncpg.create_pool(dsn=DATABASE_URL, max_size=50)
    m.return_value = pool


@pytest.fixture
def catalog_content():
    with open("tests/catalog.csv", "rb") as cfile:
        return cfile.read()


@pytest.fixture
def setup_catalog(catalog_content, rmock):
    catalog = "https://example.com/catalog"
    rmock.get(catalog, status=200, body=catalog_content)
    run("init_db", drop=True, table=None, index=True, reindex=False)
    run("load_catalog", url=catalog)


@pytest.fixture
def rmock():
    # passthrough for local requests (aiohttp TestServer)
    with aioresponses(passthrough=["http://127.0.0.1"]) as m:
        yield m


@pytest.fixture
async def db():
    conn = await asyncpg.connect(dsn=DATABASE_URL)
    yield conn
    await conn.close()


@pytest.fixture
async def fake_check(db):

    async def _fake_check(status=200, error=None, timeout=False, resource=1,
                          created_at=None, headers={"x-do": "you"}):
        data = {
            "url": f"https://example.com/resource-{resource}",
            "domain": "example.com",
            "status": status,
            "headers": headers,
            "timeout": timeout,
            "response_time": 0.1,
            "error": error,
        }
        id = await insert_check(data)
        if created_at:
            data["created_at"] = created_at
            await db.execute("""
                UPDATE checks
                SET created_at = $1
                WHERE id = $2
            """, created_at, id)
        return data

    return _fake_check
