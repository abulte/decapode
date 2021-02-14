import os

from unittest import mock

from aioresponses import aioresponses
import asyncpg
import pytest

from minicli import run

import decapode.cli  # noqa - this register the cli cmds

DATABASE_URL = "postgres://postgres:postgres@localhost:5433/postgres"

pytestmark = pytest.mark.asyncio


# this really really really should run first
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
