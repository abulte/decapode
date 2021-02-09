import os
import json
import pytest
from unittest import mock

import asyncpg
import nest_asyncio

from aioresponses import aioresponses
from minicli import run
from yarl import URL

from decapode.crawl import run as run_crawl

import decapode.cli  # noqa - this register the cli cmds

DATABASE_URL = "postgres://postgres:postgres@localhost:5433/postgres"
pytestmark = pytest.mark.asyncio
# allows nested async to test async with async :mindblown:
nest_asyncio.apply()


@pytest.fixture
def catalog_content():
    with open("tests/catalog.csv", "rb") as cfile:
        return cfile.read()


@pytest.fixture
def rmock():
    with aioresponses() as m:
        yield m


@pytest.fixture
async def db():
    conn = await asyncpg.connect(dsn=DATABASE_URL)
    yield conn
    await conn.close()


@pytest.fixture(autouse=True)
def setup(catalog_content, rmock):
    catalog = "https://example.com/catalog"
    rmock.get(catalog, status=200, body=catalog_content)
    with mock.patch.dict(os.environ, {"DATABASE_URL": DATABASE_URL}):
        run("init_db", drop=True, table=None, index=True, reindex=False)
        run("load_catalog", url=catalog)
        yield


async def test_catalog(db):
    res = await db.fetch(
        "SELECT * FROM catalog WHERE resource_id = $1",
        "c4e3a9fb-4415-488e-ba57-d05269b27adf"
    )
    assert len(res) == 1
    resource = res[0]
    assert resource["url"] == "https://example.com/resource-1"
    assert resource["dataset_id"] == "601ddcfc85a59c3a45c2435a"


async def test_crawl(rmock, db):
    rurl = "https://example.com/resource-1"
    rmock.head(rurl, status=200, headers={"Content-LENGTH": "10", "X-Do": "you"})
    run_crawl(iterations=1)
    assert ('HEAD', URL(rurl)) in rmock.requests
    res = await db.fetch("SELECT * from checks")
    assert len(res) == 1
    resource = res[0]
    assert resource["url"] == rurl
    assert resource["status"] == 200
    assert json.loads(resource["headers"]) == {
        "x-do": "you",
        # added by aioresponses :shrug:
        "content-type": "application/json",
        "content-length": "10"
    }
