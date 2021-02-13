import json
import pytest

import nest_asyncio

from yarl import URL

from decapode.crawl import crawl


pytestmark = pytest.mark.asyncio
# allows nested async to test async with async :mindblown:
nest_asyncio.apply()


async def test_catalog(setup_catalog, db):
    res = await db.fetch(
        "SELECT * FROM catalog WHERE resource_id = $1",
        "c4e3a9fb-4415-488e-ba57-d05269b27adf"
    )
    assert len(res) == 1
    resource = res[0]
    assert resource["url"] == "https://example.com/resource-1"
    assert resource["dataset_id"] == "601ddcfc85a59c3a45c2435a"


async def test_crawl(setup_catalog, rmock, event_loop, db):
    rurl = "https://example.com/resource-1"
    rmock.head(rurl, status=200, headers={"Content-LENGTH": "10", "X-Do": "you"})
    event_loop.run_until_complete(crawl(iterations=1))
    assert ('HEAD', URL(rurl)) in rmock.requests
    res = await db.fetch("SELECT * FROM checks")
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
