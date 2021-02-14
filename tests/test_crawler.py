import json
import pytest

import nest_asyncio

from aiohttp.client_exceptions import ClientError
from asyncio.exceptions import TimeoutError
from yarl import URL

from decapode.crawl import crawl, setup_logging


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


@pytest.mark.parametrize("resource", [
    # status, timeout, exception
    (200, False, None),
    (500, False, None),
    (None, False, ClientError("client error")),
    (None, False, AssertionError),
    (None, False, UnicodeError),
    (None, True, TimeoutError),
])
async def test_crawl(setup_catalog, rmock, event_loop, db, resource):
    setup_logging()
    status, timeout, exception = resource
    rurl = "https://example.com/resource-1"
    rmock.head(
        rurl, status=status,
        headers={"Content-LENGTH": "10", "X-Do": "you"},
        exception=exception,
    )
    event_loop.run_until_complete(crawl(iterations=1))
    assert ('HEAD', URL(rurl)) in rmock.requests
    res = await db.fetchrow("SELECT * FROM checks")
    assert res["url"] == rurl
    assert res["status"] == status
    if not exception:
        assert json.loads(res["headers"]) == {
            "x-do": "you",
            # added by aioresponses :shrug:
            "content-type": "application/json",
            "content-length": "10"
        }
    assert res["timeout"] == timeout
    if isinstance(exception, ClientError):
        assert res["error"] == "client error"
    elif status == 500:
        assert res["error"] == "Internal Server Error"
    else:
        assert not res["error"]


# TODO:
# - test backoff
# - test excluded clause
