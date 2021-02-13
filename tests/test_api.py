import json

import pytest

from decapode.crawl import insert_check

pytestmark = pytest.mark.asyncio


async def fake_check(status=200, error=None, timeout=False):
    await insert_check({
        "url": "https://example.com/resource-1",
        "domain": "example.com",
        "status": status,
        "headers": json.dumps({"x-do": "you"}),
        "timeout": timeout,
        "response_time": 0.1,
        "error": error,
    })


async def test_api(setup_catalog):
    await fake_check()
