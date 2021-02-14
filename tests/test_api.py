"""
NB: we can't use pytest-aiohttp helpers beause
it will interfere with the rest of our async code
"""
import json

import pytest

from aiohttp.test_utils import TestClient, TestServer

from decapode.crawl import insert_check
from decapode.app import app_factory

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


@pytest.mark.parametrize("query", [
    "url=https://example.com/resource-1", "resource_id=c4e3a9fb-4415-488e-ba57-d05269b27adf"
])
async def test_api_latest(query):
    await fake_check()
    app = await app_factory()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get(f"/checks/latest/?{query}")
        assert resp.status == 200
        data = await resp.json()
        assert data.pop("created_at")
        assert data.pop("id")
        assert data == {
            "response_time": 0.1,
            "deleted": False,
            "resource_id": "c4e3a9fb-4415-488e-ba57-d05269b27adf",
            "catalog_id": 1,
            "domain": "example.com",
            "error": None,
            "url": "https://example.com/resource-1",
            "headers": {
                "x-do": "you"
            },
            "timeout": False,
            "dataset_id": "601ddcfc85a59c3a45c2435a",
            "status": 200
        }


@pytest.mark.parametrize("query", [
    "url=https://example.com/resource-1", "resource_id=c4e3a9fb-4415-488e-ba57-d05269b27adf"
])
async def test_api_all(setup_catalog, query):
    await fake_check(status=500, error="no-can-do")
    await fake_check()
    app = await app_factory()
    async with TestClient(TestServer(app)) as client:
        resp = await client.get(f"/checks/all/?{query}")
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 2
        first, second = data
        assert first["status"] == 200
        assert second["status"] == 500
        assert second["error"] == "no-can-do"