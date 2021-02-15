"""
NB: we can't use pytest-aiohttp helpers beause
it will interfere with the rest of our async code
"""
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
        "headers": {"x-do": "you"},
        "timeout": timeout,
        "response_time": 0.1,
        "error": error,
    })


@pytest.fixture
async def client():
    app = await app_factory()
    async with TestClient(TestServer(app)) as client:
        yield client


@pytest.mark.parametrize("query", [
    "url=https://example.com/resource-1", "resource_id=c4e3a9fb-4415-488e-ba57-d05269b27adf"
])
async def test_api_latest(setup_catalog, query, client):
    await fake_check()
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
async def test_api_all(setup_catalog, query, client):
    await fake_check(status=500, error="no-can-do")
    await fake_check()
    resp = await client.get(f"/checks/all/?{query}")
    assert resp.status == 200
    data = await resp.json()
    assert len(data) == 2
    first, second = data
    assert first["status"] == 200
    assert second["status"] == 500
    assert second["error"] == "no-can-do"


async def test_api_status(setup_catalog, client):
    resp = await client.get("/status/")
    assert resp.status == 200
    data = await resp.json()
    assert data == {
        "total": 1,
        "pending_checks": 1,
        "fresh_checks": 0,
        "checks_percentage": 0.0,
        "fresh_checks_percentage": 0.0,
    }

    await fake_check()
    resp = await client.get("/status/")
    assert resp.status == 200
    data = await resp.json()
    assert data == {
        "total": 1,
        "pending_checks": 0,
        "fresh_checks": 1,
        "checks_percentage": 100.0,
        "fresh_checks_percentage": 100.0,
    }


async def test_api_stats(setup_catalog, client):
    resp = await client.get("/stats/")
    assert resp.status == 200
    data = await resp.json()
    assert data == {
        "status": [
            {
                "label": "error",
                "count": 0,
                "percentage": 0
            },
            {
                "label": "timeout",
                "count": 0,
                "percentage": 0
            },
            {
                "label": "ok",
                "count": 0,
                "percentage": 0
            }
        ],
        "status_codes": []
    }

    # only the last one should count
    await fake_check()
    await fake_check(timeout=True, status=None)
    await fake_check(status=500, error="error")
    resp = await client.get("/stats/")
    assert resp.status == 200
    data = await resp.json()
    assert data == {
        "status": [
            {
                "label": "error",
                "count": 1,
                "percentage": 100.0
            },
            {
                "label": "timeout",
                "count": 0,
                "percentage": 0
            },
            {
                "label": "ok",
                "count": 0,
                "percentage": 0
            }
        ],
        "status_codes": [
            {
                "code": 500,
                "count": 1,
                "percentage": 100.0
            }
        ]
    }
