import pytest
import nest_asyncio

from minicli import run

pytestmark = pytest.mark.asyncio
nest_asyncio.apply()


async def test_report(setup_catalog, fake_check, tmp_path):
    await fake_check(headers={"last-modified": "Wed, 21 Oct 2015 07:28:00 GMT"})
    report = tmp_path / "test.html"
    run("report", filepath=report.__str__())
    assert report.exists()
    with report.open() as f:
        assert "Decapode report" in f.read()
