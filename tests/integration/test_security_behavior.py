import pytest

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_statement_timeout_fires(live_driver):
    # Pool started with statement_timeout from env (CI sets to 2000).
    # Verify by running pg_sleep beyond the timeout.
    with pytest.raises(Exception):
        await live_driver.execute_query("SELECT pg_sleep(40)", force_readonly=False)


@pytest.mark.asyncio
async def test_uri_not_in_error_path(live_driver):
    try:
        await live_driver.execute_query("SELECT * FROM does_not_exist")
    except Exception as e:
        text = str(e)
        # password "test" from URI postgresql://postgres:test@... should not appear
        assert "postgres:test@" not in text
