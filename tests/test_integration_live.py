import os
import time
import pytest

# Guard: Only run when explicitly enabled
RUN_LIVE = bool(os.environ.get("RUN_INTEGRATION_TESTS"))

REQUIRED_SUPABASE_ENVS = (
    os.environ.get("SUPABASE_URL"),
    os.environ.get("SUPABASE_KEY"),
)

USER_AGENT = os.environ.get("USER_AGENT")


@pytest.mark.integration
@pytest.mark.skipif(not RUN_LIVE, reason="Set RUN_INTEGRATION_TESTS=1 to enable live tests")
@pytest.mark.skipif(
    not all(REQUIRED_SUPABASE_ENVS),
    reason="Supabase envs missing: set SUPABASE_URL and SUPABASE_KEY",
)
@pytest.mark.asyncio
async def test_supabase_live_insert_select_delete_company():
    """Insert a temporary company into Supabase, read it back, then delete it."""
    from src.database.connection import SupabaseClient
    from src.database.schema import Company

    client = SupabaseClient()

    # Unique ticker for this test run (<= 10 chars)
    ticker = f"ZZ{int(time.time())%100000000:08d}"
    company = Company(
        ticker=ticker,
        cik="0000000000",
        company_name="Integration Test Co",
        exchange="TEST",
    )

    # Upsert
    upserted = await client.upsert_company(company)
    assert upserted is not None

    # Read back
    fetched = await client.get_company_by_ticker(ticker)
    assert fetched is not None
    assert fetched.get("ticker") == ticker

    # Cleanup
    resp = client.client.table("companies").delete().eq("ticker", ticker).execute()
    # Some client versions return list in data; we just assert no exception and data present
    assert resp is not None


@pytest.mark.integration
@pytest.mark.skipif(not RUN_LIVE, reason="Set RUN_INTEGRATION_TESTS=1 to enable live tests")
@pytest.mark.skipif(not USER_AGENT, reason="USER_AGENT is required by SEC.gov policy")
@pytest.mark.asyncio
async def test_sec_live_fetch_10k_html_and_parse_sections():
    """Fetch Apple (AAPL) recent 10-K, download HTML, and parse sections."""
    from src.api.edgar_client import EdgarClient

    cik = "0000320193"  # Apple Inc.

    async with EdgarClient() as client:
        submissions = await client.get_company_submissions(cik)
        assert isinstance(submissions, dict) and submissions
        assert "filings" in submissions

        filings = client.extract_10k_filings(submissions, limit=1)
        assert len(filings) >= 1
        acc_no = filings[0]["accessionNumber"]

        html = await client.get_filing_html_content(cik, acc_no)
        assert html and len(html) > 1000

        sections = client.extract_document_sections(html)
        # At least one of the target sections should be non-empty
        non_empty = [k for k, v in sections.items() if isinstance(v, str) and len(v.strip()) > 0]
        assert len(non_empty) >= 1, f"No sections extracted. Keys: {list(sections.keys())}" 