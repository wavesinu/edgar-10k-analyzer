import os
import pytest
from unittest.mock import MagicMock, patch
from importlib import reload

# Ensure required env vars exist before importing settings-bound modules
os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "test-key")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "test-service-key")
os.environ.setdefault("OPENAI_API_KEY", "sk-test")
os.environ.setdefault("USER_AGENT", "EDGAR-Analyzer test@example.com")


class MockResponse:
    def __init__(self, status=200, json_data=None, text_data=""):
        self.status = status
        self._json = json_data or {}
        self._text = text_data

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self):
        return self._json

    async def text(self):
        return self._text

    def raise_for_status(self):
        if 400 <= self.status:
            raise Exception(f"HTTP {self.status}")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_company_submissions_makes_request_and_parses():
    with patch("src.database.connection.create_client") as mock_create:
        mock_create.return_value = MagicMock()
        import src.api.edgar_client as edgar_mod
        reload(edgar_mod)
        EdgarClient = edgar_mod.EdgarClient

        client = EdgarClient()
        client.session = MagicMock()

        cik = "0000320193"
        submissions = {
            "filings": {
                "recent": {
                    "form": ["10-K"],
                    "accessionNumber": ["0000320193-23-000105"],
                    "filingDate": ["2023-10-01"],
                    "reportDate": ["2023-09-30"],
                    "acceptanceDateTime": ["2023-10-01T12:00:00.000Z"],
                }
            }
        }

        client.session.get.return_value = MockResponse(status=200, json_data=submissions)

        data = await client.get_company_submissions(cik)

        assert isinstance(data, dict)
        assert "filings" in data
        client.session.get.assert_called_once()
        called_url = client.session.get.call_args[0][0]
        assert cik in called_url


@pytest.mark.unit
@pytest.mark.asyncio
async def test_fetch_filing_content_handles_404_returns_none():
    with patch("src.database.connection.create_client") as mock_create:
        mock_create.return_value = MagicMock()
        import src.api.edgar_client as edgar_mod
        reload(edgar_mod)
        EdgarClient = edgar_mod.EdgarClient

        client = EdgarClient()
        client.session = MagicMock()
        client.session.get.return_value = MockResponse(status=404)

        content = await client.fetch_filing_content("https://www.sec.gov/Archives/some-missing.htm")
        assert content is None


@pytest.mark.unit
def test_extract_10k_filings_filters_and_maps():
    with patch("src.database.connection.create_client") as mock_create:
        mock_create.return_value = MagicMock()
        import src.api.edgar_client as edgar_mod
        reload(edgar_mod)
        EdgarClient = edgar_mod.EdgarClient

        client = EdgarClient()
        sample = {
            "filings": {
                "recent": {
                    "form": ["10-Q", "10-K", "8-K"],
                    "accessionNumber": [
                        "0000320193-23-000100",
                        "0000320193-23-000105",
                        "0000320193-23-000110",
                    ],
                    "filingDate": ["2023-07-01", "2023-10-01", "2023-11-01"],
                    "reportDate": ["2023-06-30", "2023-09-30", "2023-10-31"],
                    "acceptanceDateTime": [
                        "2023-07-01T10:00:00.000Z",
                        "2023-10-01T12:00:00.000Z",
                        "2023-11-01T13:00:00.000Z",
                    ],
                }
            }
        }
        filings = client.extract_10k_filings(sample, limit=2)
        assert len(filings) == 1
        f = filings[0]
        assert f["form"] == "10-K"
        assert f["fiscalYear"] == 2023


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.skipif(
    not os.environ.get("RUN_INTEGRATION_TESTS"),
    reason="Set RUN_INTEGRATION_TESTS=1 to enable live SEC integration test",
)
async def test_sec_live_get_company_submissions():
    # For live test, we still patch Supabase create_client to avoid DB side-effects
    with patch("src.database.connection.create_client") as mock_create:
        mock_create.return_value = MagicMock()
        import src.api.edgar_client as edgar_mod
        reload(edgar_mod)
        EdgarClient = edgar_mod.EdgarClient

        async with EdgarClient() as client:
            data = await client.get_company_submissions("0000320193")
            assert isinstance(data, dict)
            assert "filings" in data 