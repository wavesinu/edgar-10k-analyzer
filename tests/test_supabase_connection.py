import os
import pytest
from unittest.mock import patch, MagicMock

# Ensure required env vars exist before importing settings-bound modules
os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "test-key")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "test-service-key")
os.environ.setdefault("OPENAI_API_KEY", "sk-test")

from src.database.connection import SupabaseClient  # noqa: E402


@pytest.mark.unit
def test_supabase_client_initialization_calls_create_client():
    with patch("src.database.connection.create_client") as mock_create, \
         patch("src.database.connection.settings") as mock_settings:
        mock_client = MagicMock()
        mock_create.return_value = mock_client
        mock_settings.supabase_url = "https://example.supabase.co"
        mock_settings.supabase_key = "test-key"

        client = SupabaseClient()

        mock_create.assert_called_once_with(
            supabase_url=mock_settings.supabase_url,
            supabase_key=mock_settings.supabase_key,
        )
        assert client.client is mock_client


@pytest.mark.unit
@pytest.mark.asyncio
async def test_supabase_upsert_company_uses_table_api():
    with patch("src.database.connection.create_client") as mock_create, \
         patch("src.database.connection.settings") as mock_settings:
        table_mock = MagicMock()
        table_mock.upsert.return_value.execute.return_value = MagicMock(data=[{"id": "1"}])

        supa_mock = MagicMock()
        supa_mock.table.return_value = table_mock
        mock_create.return_value = supa_mock

        mock_settings.supabase_url = "https://example.supabase.co"
        mock_settings.supabase_key = "test-key"

        client = SupabaseClient()

        from src.database.schema import Company

        company = Company(
            ticker="AAPL",
            cik="0000320193",
            company_name="Apple Inc.",
            exchange="NASDAQ",
        )

        result = await client.upsert_company(company)

        supa_mock.table.assert_called_with("companies")
        assert table_mock.upsert.called
        assert result == {"id": "1"}


@pytest.mark.integration
@pytest.mark.skipif(
    not os.environ.get("RUN_INTEGRATION_TESTS"),
    reason="Set RUN_INTEGRATION_TESTS=1 to enable live Supabase integration test",
)
def test_supabase_live_connection_select_companies():
    client = SupabaseClient()

    try:
        response = client.client.table("companies").select("*").limit(1).execute()
        assert response is not None
        assert hasattr(response, "data")
    except Exception as exc:
        pytest.fail(f"Live Supabase connectivity failed: {exc}") 