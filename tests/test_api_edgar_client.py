"""Comprehensive tests for EDGAR API client functionality."""

import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime, date
import aiohttp

from tests.conftest import (
    unit_test, integration_test, requires_api,
    MockHttpResponse, skip_if_no_live_api
)


@unit_test
class TestEdgarClientUnit:
    """Unit tests for EdgarClient."""

    @pytest.fixture
    def edgar_client(self, mock_supabase_client):
        """Create EdgarClient instance with mocked dependencies."""
        with patch("src.database.connection.create_client", return_value=mock_supabase_client):
            from src.api.edgar_client import EdgarClient
            return EdgarClient()

    @pytest.mark.asyncio
    async def test_initialization(self, edgar_client):
        """Test EdgarClient initialization."""
        assert edgar_client is not None
        assert edgar_client.base_url == "https://data.sec.gov"
        assert "EDGAR-Analyzer" in edgar_client.headers["User-Agent"]

    @pytest.mark.asyncio
    async def test_get_company_submissions_success(self, edgar_client, sample_sec_submissions):
        """Test successful company submissions retrieval."""
        edgar_client.session = MagicMock()
        edgar_client.session.get.return_value = MockHttpResponse(200, json_data=sample_sec_submissions)
        
        result = await edgar_client.get_company_submissions("0000320193")
        
        assert result == sample_sec_submissions
        edgar_client.session.get.assert_called_once()
        called_url = edgar_client.session.get.call_args[0][0]
        assert "0000320193" in called_url

    @pytest.mark.asyncio
    async def test_get_company_submissions_http_error(self, edgar_client):
        """Test company submissions retrieval with HTTP error."""
        edgar_client.session = MagicMock()
        edgar_client.session.get.return_value = MockHttpResponse(404)
        
        result = await edgar_client.get_company_submissions("0000999999")
        
        assert result is None

    @pytest.mark.asyncio
    async def test_get_company_submissions_network_error(self, edgar_client):
        """Test company submissions retrieval with network error."""
        edgar_client.session = MagicMock()
        edgar_client.session.get.side_effect = aiohttp.ClientError("Network error")
        
        result = await edgar_client.get_company_submissions("0000320193")
        
        assert result is None

    def test_extract_10k_filings_filters_correctly(self, edgar_client, sample_sec_submissions):
        """Test 10-K filing extraction and filtering."""
        filings = edgar_client.extract_10k_filings(sample_sec_submissions, limit=5)
        
        # Should only return 10-K filings
        assert len(filings) == 1
        filing = filings[0]
        assert filing["form"] == "10-K"
        assert filing["accessionNumber"] == "0000320193-23-000105"
        assert filing["fiscalYear"] == 2023

    def test_extract_10k_filings_limit_respected(self, edgar_client):
        """Test that filing limit is respected."""
        submissions = {
            "filings": {
                "recent": {
                    "form": ["10-K", "10-K", "10-K"],
                    "accessionNumber": ["acc1", "acc2", "acc3"],
                    "filingDate": ["2023-10-01", "2022-10-01", "2021-10-01"],
                    "reportDate": ["2023-09-30", "2022-09-30", "2021-09-30"],
                    "acceptanceDateTime": ["2023-10-01T10:00:00.000Z", "2022-10-01T10:00:00.000Z", "2021-10-01T10:00:00.000Z"]
                }
            }
        }
        
        filings = edgar_client.extract_10k_filings(submissions, limit=2)
        
        assert len(filings) == 2

    def test_extract_10k_filings_empty_data(self, edgar_client):
        """Test extraction with empty data."""
        submissions = {"filings": {"recent": {"form": [], "accessionNumber": []}}}
        
        filings = edgar_client.extract_10k_filings(submissions, limit=5)
        
        assert len(filings) == 0

    @pytest.mark.asyncio
    async def test_fetch_filing_content_success(self, edgar_client, sample_10k_html):
        """Test successful filing content retrieval."""
        edgar_client.session = MagicMock()
        edgar_client.session.get.return_value = MockHttpResponse(200, text_data=sample_10k_html)
        
        content = await edgar_client.fetch_filing_content("https://test.url/filing.htm")
        
        assert content == sample_10k_html

    @pytest.mark.asyncio
    async def test_fetch_filing_content_not_found(self, edgar_client):
        """Test filing content retrieval for non-existent file."""
        edgar_client.session = MagicMock()
        edgar_client.session.get.return_value = MockHttpResponse(404)
        
        content = await edgar_client.fetch_filing_content("https://test.url/missing.htm")
        
        assert content is None

    def test_build_filing_url_formats_correctly(self, edgar_client):
        """Test filing URL construction."""
        url = edgar_client.build_filing_url("0000320193", "0000320193-23-000105")
        
        expected = "https://www.sec.gov/Archives/edgar/data/320193/000032019323000105/0000320193-23-000105-index.htm"
        assert url == expected

    def test_extract_document_sections_parses_html(self, edgar_client, sample_10k_html):
        """Test HTML parsing and section extraction."""
        sections = edgar_client.extract_document_sections(sample_10k_html)
        
        assert "item_1_business" in sections
        assert "item_1a_risk_factors" in sections
        assert "item_7_mda" in sections
        
        assert "Apple Inc." in sections["item_1_business"]
        assert "risk factors" in sections["item_1a_risk_factors"]
        assert "discussion" in sections["item_7_mda"]

    @pytest.mark.asyncio
    async def test_get_filing_html_content_integration(self, edgar_client, sample_10k_html):
        """Test complete HTML content retrieval flow."""
        edgar_client.session = MagicMock()
        edgar_client.session.get.return_value = MockHttpResponse(200, text_data=sample_10k_html)
        
        content = await edgar_client.get_filing_html_content("0000320193", "0000320193-23-000105")
        
        assert content == sample_10k_html

    @pytest.mark.asyncio
    async def test_context_manager_session_handling(self, mock_supabase_client):
        """Test proper session handling in context manager."""
        with patch("src.database.connection.create_client", return_value=mock_supabase_client):
            from src.api.edgar_client import EdgarClient
            
            async with EdgarClient() as client:
                assert client.session is not None
                assert isinstance(client.session, (aiohttp.ClientSession, MagicMock))


@integration_test
@requires_api
class TestEdgarClientIntegration:
    """Integration tests for EdgarClient with real API calls."""

    @pytest.fixture
    def edgar_client(self, mock_supabase_client):
        """Create EdgarClient instance for integration testing."""
        with patch("src.database.connection.create_client", return_value=mock_supabase_client):
            from src.api.edgar_client import EdgarClient
            return EdgarClient()

    @skip_if_no_live_api()
    @pytest.mark.asyncio
    async def test_real_company_submissions_apple(self, edgar_client):
        """Test real API call to get Apple's submissions."""
        async with edgar_client:
            submissions = await edgar_client.get_company_submissions("0000320193")
            
            assert submissions is not None
            assert "filings" in submissions
            assert "recent" in submissions["filings"]
            assert isinstance(submissions["filings"]["recent"]["form"], list)

    @skip_if_no_live_api()
    @pytest.mark.asyncio
    async def test_real_10k_extraction_and_parsing(self, edgar_client):
        """Test real 10-K filing extraction and parsing."""
        async with edgar_client:
            # Get Apple's submissions
            submissions = await edgar_client.get_company_submissions("0000320193")
            assert submissions is not None
            
            # Extract recent 10-K filings
            filings = edgar_client.extract_10k_filings(submissions, limit=1)
            assert len(filings) >= 1
            
            # Get HTML content for the most recent 10-K
            filing = filings[0]
            html_content = await edgar_client.get_filing_html_content(
                "0000320193", 
                filing["accessionNumber"]
            )
            
            assert html_content is not None
            assert len(html_content) > 1000  # Should be substantial content
            
            # Extract sections
            sections = edgar_client.extract_document_sections(html_content)
            
            # Should extract at least one section
            non_empty_sections = [k for k, v in sections.items() if v and len(v.strip()) > 0]
            assert len(non_empty_sections) >= 1

    @skip_if_no_live_api()
    @pytest.mark.asyncio
    async def test_rate_limiting_compliance(self, edgar_client):
        """Test that rate limiting is properly implemented."""
        start_time = datetime.now()
        
        async with edgar_client:
            # Make multiple requests
            companies = ["0000320193", "0000789019", "0001018724"]  # Apple, Microsoft, Amazon
            
            tasks = []
            for cik in companies:
                task = edgar_client.get_company_submissions(cik)
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Should take at least some time due to rate limiting
        assert duration >= 0.1  # At least 100ms for rate limiting
        
        # Should get successful responses
        successful_results = [r for r in results if isinstance(r, dict)]
        assert len(successful_results) >= 1

    @skip_if_no_live_api()
    @pytest.mark.asyncio
    async def test_error_handling_with_invalid_cik(self, edgar_client):
        """Test error handling with invalid CIK."""
        async with edgar_client:
            result = await edgar_client.get_company_submissions("9999999999")
            
            # Should return None for invalid CIK
            assert result is None

    @skip_if_no_live_api()
    @pytest.mark.asyncio
    async def test_user_agent_compliance(self, edgar_client):
        """Test that proper User-Agent header is sent."""
        # This test ensures SEC.gov compliance
        async with edgar_client:
            result = await edgar_client.get_company_submissions("0000320193")
            
            # If we get a result, it means our User-Agent was accepted
            assert result is not None
            assert "filings" in result