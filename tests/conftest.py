"""Pytest configuration and shared fixtures for EDGAR 10-K analyzer tests."""

import os
import pytest
import asyncio
import tempfile
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime, date
from typing import Dict, Any, List
from pathlib import Path

# Set required environment variables before imports
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "test-key")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "test-service-key")
os.environ.setdefault("OPENAI_API_KEY", "sk-test-12345")
os.environ.setdefault("USER_AGENT", "EDGAR-Analyzer test@example.com")
os.environ.setdefault("DATA_DIR", "test_data")
os.environ.setdefault("LOGS_DIR", "test_logs")

# Pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "e2e: mark test as an end-to-end test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "requires_api: mark test as requiring external API access"
    )
    config.addinivalue_line(
        "markers", "requires_db: mark test as requiring database access"
    )


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def mock_settings():
    """Mock settings configuration."""
    with patch("config.settings.settings") as mock:
        mock.supabase_url = "https://test.supabase.co"
        mock.supabase_key = "test-key"
        mock.supabase_service_key = "test-service-key"
        mock.openai_api_key = "sk-test-12345"
        mock.user_agent = "EDGAR-Analyzer test@example.com"
        mock.data_dir = "test_data"
        mock.logs_dir = "test_logs"
        mock.batch_size = 5
        mock.max_concurrent_requests = 2
        mock.request_delay = 0.01
        mock.openai_model = "gpt-4-turbo-preview"
        mock.openai_max_tokens = 1000
        mock.openai_temperature = 0.3
        mock.top_nasdaq_tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
        mock.qualitative_sections = [
            "item_1_business",
            "item_1a_risk_factors",
            "item_7_mda",
        ]
        yield mock


# Mock HTTP Response Classes
class MockHttpResponse:
    """Mock HTTP response for aiohttp."""
    
    def __init__(self, status=200, json_data=None, text_data="", headers=None):
        self.status = status
        self._json = json_data or {}
        self._text = text_data
        self.headers = headers or {}
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc, tb):
        return False
    
    async def json(self):
        return self._json
    
    async def text(self):
        return self._text
    
    def raise_for_status(self):
        if 400 <= self.status < 600:
            raise Exception(f"HTTP {self.status}")


class MockSupabaseResponse:
    """Mock Supabase response."""
    
    def __init__(self, data=None, count=None):
        self.data = data or []
        self.count = count or len(self.data) if data else 0
    
    def execute(self):
        return self
    
    def limit(self, count):
        return self
    
    def eq(self, column, value):
        return self
    
    def select(self, columns):
        return self
    
    def insert(self, data):
        return self
    
    def upsert(self, data):
        return self
    
    def update(self, data):
        return self
    
    def delete(self):
        return self


# Test Data Fixtures
@pytest.fixture
def sample_company_data():
    """Sample company data for testing."""
    return {
        "ticker": "AAPL",
        "cik": "0000320193",
        "company_name": "Apple Inc.",
        "exchange": "NASDAQ",
        "sector": "Technology",
        "industry": "Consumer Electronics",
        "market_cap": 3000000000000
    }


@pytest.fixture
def sample_filing_data():
    """Sample filing data for testing."""
    return {
        "company_id": "test-company-id",
        "ticker": "AAPL",
        "cik": "0000320193",
        "accession_number": "0000320193-23-000105",
        "form_type": "10-K",
        "filing_date": date(2023, 10, 27),
        "report_date": date(2023, 9, 30),
        "fiscal_year": 2023,
        "edgar_url": "https://www.sec.gov/Archives/edgar/data/320193/000032019323000105/0000320193-23-000105-index.htm"
    }


@pytest.fixture
def sample_sec_submissions():
    """Sample SEC submissions response for testing."""
    return {
        "cik": "0000320193",
        "entityType": "operating",
        "sic": "3571",
        "sicDescription": "Electronic Computers",
        "name": "Apple Inc.",
        "tickers": ["AAPL"],
        "exchanges": ["Nasdaq"],
        "filings": {
            "recent": {
                "accessionNumber": [
                    "0000320193-23-000105",
                    "0000320193-23-000077",
                    "0000320193-23-000064"
                ],
                "filingDate": [
                    "2023-10-27",
                    "2023-08-03",
                    "2023-05-04"
                ],
                "reportDate": [
                    "2023-09-30",
                    "2023-07-01",
                    "2023-04-01"
                ],
                "acceptanceDateTime": [
                    "2023-10-27T18:01:14.000Z",
                    "2023-08-03T18:04:28.000Z",
                    "2023-05-04T18:02:37.000Z"
                ],
                "form": ["10-K", "10-Q", "10-Q"],
                "fileNumber": ["001-36743", "001-36743", "001-36743"],
                "filmNumber": ["231354297", "231154232", "23874639"]
            }
        }
    }


@pytest.fixture
def sample_10k_html():
    """Sample 10-K HTML content for testing."""
    return '''
    <html>
    <body>
        <div>
            <p>ITEM 1. BUSINESS</p>
            <p>Apple Inc. ("Apple," "we," "us" or "our") designs, manufactures and markets smartphones, personal computers, tablets, wearables and accessories worldwide.</p>
        </div>
        <div>
            <p>ITEM 1A. RISK FACTORS</p>
            <p>The following discussion of risk factors contains forward-looking statements. These risk factors may be important to understanding other statements in this Form 10-K.</p>
        </div>
        <div>
            <p>ITEM 7. MANAGEMENT'S DISCUSSION AND ANALYSIS</p>
            <p>The following discussion should be read in conjunction with the consolidated financial statements and notes thereto included in Part II, Item 8 of this Form 10-K.</p>
        </div>
    </body>
    </html>
    '''


@pytest.fixture
def sample_qualitative_sections():
    """Sample extracted qualitative sections."""
    return {
        "item_1_business": "Apple Inc. designs, manufactures and markets smartphones, personal computers, tablets, wearables and accessories worldwide. The Company's products include iPhone, Mac, iPad, AirPods, Apple TV, Apple Watch, and HomePod.",
        "item_1a_risk_factors": "The following discussion of risk factors contains forward-looking statements. These risk factors include market competition, supply chain disruptions, regulatory changes, and cybersecurity threats.",
        "item_7_mda": "Management's discussion and analysis of financial condition and results of operations. Revenue increased due to strong iPhone sales and services growth. Operating margins improved through operational efficiencies."
    }


@pytest.fixture
def mock_supabase_client():
    """Mock Supabase client for testing."""
    mock_client = MagicMock()
    
    # Mock table operations
    mock_table = MagicMock()
    mock_table.select.return_value.execute.return_value = MockSupabaseResponse([])
    mock_table.insert.return_value.execute.return_value = MockSupabaseResponse([{"id": "test-id"}])
    mock_table.upsert.return_value.execute.return_value = MockSupabaseResponse([{"id": "test-id"}])
    mock_table.update.return_value.execute.return_value = MockSupabaseResponse([{"id": "test-id"}])
    mock_table.delete.return_value.execute.return_value = MockSupabaseResponse([])
    
    mock_client.table.return_value = mock_table
    
    return mock_client


@pytest.fixture
def mock_openai_client():
    """Mock OpenAI client for testing."""
    mock_client = MagicMock()
    
    # Mock chat completions
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = "This is a positive analysis with strong growth prospects."
    mock_response.usage.total_tokens = 500
    
    mock_client.chat.completions.create = AsyncMock(return_value=mock_response)
    
    return mock_client


@pytest.fixture
def mock_edgar_response():
    """Mock EDGAR API response."""
    async def mock_get(*args, **kwargs):
        url = args[0] if args else ""
        
        if "submissions" in url:
            return MockHttpResponse(200, json_data={
                "filings": {
                    "recent": {
                        "form": ["10-K", "10-Q"],
                        "accessionNumber": ["0000320193-23-000105", "0000320193-23-000077"],
                        "filingDate": ["2023-10-27", "2023-08-03"],
                        "reportDate": ["2023-09-30", "2023-07-01"],
                        "acceptanceDateTime": ["2023-10-27T18:01:14.000Z", "2023-08-03T18:04:28.000Z"]
                    }
                }
            })
        elif ".htm" in url:
            return MockHttpResponse(200, text_data='''
                <html><body>
                <div>ITEM 1. BUSINESS<p>Sample business content</p></div>
                <div>ITEM 1A. RISK FACTORS<p>Sample risk content</p></div>
                </body></html>
            ''')
        else:
            return MockHttpResponse(404)
    
    return mock_get


# Environment-based skip conditions
def skip_if_no_integration():
    """Skip test if integration tests are disabled."""
    return pytest.mark.skipif(
        not os.environ.get("RUN_INTEGRATION_TESTS"),
        reason="Set RUN_INTEGRATION_TESTS=1 to enable integration tests"
    )


def skip_if_no_live_api():
    """Skip test if live API tests are disabled."""
    return pytest.mark.skipif(
        not os.environ.get("RUN_LIVE_API_TESTS"),
        reason="Set RUN_LIVE_API_TESTS=1 to enable live API tests"
    )


# Test categories as pytest markers
unit_test = pytest.mark.unit
integration_test = pytest.mark.integration
e2e_test = pytest.mark.e2e
slow_test = pytest.mark.slow
requires_api = pytest.mark.requires_api
requires_db = pytest.mark.requires_db