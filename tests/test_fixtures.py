"""Additional test fixtures and utilities for comprehensive testing."""

import pytest
import json
import tempfile
from pathlib import Path
from datetime import datetime, date
from typing import Dict, Any, List
import pandas as pd


class TestDataGenerator:
    """Generate test data for various scenarios."""
    
    @staticmethod
    def generate_company_data(count: int = 5) -> List[Dict[str, Any]]:
        """Generate multiple company records."""
        companies = []
        base_timestamp = int(datetime.now().timestamp())
        
        for i in range(count):
            companies.append({
                "ticker": f"TEST{i:03d}",
                "cik": f"{base_timestamp + i:010d}",
                "company_name": f"Test Company {i}",
                "exchange": "NASDAQ" if i % 2 == 0 else "NYSE",
                "sector": ["Technology", "Healthcare", "Finance", "Energy", "Consumer"][i % 5],
                "industry": f"Test Industry {i}",
                "market_cap": (i + 1) * 1000000000
            })
        
        return companies

    @staticmethod
    def generate_filing_data(company_id: str, count: int = 3) -> List[Dict[str, Any]]:
        """Generate multiple filing records for a company."""
        filings = []
        base_year = 2021
        
        for i in range(count):
            filings.append({
                "company_id": company_id,
                "ticker": f"TEST{i:03d}",
                "cik": f"000000000{i}",
                "accession_number": f"0000000000-{base_year + i}-{i:06d}",
                "form_type": "10-K",
                "filing_date": date(base_year + i, 10, 1),
                "report_date": date(base_year + i, 9, 30),
                "fiscal_year": base_year + i,
                "edgar_url": f"https://test.sec.gov/filing-{i}"
            })
        
        return filings

    @staticmethod
    def generate_qualitative_sections() -> Dict[str, str]:
        """Generate comprehensive qualitative section content."""
        return {
            "item_1_business": """
                Test Company Inc. is a leading technology company specializing in innovative
                software solutions and cloud services. Our primary products include enterprise
                software platforms, mobile applications, and data analytics tools. We serve
                customers across multiple industries including healthcare, finance, and retail.
                
                The company has experienced strong growth over the past three years, with
                revenue increasing by 25% annually. Our market position is strengthened by
                our proprietary technology and strong customer relationships. We continue
                to invest in research and development to maintain our competitive advantage.
            """,
            "item_1a_risk_factors": """
                The company faces various risks that could materially affect our business,
                financial condition, and results of operations. Key risk factors include:
                
                Market Competition: Intense competition in the technology sector may reduce
                our market share and pricing power. New entrants with innovative solutions
                pose ongoing competitive threats.
                
                Regulatory Changes: Changes in data privacy regulations, cybersecurity
                requirements, and international trade policies could increase compliance
                costs and operational complexity.
                
                Economic Uncertainty: Economic downturns or recessions could reduce customer
                spending on technology solutions, negatively impacting our revenue growth.
                
                Cybersecurity Threats: Security breaches or data incidents could damage our
                reputation and result in significant financial losses and legal liabilities.
            """,
            "item_7_mda": """
                Management's Discussion and Analysis of Financial Condition and Results
                of Operations provides insight into our financial performance and outlook.
                
                Fiscal Year 2023 Performance:
                Revenue increased 28% to $2.5 billion, driven by strong demand for our
                cloud services and enterprise software products. Gross margins improved
                to 72%, reflecting operational efficiencies and pricing optimization.
                
                Operating expenses increased 15% due to strategic investments in research
                and development and sales team expansion. Despite higher expenses, operating
                income grew 45% to $650 million.
                
                Cash Flow and Liquidity:
                Cash flow from operations was $800 million, providing strong liquidity for
                growth investments and strategic acquisitions. Our balance sheet remains
                strong with minimal debt and substantial cash reserves.
                
                Future Outlook:
                We expect continued growth driven by increasing adoption of our cloud
                platforms and expansion into new market segments. Management remains
                confident in our long-term growth strategy and market opportunities.
            """
        }

    @staticmethod
    def generate_sentiment_data() -> Dict[str, float]:
        """Generate sample sentiment analysis data."""
        return {
            "overall_sentiment": 0.65,
            "confidence": 0.82,
            "positive_score": 0.70,
            "negative_score": 0.15,
            "neutral_score": 0.15,
            "sentiment_label": "positive"
        }

    @staticmethod
    def generate_investment_analysis() -> Dict[str, Any]:
        """Generate comprehensive investment analysis data."""
        return {
            "ticker": "TEST001",
            "fiscal_year": 2023,
            "qualitative_score": 78.5,
            "sentiment_weight": 0.30,
            "risk_weight": 0.40,
            "opportunity_weight": 0.30,
            "recommendation": "buy",
            "confidence": 0.85,
            "target_price_adjustment": 12.5,
            "key_strengths": [
                "Strong revenue growth",
                "Excellent management team",
                "Market-leading products",
                "Strong financial position"
            ],
            "key_concerns": [
                "Increasing competition",
                "Regulatory uncertainty",
                "Economic headwinds"
            ],
            "risk_factors": [
                "Market volatility",
                "Technology disruption",
                "Customer concentration"
            ],
            "growth_opportunities": [
                "International expansion",
                "New product development",
                "Strategic acquisitions",
                "Market penetration"
            ]
        }


@pytest.fixture
def test_data_generator():
    """Provide test data generator instance."""
    return TestDataGenerator()


@pytest.fixture
def comprehensive_company_dataset(test_data_generator):
    """Generate a comprehensive company dataset."""
    return test_data_generator.generate_company_data(count=10)


@pytest.fixture
def comprehensive_filing_dataset(test_data_generator):
    """Generate a comprehensive filing dataset."""
    filings = []
    for i in range(5):  # 5 companies
        company_filings = test_data_generator.generate_filing_data(
            company_id=f"company-{i}", 
            count=3  # 3 filings each
        )
        filings.extend(company_filings)
    return filings


@pytest.fixture
def complex_qualitative_sections(test_data_generator):
    """Generate complex qualitative sections for testing."""
    return test_data_generator.generate_qualitative_sections()


@pytest.fixture
def mock_sec_api_responses():
    """Generate comprehensive SEC API response mocks."""
    return {
        "submissions_response": {
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
                        "0000320193-22-000108",
                        "0000320193-21-000010"
                    ],
                    "filingDate": [
                        "2023-10-27",
                        "2022-10-28", 
                        "2021-10-29"
                    ],
                    "reportDate": [
                        "2023-09-30",
                        "2022-09-24",
                        "2021-09-25"
                    ],
                    "acceptanceDateTime": [
                        "2023-10-27T18:01:14.000Z",
                        "2022-10-28T18:04:28.000Z",
                        "2021-10-29T18:02:37.000Z"
                    ],
                    "form": ["10-K", "10-K", "10-K"],
                    "fileNumber": ["001-36743", "001-36743", "001-36743"],
                    "filmNumber": ["231354297", "221354232", "211354639"]
                }
            }
        },
        "filing_html_response": """
            <!DOCTYPE html>
            <html>
            <head><title>Apple Inc. 10-K</title></head>
            <body>
                <div>
                    <p><strong>PART I</strong></p>
                    <p><strong>Item 1. Business</strong></p>
                    <p>Apple Inc. ("Apple," "we," "us" or "our") designs, manufactures and markets smartphones, personal computers, tablets, wearables and accessories worldwide. Apple also sells a range of related services.</p>
                    <p>The Company's fiscal year is the 52 or 53-week period that ends on the last Saturday of September.</p>
                </div>
                <div>
                    <p><strong>Item 1A. Risk Factors</strong></p>
                    <p>The following discussion of risk factors contains forward-looking statements.</p>
                    <p><strong>Global and regional economic conditions could materially adversely affect the Company.</strong></p>
                    <p>The Company's operations and performance depend significantly on global and regional economic conditions.</p>
                </div>
                <div>
                    <p><strong>Item 7. Management's Discussion and Analysis of Financial Condition and Results of Operations</strong></p>
                    <p>The following discussion should be read in conjunction with the consolidated financial statements.</p>
                    <p><strong>Products and Services Performance</strong></p>
                    <p>iPhone net sales increased during 2023 compared to 2022 due primarily to higher net sales of iPhone 14 models.</p>
                </div>
            </body>
            </html>
        """
    }


@pytest.fixture
def performance_test_data():
    """Generate data for performance testing."""
    large_text = """
        This is a large text document for performance testing. It contains multiple
        sentences and paragraphs to simulate real 10-K filing content. The content
        includes business descriptions, financial information, risk factors, and
        management discussion and analysis sections.
        
        Performance testing is crucial for ensuring the application can handle
        real-world data volumes efficiently. We test processing speed, memory usage,
        and scalability under various load conditions.
        
        The text processing pipeline must handle documents with thousands of words
        while maintaining reasonable response times and memory consumption patterns.
    """ * 100  # Create large document
    
    return {
        "large_document": large_text,
        "word_count": len(large_text.split()),
        "char_count": len(large_text),
        "expected_processing_time": 10.0  # seconds
    }


@pytest.fixture
def temp_test_files(tmp_path):
    """Create temporary test files."""
    # Create test data directory structure
    data_dir = tmp_path / "test_data"
    data_dir.mkdir()
    
    # Create sample files
    sample_filing = data_dir / "sample_filing.json"
    sample_filing.write_text(json.dumps({
        "ticker": "AAPL",
        "cik": "0000320193",
        "accession_number": "0000320193-23-000105",
        "sections": {
            "business": "Sample business content",
            "risk_factors": "Sample risk content"
        }
    }))
    
    sample_companies = data_dir / "companies.csv"
    companies_df = pd.DataFrame([
        {"ticker": "AAPL", "company_name": "Apple Inc.", "cik": "0000320193"},
        {"ticker": "MSFT", "company_name": "Microsoft Corp.", "cik": "0000789019"}
    ])
    companies_df.to_csv(sample_companies, index=False)
    
    return {
        "data_dir": data_dir,
        "sample_filing": sample_filing,
        "sample_companies": sample_companies
    }


@pytest.fixture
def error_scenarios():
    """Generate various error scenarios for testing."""
    return {
        "network_errors": [
            "Connection timeout",
            "DNS resolution failed",
            "HTTP 503 Service Unavailable",
            "HTTP 429 Too Many Requests"
        ],
        "data_errors": [
            {"malformed": "json"},
            None,
            "",
            "not-json-at-all"
        ],
        "database_errors": [
            "Connection refused",
            "Table does not exist", 
            "Permission denied",
            "Constraint violation"
        ],
        "api_errors": [
            {"error": "Invalid API key"},
            {"error": "Rate limit exceeded"},
            {"error": "Service temporarily unavailable"}
        ]
    }


class MockResponseBuilder:
    """Builder for creating complex mock responses."""
    
    def __init__(self):
        self.response_data = {}
    
    def with_status(self, status: int):
        """Set response status."""
        self.response_data["status"] = status
        return self
    
    def with_json(self, data: dict):
        """Set JSON response data."""
        self.response_data["json_data"] = data
        return self
    
    def with_text(self, text: str):
        """Set text response data."""
        self.response_data["text_data"] = text
        return self
    
    def with_headers(self, headers: dict):
        """Set response headers."""
        self.response_data["headers"] = headers
        return self
    
    def build(self):
        """Build the mock response."""
        from tests.conftest import MockHttpResponse
        return MockHttpResponse(**self.response_data)


@pytest.fixture
def mock_response_builder():
    """Provide mock response builder."""
    return MockResponseBuilder


class DatabaseTestHelper:
    """Helper for database testing operations."""
    
    def __init__(self, db_client):
        self.db_client = db_client
        self.created_records = []
    
    async def create_test_company(self, **kwargs):
        """Create a test company and track for cleanup."""
        from src.database.schema import Company
        
        defaults = {
            "ticker": f"TEST{int(datetime.now().timestamp()) % 10000}",
            "cik": f"{int(datetime.now().timestamp()) % 10000000000:010d}",
            "company_name": "Test Company",
            "exchange": "TEST"
        }
        defaults.update(kwargs)
        
        company = Company(**defaults)
        result = await self.db_client.insert_company(company)
        
        if result and "id" in result:
            self.created_records.append(("companies", "id", result["id"]))
        
        return result
    
    async def cleanup(self):
        """Clean up created test records."""
        for table, id_field, record_id in self.created_records:
            try:
                self.db_client.client.table(table).delete().eq(id_field, record_id).execute()
            except Exception:
                pass  # Ignore cleanup errors
        
        self.created_records.clear()


@pytest.fixture
async def db_test_helper(mock_supabase_client):
    """Provide database test helper."""
    from src.database.connection import SupabaseClient
    
    with patch("src.database.connection.create_client", return_value=mock_supabase_client):
        db_client = SupabaseClient()
        helper = DatabaseTestHelper(db_client)
        
        yield helper
        
        # Cleanup after test
        await helper.cleanup()


# Performance monitoring utilities
class PerformanceMonitor:
    """Monitor performance during tests."""
    
    def __init__(self):
        self.start_time = None
        self.metrics = {}
    
    def start(self):
        """Start performance monitoring."""
        self.start_time = datetime.now()
        return self
    
    def stop(self, operation_name: str = "operation"):
        """Stop monitoring and record metrics."""
        if self.start_time:
            duration = (datetime.now() - self.start_time).total_seconds()
            self.metrics[operation_name] = duration
        return self
    
    def assert_performance(self, operation_name: str, max_duration: float):
        """Assert operation completed within time limit."""
        if operation_name in self.metrics:
            actual_duration = self.metrics[operation_name]
            assert actual_duration <= max_duration, \
                f"{operation_name} took {actual_duration:.2f}s, expected <= {max_duration}s"


@pytest.fixture
def performance_monitor():
    """Provide performance monitoring."""
    return PerformanceMonitor()