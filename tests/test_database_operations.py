"""Comprehensive tests for database operations and Supabase integration."""

import pytest
import asyncio
import time
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime, date
from uuid import uuid4

from tests.conftest import (
    unit_test, integration_test, requires_db,
    MockSupabaseResponse, skip_if_no_integration
)


@unit_test
class TestSupabaseClientUnit:
    """Unit tests for SupabaseClient."""

    @pytest.fixture
    def supabase_client(self, mock_supabase_client):
        """Create SupabaseClient with mocked Supabase client."""
        with patch("src.database.connection.create_client", return_value=mock_supabase_client):
            from src.database.connection import SupabaseClient
            return SupabaseClient()

    def test_client_initialization(self, supabase_client):
        """Test SupabaseClient initialization."""
        assert supabase_client.client is not None

    @pytest.mark.asyncio
    async def test_insert_company_success(self, supabase_client, sample_company_data):
        """Test successful company insertion."""
        from src.database.schema import Company
        
        # Setup mock response
        expected_response = [{"id": "test-company-id", **sample_company_data}]
        supabase_client.client.table.return_value.insert.return_value.execute.return_value = \
            MockSupabaseResponse(expected_response)
        
        company = Company(**sample_company_data)
        result = await supabase_client.insert_company(company)
        
        assert result["id"] == "test-company-id"
        supabase_client.client.table.assert_called_with("companies")

    @pytest.mark.asyncio
    async def test_upsert_company_success(self, supabase_client, sample_company_data):
        """Test successful company upsert."""
        from src.database.schema import Company
        
        # Setup mock response
        expected_response = [{"id": "test-company-id", **sample_company_data}]
        supabase_client.client.table.return_value.upsert.return_value.execute.return_value = \
            MockSupabaseResponse(expected_response)
        
        company = Company(**sample_company_data)
        result = await supabase_client.upsert_company(company)
        
        assert result["id"] == "test-company-id"
        supabase_client.client.table.assert_called_with("companies")

    @pytest.mark.asyncio
    async def test_get_company_by_ticker_found(self, supabase_client, sample_company_data):
        """Test retrieving company by ticker when found."""
        expected_response = [sample_company_data]
        supabase_client.client.table.return_value.select.return_value.eq.return_value.execute.return_value = \
            MockSupabaseResponse(expected_response)
        
        result = await supabase_client.get_company_by_ticker("AAPL")
        
        assert result == sample_company_data
        supabase_client.client.table.assert_called_with("companies")

    @pytest.mark.asyncio
    async def test_get_company_by_ticker_not_found(self, supabase_client):
        """Test retrieving company by ticker when not found."""
        supabase_client.client.table.return_value.select.return_value.eq.return_value.execute.return_value = \
            MockSupabaseResponse([])
        
        result = await supabase_client.get_company_by_ticker("INVALID")
        
        assert result is None

    @pytest.mark.asyncio
    async def test_insert_filing_success(self, supabase_client, sample_filing_data):
        """Test successful filing insertion."""
        from src.database.schema import Filing
        
        expected_response = [{"id": "test-filing-id", **sample_filing_data}]
        supabase_client.client.table.return_value.insert.return_value.execute.return_value = \
            MockSupabaseResponse(expected_response)
        
        filing = Filing(**sample_filing_data)
        result = await supabase_client.insert_filing(filing)
        
        assert result["id"] == "test-filing-id"
        supabase_client.client.table.assert_called_with("filings")

    @pytest.mark.asyncio
    async def test_get_filings_by_company_success(self, supabase_client):
        """Test retrieving filings by company."""
        expected_response = [
            {"id": "filing-1", "ticker": "AAPL", "fiscal_year": 2023},
            {"id": "filing-2", "ticker": "AAPL", "fiscal_year": 2022}
        ]
        supabase_client.client.table.return_value.select.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value = \
            MockSupabaseResponse(expected_response)
        
        result = await supabase_client.get_filings_by_company("AAPL", limit=10)
        
        assert len(result) == 2
        assert result[0]["fiscal_year"] == 2023

    @pytest.mark.asyncio
    async def test_insert_qualitative_section_success(self, supabase_client):
        """Test successful qualitative section insertion."""
        from src.database.schema import QualitativeSection
        
        section_data = {
            "filing_id": "test-filing-id",
            "section_name": "item_1_business",
            "section_title": "Item 1. Business",
            "content": "Sample business content",
            "word_count": 3,
            "char_count": 23
        }
        
        expected_response = [{"id": "test-section-id", **section_data}]
        supabase_client.client.table.return_value.insert.return_value.execute.return_value = \
            MockSupabaseResponse(expected_response)
        
        section = QualitativeSection(**section_data)
        result = await supabase_client.insert_qualitative_section(section)
        
        assert result["id"] == "test-section-id"
        supabase_client.client.table.assert_called_with("qualitative_sections")

    @pytest.mark.asyncio
    async def test_insert_sentiment_analysis_success(self, supabase_client):
        """Test successful sentiment analysis insertion."""
        from src.database.schema import SentimentAnalysis
        
        sentiment_data = {
            "section_id": "test-section-id",
            "filing_id": "test-filing-id",
            "overall_sentiment": 0.75,
            "confidence": 0.85,
            "positive_score": 0.8,
            "negative_score": 0.1,
            "neutral_score": 0.1,
            "sentiment_label": "positive",
            "model_used": "textblob"
        }
        
        expected_response = [{"id": "test-sentiment-id", **sentiment_data}]
        supabase_client.client.table.return_value.insert.return_value.execute.return_value = \
            MockSupabaseResponse(expected_response)
        
        sentiment = SentimentAnalysis(**sentiment_data)
        result = await supabase_client.insert_sentiment_analysis(sentiment)
        
        assert result["id"] == "test-sentiment-id"
        supabase_client.client.table.assert_called_with("sentiment_analysis")

    @pytest.mark.asyncio
    async def test_get_processing_stats_success(self, supabase_client):
        """Test retrieving processing statistics."""
        # Mock responses for different statistical queries
        stats_responses = [
            MockSupabaseResponse([{"count": 50}]),  # total_companies
            MockSupabaseResponse([{"count": 150}]),  # total_filings
            MockSupabaseResponse([{"count": 120}]),  # completed_filings
            MockSupabaseResponse([{"count": 5}]),   # failed_filings
        ]
        
        # Setup mock to return different responses for each call
        supabase_client.client.table.return_value.select.return_value.execute.side_effect = stats_responses
        
        result = await supabase_client.get_processing_stats()
        
        assert result["total_companies"] == 50
        assert result["total_filings"] == 150
        assert result["completed_filings"] == 120
        assert result["failed_filings"] == 5

    @pytest.mark.asyncio
    async def test_get_investment_recommendations_success(self, supabase_client):
        """Test retrieving investment recommendations."""
        expected_response = [
            {
                "ticker": "AAPL",
                "recommendation": "buy",
                "qualitative_score": 85.5,
                "confidence": 0.9
            },
            {
                "ticker": "MSFT", 
                "recommendation": "strong_buy",
                "qualitative_score": 92.3,
                "confidence": 0.85
            }
        ]
        
        supabase_client.client.table.return_value.select.return_value.order.return_value.limit.return_value.execute.return_value = \
            MockSupabaseResponse(expected_response)
        
        result = await supabase_client.get_investment_recommendations(limit=10)
        
        assert len(result) == 2
        assert result[0]["ticker"] == "AAPL"
        assert result[1]["recommendation"] == "strong_buy"

    @pytest.mark.asyncio
    async def test_database_error_handling(self, supabase_client):
        """Test database error handling."""
        # Setup mock to raise exception
        supabase_client.client.table.return_value.select.side_effect = Exception("Database error")
        
        result = await supabase_client.get_company_by_ticker("AAPL")
        
        # Should return None on error
        assert result is None

    @pytest.mark.asyncio
    async def test_batch_insert_companies_success(self, supabase_client):
        """Test batch insertion of companies."""
        companies_data = [
            {"ticker": "AAPL", "cik": "0000320193", "company_name": "Apple Inc.", "exchange": "NASDAQ"},
            {"ticker": "MSFT", "cik": "0000789019", "company_name": "Microsoft Corporation", "exchange": "NASDAQ"},
        ]
        
        expected_response = [{"id": f"test-id-{i}", **company} for i, company in enumerate(companies_data)]
        supabase_client.client.table.return_value.upsert.return_value.execute.return_value = \
            MockSupabaseResponse(expected_response)
        
        result = await supabase_client.batch_upsert_companies(companies_data)
        
        assert len(result) == 2
        assert all("id" in company for company in result)


@integration_test
@requires_db
class TestSupabaseClientIntegration:
    """Integration tests for SupabaseClient with real database."""

    @pytest.fixture
    def supabase_client(self):
        """Create SupabaseClient for integration testing."""
        from src.database.connection import SupabaseClient
        return SupabaseClient()

    @skip_if_no_integration()
    @pytest.mark.asyncio
    async def test_real_database_connection(self, supabase_client):
        """Test real database connection."""
        # Simple connectivity test
        try:
            # Try to select from companies table (should exist)
            response = supabase_client.client.table("companies").select("ticker").limit(1).execute()
            assert response is not None
            assert hasattr(response, "data")
        except Exception as e:
            pytest.fail(f"Database connection failed: {e}")

    @skip_if_no_integration()
    @pytest.mark.asyncio
    async def test_real_company_crud_operations(self, supabase_client):
        """Test real CRUD operations for companies."""
        # Create unique test data
        test_ticker = f"TEST{int(time.time())%10000:04d}"
        test_cik = f"{int(time.time())%10000000000:010d}"
        
        # Test data
        company_data = {
            "ticker": test_ticker,
            "cik": test_cik,
            "company_name": "Test Company Inc.",
            "exchange": "TEST",
            "sector": "Technology",
            "industry": "Testing"
        }
        
        try:
            # Test INSERT
            from src.database.schema import Company
            company = Company(**company_data)
            
            insert_result = await supabase_client.insert_company(company)
            assert insert_result is not None
            assert "id" in insert_result
            company_id = insert_result["id"]
            
            # Test SELECT
            retrieved = await supabase_client.get_company_by_ticker(test_ticker)
            assert retrieved is not None
            assert retrieved["ticker"] == test_ticker
            assert retrieved["company_name"] == "Test Company Inc."
            
            # Test UPDATE via UPSERT
            company_data["market_cap"] = 1000000000
            updated_company = Company(**company_data)
            
            upsert_result = await supabase_client.upsert_company(updated_company)
            assert upsert_result is not None
            
            # Verify update
            updated_retrieved = await supabase_client.get_company_by_ticker(test_ticker)
            assert updated_retrieved["market_cap"] == 1000000000
            
        finally:
            # Cleanup - delete test data
            try:
                supabase_client.client.table("companies").delete().eq("ticker", test_ticker).execute()
            except Exception:
                pass  # Ignore cleanup errors

    @skip_if_no_integration()
    @pytest.mark.asyncio
    async def test_real_filing_operations(self, supabase_client):
        """Test real filing operations."""
        # First create a test company
        test_ticker = f"TFIL{int(time.time())%1000:03d}"
        test_cik = f"{int(time.time())%10000000000:010d}"
        
        company_data = {
            "ticker": test_ticker,
            "cik": test_cik,
            "company_name": "Test Filing Company",
            "exchange": "TEST"
        }
        
        try:
            from src.database.schema import Company, Filing
            
            # Create company
            company = Company(**company_data)
            company_result = await supabase_client.insert_company(company)
            company_id = company_result["id"]
            
            # Create filing
            filing_data = {
                "company_id": company_id,
                "ticker": test_ticker,
                "cik": test_cik,
                "accession_number": f"test-acc-{int(time.time())}",
                "form_type": "10-K",
                "filing_date": date.today(),
                "report_date": date.today(),
                "fiscal_year": 2023,
                "edgar_url": "https://test.sec.gov/test-filing"
            }
            
            filing = Filing(**filing_data)
            filing_result = await supabase_client.insert_filing(filing)
            
            assert filing_result is not None
            assert "id" in filing_result
            
            # Test retrieval
            filings = await supabase_client.get_filings_by_company(test_ticker, limit=5)
            assert len(filings) >= 1
            assert any(f["accession_number"] == filing_data["accession_number"] for f in filings)
            
        finally:
            # Cleanup
            try:
                supabase_client.client.table("filings").delete().eq("ticker", test_ticker).execute()
                supabase_client.client.table("companies").delete().eq("ticker", test_ticker).execute()
            except Exception:
                pass

    @skip_if_no_integration()
    @pytest.mark.asyncio
    async def test_database_performance_batch_operations(self, supabase_client):
        """Test database performance with batch operations."""
        # Generate test data
        timestamp = int(time.time())
        companies_data = []
        
        for i in range(10):
            companies_data.append({
                "ticker": f"PERF{timestamp}{i:02d}",
                "cik": f"{timestamp + i:010d}",
                "company_name": f"Performance Test Company {i}",
                "exchange": "TEST"
            })
        
        try:
            # Time the batch operation
            start_time = datetime.now()
            result = await supabase_client.batch_upsert_companies(companies_data)
            end_time = datetime.now()
            
            duration = (end_time - start_time).total_seconds()
            
            # Should complete reasonably quickly (less than 10 seconds for 10 companies)
            assert duration < 10.0
            assert len(result) == 10
            
        finally:
            # Cleanup
            try:
                for company in companies_data:
                    supabase_client.client.table("companies").delete().eq("ticker", company["ticker"]).execute()
            except Exception:
                pass

    @skip_if_no_integration()
    @pytest.mark.asyncio  
    async def test_schema_validation_integration(self, supabase_client):
        """Test that database schema matches Pydantic models."""
        from src.database.schema import Company, Filing, QualitativeSection
        
        # Test with valid data that should pass both Pydantic and DB validation
        test_ticker = f"SCHM{int(time.time())%1000:03d}"
        
        company_data = {
            "ticker": test_ticker,
            "cik": f"{int(time.time())%10000000000:010d}",
            "company_name": "Schema Test Company",
            "exchange": "NASDAQ",
            "sector": "Technology"
        }
        
        try:
            # This should work - valid Pydantic model
            company = Company(**company_data)
            assert company.ticker == test_ticker
            
            # This should work - valid database insert
            result = await supabase_client.insert_company(company)
            assert result is not None
            
        finally:
            # Cleanup
            try:
                supabase_client.client.table("companies").delete().eq("ticker", test_ticker).execute()
            except Exception:
                pass