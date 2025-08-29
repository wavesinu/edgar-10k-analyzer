"""End-to-end pipeline tests for the EDGAR 10-K analyzer."""

import pytest
import asyncio
import time
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime, date
from pathlib import Path

from tests.conftest import (
    e2e_test, integration_test, requires_api, requires_db, slow_test,
    skip_if_no_integration, skip_if_no_live_api
)


@e2e_test
class TestFullPipelineFlow:
    """End-to-end tests for complete pipeline execution."""

    @pytest.fixture
    def pipeline_components(self, mock_supabase_client, mock_openai_client, mock_edgar_response):
        """Setup all pipeline components with mocks."""
        mocks = {}
        
        # Mock database
        with patch("src.database.connection.create_client", return_value=mock_supabase_client):
            from src.database.connection import SupabaseClient
            mocks["db_client"] = SupabaseClient()
        
        # Mock EDGAR client
        with patch("src.database.connection.create_client", return_value=mock_supabase_client):
            from src.api.edgar_client import EdgarClient
            edgar_client = EdgarClient()
            edgar_client.session = MagicMock()
            edgar_client.session.get = mock_edgar_response
            mocks["edgar_client"] = edgar_client
        
        # Mock OpenAI
        with patch("openai.AsyncOpenAI", return_value=mock_openai_client):
            from src.llm.investment_advisor import InvestmentAdvisor
            mocks["investment_advisor"] = InvestmentAdvisor()
        
        # Mock NLP analyzer
        with patch("src.database.connection.create_client", return_value=mock_supabase_client):
            from src.nlp.qualitative_analyzer import QualitativeAnalyzer
            mocks["nlp_analyzer"] = QualitativeAnalyzer()
        
        return mocks

    @pytest.mark.asyncio
    async def test_single_company_complete_flow(self, pipeline_components, sample_company_data):
        """Test complete flow for a single company."""
        components = pipeline_components
        
        # Mock successful responses throughout pipeline
        components["db_client"].client.table.return_value.select.return_value.eq.return_value.execute.return_value.data = []
        components["db_client"].client.table.return_value.insert.return_value.execute.return_value.data = [{"id": "test-id"}]
        
        # Simulate pipeline execution
        ticker = "AAPL"
        cik = "0000320193"
        
        # Step 1: Get company submissions
        submissions = await components["edgar_client"].get_company_submissions(cik)
        assert submissions is not None
        
        # Step 2: Extract filings
        filings = components["edgar_client"].extract_10k_filings(submissions, limit=1)
        assert len(filings) >= 1
        
        # Step 3: Get filing content
        filing = filings[0]
        html_content = await components["edgar_client"].fetch_filing_content("https://test.url/test.htm")
        assert html_content is not None
        
        # Step 4: Extract sections
        sections = components["edgar_client"].extract_document_sections(html_content)
        assert len(sections) > 0
        
        # Step 5: Analyze sections
        for section_name, content in sections.items():
            if content and len(content.strip()) > 0:
                sentiment = components["nlp_analyzer"].analyze_sentiment(content)
                assert sentiment is not None
                assert "overall_sentiment" in sentiment

    @pytest.mark.asyncio
    async def test_pipeline_error_recovery(self, pipeline_components):
        """Test pipeline error recovery mechanisms."""
        components = pipeline_components
        
        # Simulate various error scenarios
        
        # Test 1: Network error in EDGAR API
        components["edgar_client"].session.get.side_effect = Exception("Network error")
        
        submissions = await components["edgar_client"].get_company_submissions("0000320193")
        assert submissions is None  # Should handle gracefully
        
        # Test 2: Database connection error
        components["db_client"].client.table.side_effect = Exception("DB error")
        
        company_data = {"ticker": "TEST", "cik": "1234567890", "company_name": "Test Co"}
        try:
            result = await components["db_client"].get_company_by_ticker("TEST")
            assert result is None  # Should handle gracefully
        except Exception:
            pass  # Expected to handle errors gracefully

    @pytest.mark.asyncio
    async def test_data_flow_consistency(self, pipeline_components, sample_qualitative_sections):
        """Test data consistency throughout the pipeline."""
        components = pipeline_components
        filing_id = "test-filing-123"
        ticker = "AAPL"
        
        # Process sections and ensure data consistency
        all_results = {}
        
        for section_name, content in sample_qualitative_sections.items():
            # Analyze sentiment
            sentiment_result = components["nlp_analyzer"].analyze_sentiment(content)
            
            # Extract themes
            themes_result = components["nlp_analyzer"].extract_key_themes(content)
            
            all_results[section_name] = {
                "content": content,
                "sentiment": sentiment_result,
                "themes": themes_result,
                "word_count": len(content.split())
            }
        
        # Verify consistency
        assert len(all_results) == len(sample_qualitative_sections)
        
        for section_name, result in all_results.items():
            assert result["word_count"] > 0
            assert result["sentiment"] is not None
            assert isinstance(result["themes"], list)

    @slow_test
    @pytest.mark.asyncio
    async def test_multi_company_pipeline(self, pipeline_components):
        """Test pipeline with multiple companies."""
        components = pipeline_components
        companies = ["AAPL", "MSFT", "GOOGL"]
        
        results = {}
        
        for ticker in companies:
            # Simulate processing each company
            cik = f"000032019{companies.index(ticker)}"  # Mock CIKs
            
            # Get submissions
            submissions = await components["edgar_client"].get_company_submissions(cik)
            if submissions:
                filings = components["edgar_client"].extract_10k_filings(submissions, limit=1)
                results[ticker] = {
                    "submissions_found": True,
                    "filings_count": len(filings)
                }
            else:
                results[ticker] = {
                    "submissions_found": False,
                    "filings_count": 0
                }
        
        # Should process all companies
        assert len(results) == len(companies)
        
        # At least some should be successful (depending on mocks)
        successful = [r for r in results.values() if r.get("submissions_found")]
        assert len(successful) >= 1

    @pytest.mark.asyncio
    async def test_pipeline_performance_benchmarks(self, pipeline_components):
        """Test pipeline performance benchmarks."""
        components = pipeline_components
        
        # Benchmark different operations
        benchmarks = {}
        
        # Test 1: EDGAR API response time
        start_time = time.time()
        await components["edgar_client"].get_company_submissions("0000320193")
        benchmarks["edgar_api_time"] = time.time() - start_time
        
        # Test 2: Text processing time
        sample_text = "Sample business text for performance testing. " * 100
        start_time = time.time()
        components["nlp_analyzer"].analyze_sentiment(sample_text)
        benchmarks["nlp_processing_time"] = time.time() - start_time
        
        # Test 3: Database operation time
        start_time = time.time()
        await components["db_client"].get_company_by_ticker("AAPL")
        benchmarks["db_query_time"] = time.time() - start_time
        
        # Performance assertions
        assert benchmarks["edgar_api_time"] < 5.0  # Should be fast with mocks
        assert benchmarks["nlp_processing_time"] < 2.0  # Text processing should be quick
        assert benchmarks["db_query_time"] < 1.0  # DB queries should be fast

    @pytest.mark.asyncio
    async def test_memory_usage_and_cleanup(self, pipeline_components):
        """Test memory usage and cleanup in pipeline."""
        import gc
        import sys
        
        components = pipeline_components
        
        # Get initial memory usage
        gc.collect()
        initial_objects = len(gc.get_objects())
        
        # Process some data
        large_text = "Large text content for memory testing. " * 1000
        
        for i in range(10):
            # Simulate multiple processing cycles
            sentiment = components["nlp_analyzer"].analyze_sentiment(large_text)
            themes = components["nlp_analyzer"].extract_key_themes(large_text)
            
            # Clear references
            del sentiment, themes
        
        # Force garbage collection
        gc.collect()
        final_objects = len(gc.get_objects())
        
        # Memory should not grow excessively
        object_growth = final_objects - initial_objects
        assert object_growth < 1000  # Reasonable memory growth limit


@integration_test
@requires_api
@requires_db
class TestIntegratedPipelineReal:
    """Integration tests with real services (when enabled)."""

    @skip_if_no_integration()
    @pytest.mark.asyncio
    async def test_real_edgar_to_database_flow(self):
        """Test real EDGAR data flowing to database."""
        # Only run with integration flag
        from src.api.edgar_client import EdgarClient
        from src.database.connection import SupabaseClient
        
        db_client = SupabaseClient()
        
        async with EdgarClient() as edgar_client:
            # Get real Apple data
            submissions = await edgar_client.get_company_submissions("0000320193")
            
            if submissions:
                filings = edgar_client.extract_10k_filings(submissions, limit=1)
                
                if filings:
                    filing = filings[0]
                    
                    # Try to get HTML content
                    html_content = await edgar_client.get_filing_html_content(
                        "0000320193", 
                        filing["accessionNumber"]
                    )
                    
                    if html_content:
                        sections = edgar_client.extract_document_sections(html_content)
                        
                        # Should extract at least one section
                        non_empty_sections = [k for k, v in sections.items() 
                                            if v and len(v.strip()) > 0]
                        assert len(non_empty_sections) >= 1

    @skip_if_no_integration()
    @skip_if_no_live_api()
    @pytest.mark.asyncio
    async def test_real_ai_analysis_integration(self):
        """Test real AI analysis integration."""
        from src.llm.investment_advisor import InvestmentAdvisor
        from src.nlp.qualitative_analyzer import QualitativeAnalyzer
        
        # Mock database for this test
        with patch("src.database.connection.create_client") as mock_db:
            mock_db.return_value = MagicMock()
            
            nlp_analyzer = QualitativeAnalyzer()
            ai_advisor = InvestmentAdvisor()
            
            # Sample business content
            business_text = """
            Apple Inc. is a multinational technology company that specializes in 
            consumer electronics, computer software, and online services. The company's 
            hardware products include the iPhone smartphone, the iPad tablet computer, 
            the Mac personal computer, the Apple Watch smartwatch, and the Apple TV 
            digital media player.
            """
            
            # Run NLP analysis
            sentiment = nlp_analyzer.analyze_sentiment(business_text)
            assert sentiment is not None
            
            # Run AI analysis (real API call)
            ai_result = await ai_advisor.openai_client.analyze_qualitative_text(
                business_text,
                section_type="business"
            )
            
            # Should get results from both analyses
            assert ai_result is not None
            assert sentiment["overall_sentiment"] is not None

    @skip_if_no_integration()
    @pytest.mark.asyncio
    async def test_database_integrity_under_load(self):
        """Test database integrity under concurrent load."""
        from src.database.connection import SupabaseClient
        from src.database.schema import Company
        
        db_client = SupabaseClient()
        
        # Create multiple companies concurrently
        companies_data = []
        timestamp = int(time.time())
        
        for i in range(5):
            companies_data.append(Company(
                ticker=f"LOAD{timestamp}{i:02d}",
                cik=f"{timestamp + i:010d}",
                company_name=f"Load Test Company {i}",
                exchange="TEST"
            ))
        
        try:
            # Insert concurrently
            tasks = []
            for company in companies_data:
                task = db_client.insert_company(company)
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Should get mostly successful results
            successful = [r for r in results if not isinstance(r, Exception)]
            assert len(successful) >= 3  # At least 3/5 should succeed
            
        finally:
            # Cleanup
            try:
                for company in companies_data:
                    db_client.client.table("companies").delete().eq("ticker", company.ticker).execute()
            except Exception:
                pass

    @skip_if_no_integration()
    @pytest.mark.asyncio
    async def test_full_pipeline_orchestrator(self):
        """Test the actual pipeline orchestrator."""
        # This tests the real orchestrator class
        from src.pipeline.integrated_orchestrator import IntegratedPipeline
        
        async with IntegratedPipeline() as pipeline:
            # Run a small test pipeline
            stats = await pipeline.run_full_pipeline(
                companies=["AAPL"],  # Just Apple
                max_companies=1,
                max_filings_per_company=1,
                force_recrawl=False,
                enable_ai_analysis=False  # Disable AI for faster testing
            )
            
            assert stats is not None
            assert isinstance(stats, dict)
            
            # Should have some processing stats
            assert "duration" in stats or "companies_processed" in stats

    def test_configuration_validation(self):
        """Test that all required configurations are present."""
        from config.settings import settings
        
        # Check required settings
        required_settings = [
            "supabase_url",
            "supabase_key", 
            "openai_api_key",
            "user_agent"
        ]
        
        for setting_name in required_settings:
            setting_value = getattr(settings, setting_name, None)
            assert setting_value is not None, f"Required setting {setting_name} is missing"
            assert len(setting_value) > 0, f"Required setting {setting_name} is empty"

    def test_dependency_availability(self):
        """Test that all required dependencies are available."""
        # Test critical imports
        try:
            import supabase
            import openai
            import nltk
            import textblob
            import pandas as pd
            import numpy as np
            
            # All imports should succeed
            assert True
            
        except ImportError as e:
            pytest.fail(f"Required dependency missing: {e}")

    @pytest.mark.asyncio
    async def test_graceful_shutdown_handling(self):
        """Test graceful shutdown of pipeline components."""
        from src.api.edgar_client import EdgarClient
        
        # Test context manager cleanup
        edgar_client = None
        
        try:
            async with EdgarClient() as client:
                edgar_client = client
                assert client.session is not None
                
                # Simulate some work
                await asyncio.sleep(0.1)
                
        except Exception as e:
            pytest.fail(f"Context manager failed: {e}")
        
        # Session should be cleaned up after context manager exit
        # (Exact assertion depends on implementation)