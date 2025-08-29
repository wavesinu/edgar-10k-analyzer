"""Comprehensive tests for AI analysis and OpenAI integration."""

import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock
import json
from datetime import datetime

from tests.conftest import (
    unit_test, integration_test, requires_api,
    skip_if_no_integration, skip_if_no_live_api
)


@unit_test
class TestOpenAIFinancialAnalystUnit:
    """Unit tests for OpenAI client."""

    @pytest.fixture
    def openai_client(self, mock_openai_client):
        """Create OpenAI client with mocked API."""
        with patch("openai.AsyncOpenAI", return_value=mock_openai_client):
            from src.llm.openai_client import OpenAIFinancialAnalyst
            return OpenAIFinancialAnalyst()

    @pytest.mark.asyncio
    async def test_client_initialization(self, openai_client):
        """Test OpenAI client initialization."""
        assert openai_client is not None
        assert openai_client.model == "gpt-4-turbo-preview"

    @pytest.mark.asyncio
    async def test_chat_completion_success(self, openai_client, mock_openai_client):
        """Test successful chat completion."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "This is a positive financial analysis."
        mock_response.usage.total_tokens = 150
        mock_response.usage.prompt_tokens = 100
        mock_response.usage.completion_tokens = 50
        
        mock_openai_client.chat.completions.create = AsyncMock(return_value=mock_response)
        
        result = await openai_client.chat_completion(
            messages=[{"role": "user", "content": "Analyze this text"}],
            max_tokens=500
        )
        
        assert result is not None
        assert result.choices[0].message.content == "This is a positive financial analysis."
        assert result.usage.total_tokens == 150

    @pytest.mark.asyncio
    async def test_chat_completion_error_handling(self, openai_client, mock_openai_client):
        """Test chat completion error handling."""
        # Setup mock to raise exception
        mock_openai_client.chat.completions.create = AsyncMock(side_effect=Exception("API Error"))
        
        result = await openai_client.chat_completion(
            messages=[{"role": "user", "content": "Test"}]
        )
        
        assert result is None

    @pytest.mark.asyncio
    async def test_analyze_qualitative_text_success(self, openai_client, mock_openai_client):
        """Test qualitative text analysis."""
        # Setup mock response with structured analysis
        analysis_response = {
            "sentiment": "positive",
            "key_insights": ["Strong growth", "Good management"],
            "risk_factors": ["Market competition"],
            "investment_recommendation": "buy"
        }
        
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = json.dumps(analysis_response)
        
        mock_openai_client.chat.completions.create = AsyncMock(return_value=mock_response)
        
        result = await openai_client.analyze_qualitative_text(
            "Sample business description text",
            section_type="business"
        )
        
        assert result is not None
        assert "sentiment" in result
        assert result["sentiment"] == "positive"

    @pytest.mark.asyncio
    async def test_generate_investment_recommendation_success(self, openai_client, mock_openai_client):
        """Test investment recommendation generation."""
        recommendation = {
            "recommendation": "BUY",
            "confidence": 0.85,
            "reasoning": "Strong fundamentals and growth prospects"
        }
        
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = json.dumps(recommendation)
        
        mock_openai_client.chat.completions.create = AsyncMock(return_value=mock_response)
        
        analysis_data = {
            "business_sentiment": 0.8,
            "risk_score": 0.3,
            "growth_indicators": 0.9
        }
        
        result = await openai_client.generate_investment_recommendation(
            ticker="AAPL",
            analysis_data=analysis_data
        )
        
        assert result is not None
        assert result["recommendation"] == "BUY"
        assert result["confidence"] == 0.85

    @pytest.mark.asyncio
    async def test_token_counting(self, openai_client):
        """Test token counting functionality."""
        text = "This is a test message for token counting."
        
        token_count = openai_client.count_tokens(text)
        
        assert isinstance(token_count, int)
        assert token_count > 0

    @pytest.mark.asyncio
    async def test_prompt_construction(self, openai_client):
        """Test prompt construction for different analysis types."""
        # Test business analysis prompt
        business_prompt = openai_client.construct_analysis_prompt(
            text="Sample business text",
            section_type="business",
            ticker="AAPL"
        )
        
        assert "business" in business_prompt.lower()
        assert "AAPL" in business_prompt
        
        # Test risk factors prompt
        risk_prompt = openai_client.construct_analysis_prompt(
            text="Sample risk text",
            section_type="risk_factors", 
            ticker="MSFT"
        )
        
        assert "risk" in risk_prompt.lower()
        assert "MSFT" in risk_prompt


@unit_test
class TestInvestmentAdvisorUnit:
    """Unit tests for InvestmentAdvisor."""

    @pytest.fixture
    def investment_advisor(self, mock_openai_client, mock_supabase_client):
        """Create InvestmentAdvisor with mocked dependencies."""
        with patch("openai.AsyncOpenAI", return_value=mock_openai_client), \
             patch("src.database.connection.create_client", return_value=mock_supabase_client):
            from src.llm.investment_advisor import InvestmentAdvisor
            return InvestmentAdvisor()

    @pytest.mark.asyncio
    async def test_analyze_filing_sections_success(self, investment_advisor, mock_openai_client, sample_qualitative_sections):
        """Test filing sections analysis."""
        # Setup mock AI response
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = json.dumps({
            "sentiment": "positive",
            "key_themes": ["growth", "innovation"],
            "risk_assessment": "moderate",
            "management_quality": "strong"
        })
        
        mock_openai_client.chat.completions.create = AsyncMock(return_value=mock_response)
        
        result = await investment_advisor.analyze_filing_sections(
            sections=sample_qualitative_sections,
            ticker="AAPL",
            fiscal_year=2023
        )
        
        assert result is not None
        assert "business_analysis" in result or "sentiment" in result

    @pytest.mark.asyncio
    async def test_generate_comprehensive_analysis_success(self, investment_advisor, mock_openai_client):
        """Test comprehensive analysis generation."""
        # Mock qualitative score data
        qualitative_data = {
            "business_sentiment": 0.8,
            "risk_factors_sentiment": -0.2,
            "mda_sentiment": 0.6,
            "overall_sentiment": 0.4,
            "risk_score": 0.3,
            "growth_indicators": 0.9,
            "management_tone": 0.7
        }
        
        # Setup mock response
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = json.dumps({
            "recommendation": "BUY",
            "confidence": 0.85,
            "key_strengths": ["Strong growth", "Good management"],
            "key_concerns": ["Market competition"],
            "target_price_adjustment": 5.0
        })
        
        mock_openai_client.chat.completions.create = AsyncMock(return_value=mock_response)
        
        result = await investment_advisor.generate_comprehensive_analysis(
            ticker="AAPL",
            qualitative_data=qualitative_data,
            filing_id="test-filing-id"
        )
        
        assert result is not None
        assert "recommendation" in result
        assert result["recommendation"] == "BUY"

    @pytest.mark.asyncio
    async def test_chat_query_processing(self, investment_advisor, mock_openai_client):
        """Test chat query processing."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "Based on the analysis, AAPL shows strong growth potential."
        
        mock_openai_client.chat.completions.create = AsyncMock(return_value=mock_response)
        
        result = await investment_advisor.process_chat_query(
            user_query="What do you think about Apple stock?",
            context_data={"ticker": "AAPL"},
            conversation_history=[]
        )
        
        assert result is not None
        assert "AAPL" in result
        assert "growth" in result.lower()

    def test_risk_categorization(self, investment_advisor):
        """Test risk categorization logic."""
        risk_texts = [
            "Market competition may reduce market share",
            "New regulations could increase costs",
            "Cybersecurity threats pose data risks",
            "Supply chain disruptions affect production"
        ]
        
        categories = []
        for text in risk_texts:
            category = investment_advisor.categorize_risk_factor(text)
            categories.append(category)
        
        # Should categorize different types of risks
        unique_categories = set(categories)
        assert len(unique_categories) > 1  # Multiple categories

    def test_sentiment_scoring_logic(self, investment_advisor):
        """Test sentiment scoring logic."""
        # Test different sentiment scenarios
        positive_scores = {
            "business_sentiment": 0.8,
            "risk_factors_sentiment": -0.1,  # Less negative risks
            "mda_sentiment": 0.7,
            "overall_sentiment": 0.5
        }
        
        negative_scores = {
            "business_sentiment": -0.2,
            "risk_factors_sentiment": -0.8,  # Very negative risks
            "mda_sentiment": -0.1,
            "overall_sentiment": -0.3
        }
        
        positive_composite = investment_advisor.calculate_composite_sentiment(positive_scores)
        negative_composite = investment_advisor.calculate_composite_sentiment(negative_scores)
        
        assert positive_composite > negative_composite

    @pytest.mark.asyncio
    async def test_error_handling_ai_failure(self, investment_advisor, mock_openai_client):
        """Test handling of AI API failures."""
        # Setup mock to fail
        mock_openai_client.chat.completions.create = AsyncMock(side_effect=Exception("AI API Error"))
        
        result = await investment_advisor.analyze_filing_sections(
            sections={"business": "test content"},
            ticker="TEST",
            fiscal_year=2023
        )
        
        # Should handle gracefully
        assert result is None or "error" in str(result).lower()


@integration_test
@requires_api
class TestAIIntegration:
    """Integration tests for AI components."""

    @pytest.fixture
    def investment_advisor(self, mock_supabase_client):
        """Create InvestmentAdvisor for integration testing."""
        with patch("src.database.connection.create_client", return_value=mock_supabase_client):
            from src.llm.investment_advisor import InvestmentAdvisor
            return InvestmentAdvisor()

    @skip_if_no_live_api()
    @pytest.mark.asyncio
    async def test_real_openai_api_analysis(self, investment_advisor):
        """Test real OpenAI API integration."""
        # Simple test with real API
        sample_text = """
        Apple Inc. continues to demonstrate strong financial performance with
        record revenue growth across all product categories. The company's
        innovative product lineup and expanding services business provide
        multiple growth drivers for the future.
        """
        
        # This will make a real API call
        result = await investment_advisor.openai_client.analyze_qualitative_text(
            sample_text,
            section_type="business"
        )
        
        assert result is not None
        # Should contain analysis elements
        assert isinstance(result, (dict, str))

    @skip_if_no_live_api()
    @pytest.mark.asyncio
    async def test_real_investment_recommendation(self, investment_advisor):
        """Test real investment recommendation generation."""
        # Sample analysis data
        analysis_data = {
            "business_sentiment": 0.75,
            "risk_factors_sentiment": -0.25,
            "mda_sentiment": 0.60,
            "overall_sentiment": 0.50,
            "risk_score": 0.30,
            "growth_indicators": 0.85,
            "management_tone": 0.70
        }
        
        # Generate real recommendation
        result = await investment_advisor.generate_comprehensive_analysis(
            ticker="AAPL",
            qualitative_data=analysis_data,
            filing_id="test-integration"
        )
        
        assert result is not None
        if isinstance(result, dict):
            # Should have recommendation structure
            expected_keys = ["recommendation", "confidence", "key_strengths", "key_concerns"]
            assert any(key in result for key in expected_keys)

    def test_prompt_quality_and_structure(self, investment_advisor):
        """Test that prompts are well-structured and comprehensive."""
        from src.llm.openai_client import OpenAIFinancialAnalyst
        client = OpenAIFinancialAnalyst()
        
        # Test business analysis prompt
        business_prompt = client.construct_analysis_prompt(
            text="Sample business description",
            section_type="business",
            ticker="AAPL"
        )
        
        # Prompt should include key elements
        assert len(business_prompt) > 100  # Substantial prompt
        assert "business" in business_prompt.lower()
        assert "analysis" in business_prompt.lower()
        assert "AAPL" in business_prompt
        
        # Should have clear instructions
        assert any(word in business_prompt.lower() for word in ["analyze", "assess", "evaluate"])

    @pytest.mark.asyncio
    async def test_ai_consistency_across_runs(self, investment_advisor):
        """Test consistency of AI analysis across multiple runs."""
        if not pytest.get_environment_variable("RUN_LIVE_API_TESTS"):
            pytest.skip("Live API tests disabled")
        
        sample_text = "The company shows strong revenue growth and market expansion."
        
        results = []
        for _ in range(3):  # Run analysis 3 times
            result = await investment_advisor.openai_client.analyze_qualitative_text(
                sample_text,
                section_type="business"
            )
            results.append(result)
            
            # Add small delay between calls
            await asyncio.sleep(1)
        
        # Results should be generally consistent (all positive sentiment)
        # This is a basic consistency check
        assert len(results) == 3
        assert all(r is not None for r in results)

    @pytest.mark.asyncio
    async def test_token_usage_optimization(self, investment_advisor):
        """Test that token usage is optimized."""
        from src.llm.openai_client import OpenAIFinancialAnalyst
        client = OpenAIFinancialAnalyst()
        
        # Test with different text lengths
        short_text = "Brief business description."
        long_text = "Extended business description. " * 100
        
        short_tokens = client.count_tokens(short_text)
        long_tokens = client.count_tokens(long_text)
        
        # Token counting should work correctly
        assert short_tokens > 0
        assert long_tokens > short_tokens
        assert long_tokens < 4000  # Should be within reasonable limits

    def test_error_recovery_mechanisms(self, investment_advisor):
        """Test error recovery mechanisms."""
        # Test with malformed data
        malformed_data = {
            "invalid_key": "invalid_value",
            "business_sentiment": "not_a_number",  # Should be float
        }
        
        # Should handle gracefully without crashing
        try:
            result = investment_advisor.calculate_composite_sentiment(malformed_data)
            # Should return default or handle error
            assert result is not None or result == 0.0
        except Exception as e:
            # If it raises an exception, it should be handled appropriately
            assert "error" in str(e).lower() or "invalid" in str(e).lower()

    @pytest.mark.asyncio
    async def test_rate_limiting_compliance(self, investment_advisor):
        """Test rate limiting compliance with OpenAI API."""
        if not pytest.get_environment_variable("RUN_LIVE_API_TESTS"):
            pytest.skip("Live API tests disabled")
        
        start_time = datetime.now()
        
        # Make multiple API calls
        tasks = []
        for i in range(3):
            task = investment_advisor.openai_client.analyze_qualitative_text(
                f"Test analysis text {i}",
                section_type="business"
            )
            tasks.append(task)
        
        # Execute with proper rate limiting
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Should take some time due to rate limiting
        assert duration >= 1.0  # At least 1 second for 3 calls
        
        # Should get mostly successful results
        successful = [r for r in results if not isinstance(r, Exception)]
        assert len(successful) >= 2  # At least 2/3 should succeed