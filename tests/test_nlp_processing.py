"""Comprehensive tests for NLP and text processing components."""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock
import numpy as np
from datetime import datetime

from tests.conftest import (
    unit_test, integration_test, requires_api,
    skip_if_no_integration
)


@unit_test
class TestTextProcessorUnit:
    """Unit tests for TextProcessor."""

    @pytest.fixture
    def text_processor(self):
        """Create TextProcessor instance."""
        from src.nlp.text_processor import TextProcessor
        return TextProcessor()

    def test_clean_text_basic(self, text_processor):
        """Test basic text cleaning functionality."""
        dirty_text = "  This is a test.  \n\n Multiple spaces and   line breaks. \t\t "
        
        cleaned = text_processor.clean_text(dirty_text)
        
        assert "This is a test." in cleaned
        assert "Multiple spaces and line breaks." in cleaned
        assert "  " not in cleaned  # Multiple spaces should be cleaned
        assert "\n\n" not in cleaned  # Multiple line breaks should be cleaned

    def test_clean_text_html_tags(self, text_processor):
        """Test HTML tag removal."""
        html_text = "<p>This is <strong>important</strong> content.</p><div>More content</div>"
        
        cleaned = text_processor.clean_text(html_text)
        
        assert "<p>" not in cleaned
        assert "<strong>" not in cleaned
        assert "<div>" not in cleaned
        assert "This is important content." in cleaned
        assert "More content" in cleaned

    def test_clean_text_special_characters(self, text_processor):
        """Test special character handling."""
        special_text = "Text with—em dash, bullet • points, and fancy 'quotes'"
        
        cleaned = text_processor.clean_text(special_text)
        
        # Should handle special characters appropriately
        assert cleaned is not None
        assert len(cleaned) > 0

    def test_extract_sentences_basic(self, text_processor):
        """Test sentence extraction."""
        text = "First sentence. Second sentence! Third sentence? Fourth sentence."
        
        sentences = text_processor.extract_sentences(text)
        
        assert len(sentences) == 4
        assert "First sentence." in sentences
        assert "Second sentence!" in sentences
        assert "Third sentence?" in sentences
        assert "Fourth sentence." in sentences

    def test_extract_sentences_complex(self, text_processor):
        """Test sentence extraction with complex punctuation."""
        text = "U.S.A. is a country. Mr. Smith works there. The company's Q1 results were strong."
        
        sentences = text_processor.extract_sentences(text)
        
        # Should handle abbreviations correctly
        assert len(sentences) == 3
        assert any("U.S.A." in s for s in sentences)
        assert any("Mr. Smith" in s for s in sentences)

    def test_extract_keywords_basic(self, text_processor):
        """Test keyword extraction."""
        text = """
        Apple Inc. is a technology company that designs and manufactures consumer electronics.
        The company's main products include iPhone, iPad, Mac computers, and Apple Watch.
        Apple has shown strong revenue growth in recent quarters.
        """
        
        keywords = text_processor.extract_keywords(text, max_keywords=10)
        
        assert len(keywords) <= 10
        assert isinstance(keywords, list)
        # Should extract relevant keywords
        keyword_text = " ".join(keywords).lower()
        assert any(word in keyword_text for word in ["apple", "company", "technology", "products"])

    def test_extract_keywords_empty_text(self, text_processor):
        """Test keyword extraction with empty text."""
        keywords = text_processor.extract_keywords("", max_keywords=5)
        
        assert keywords == []

    def test_calculate_text_statistics(self, text_processor):
        """Test text statistics calculation."""
        text = "This is a sample text. It has two sentences and multiple words."
        
        stats = text_processor.calculate_text_statistics(text)
        
        assert "word_count" in stats
        assert "sentence_count" in stats
        assert "char_count" in stats
        assert "avg_sentence_length" in stats
        
        assert stats["sentence_count"] == 2
        assert stats["word_count"] > 10
        assert stats["char_count"] > 50

    def test_detect_financial_terms(self, text_processor):
        """Test financial term detection."""
        text = """
        The company reported strong revenue growth of 15% year-over-year.
        EBITDA margins improved to 25%. Free cash flow was $2.5 billion.
        Return on equity increased to 18%.
        """
        
        financial_terms = text_processor.detect_financial_terms(text)
        
        assert len(financial_terms) > 0
        # Should detect financial metrics
        term_text = " ".join(financial_terms).lower()
        assert any(term in term_text for term in ["revenue", "ebitda", "cash flow", "equity"])

    def test_extract_risk_phrases(self, text_processor):
        """Test risk phrase extraction."""
        text = """
        The company may face significant challenges in the future.
        Market volatility could impact our results. There is substantial risk
        of regulatory changes. We are uncertain about economic conditions.
        """
        
        risk_phrases = text_processor.extract_risk_phrases(text)
        
        assert len(risk_phrases) > 0
        # Should identify risk-related language
        risk_text = " ".join(risk_phrases).lower()
        assert any(phrase in risk_text for phrase in ["challenges", "volatility", "risk", "uncertain"])


@unit_test 
class TestQualitativeAnalyzerUnit:
    """Unit tests for QualitativeAnalyzer."""

    @pytest.fixture
    def qualitative_analyzer(self, mock_supabase_client):
        """Create QualitativeAnalyzer instance."""
        with patch("src.database.connection.create_client", return_value=mock_supabase_client):
            from src.nlp.qualitative_analyzer import QualitativeAnalyzer
            return QualitativeAnalyzer()

    def test_analyze_sentiment_positive(self, qualitative_analyzer):
        """Test sentiment analysis for positive text."""
        positive_text = "The company shows excellent growth prospects with strong revenue increases."
        
        result = qualitative_analyzer.analyze_sentiment(positive_text)
        
        assert "overall_sentiment" in result
        assert "confidence" in result
        assert "positive_score" in result
        assert "negative_score" in result
        assert "neutral_score" in result
        assert "sentiment_label" in result
        
        # Should detect positive sentiment
        assert result["overall_sentiment"] > 0
        assert result["sentiment_label"] in ["positive", "neutral"]

    def test_analyze_sentiment_negative(self, qualitative_analyzer):
        """Test sentiment analysis for negative text."""
        negative_text = "The company faces severe challenges with declining sales and major losses."
        
        result = qualitative_analyzer.analyze_sentiment(negative_text)
        
        assert result["overall_sentiment"] < 0.5  # Should be less positive
        assert result["negative_score"] > 0.1     # Should have some negative component

    def test_analyze_sentiment_empty_text(self, qualitative_analyzer):
        """Test sentiment analysis for empty text."""
        result = qualitative_analyzer.analyze_sentiment("")
        
        assert result["overall_sentiment"] == 0.0
        assert result["sentiment_label"] == "neutral"

    def test_extract_key_themes_business(self, qualitative_analyzer):
        """Test theme extraction from business section."""
        business_text = """
        Apple Inc. is a multinational technology company that specializes in consumer electronics,
        computer software, and online services. The company's hardware products include the iPhone,
        iPad, Mac, Apple Watch, and Apple TV. Apple's software includes macOS, iOS, watchOS, and tvOS.
        The company also provides cloud services through iCloud and operates the App Store.
        """
        
        themes = qualitative_analyzer.extract_key_themes(business_text, section_type="business")
        
        assert len(themes) > 0
        assert all(isinstance(theme, dict) for theme in themes)
        
        # Should extract technology-related themes
        theme_names = [theme.get("theme_name", "").lower() for theme in themes]
        theme_text = " ".join(theme_names)
        assert any(word in theme_text for word in ["technology", "product", "software", "service"])

    def test_extract_key_themes_risk_factors(self, qualitative_analyzer):
        """Test theme extraction from risk factors section."""
        risk_text = """
        The company faces risks related to market competition, economic downturns,
        supply chain disruptions, regulatory changes, and cybersecurity threats.
        Foreign exchange fluctuations may also impact financial results.
        """
        
        themes = qualitative_analyzer.extract_key_themes(risk_text, section_type="risk_factors")
        
        assert len(themes) > 0
        
        # Should extract risk-related themes
        theme_names = [theme.get("theme_name", "").lower() for theme in themes]
        theme_text = " ".join(theme_names)
        assert any(word in theme_text for word in ["risk", "competition", "regulatory", "cyber"])

    def test_categorize_risks(self, qualitative_analyzer):
        """Test risk categorization."""
        risk_text = """
        Market competition may reduce our market share. New regulations could increase compliance costs.
        Cybersecurity breaches might compromise customer data. Supply chain disruptions could affect production.
        Economic recession may reduce customer demand.
        """
        
        risks = qualitative_analyzer.categorize_risks(risk_text)
        
        assert len(risks) > 0
        assert all(isinstance(risk, dict) for risk in risks)
        
        # Should have different risk categories
        categories = [risk.get("risk_category", "") for risk in risks]
        assert len(set(categories)) > 1  # Multiple categories

    def test_assess_management_tone(self, qualitative_analyzer):
        """Test management tone assessment."""
        confident_text = """
        We are confident in our strategic direction and believe our investments will drive
        significant long-term value creation. Our team is executing well on key initiatives.
        """
        
        tone_analysis = qualitative_analyzer.assess_management_tone(confident_text)
        
        assert "confidence_score" in tone_analysis
        assert "optimism_score" in tone_analysis
        assert "transparency_score" in tone_analysis
        assert "overall_tone" in tone_analysis
        
        # Should detect confident tone
        assert tone_analysis["confidence_score"] > 0.5

    def test_identify_growth_indicators(self, qualitative_analyzer):
        """Test growth indicator identification."""
        growth_text = """
        The company is expanding into new markets with innovative products.
        R&D investments are increasing to drive future innovation.
        We see significant opportunities in emerging technologies.
        """
        
        growth_indicators = qualitative_analyzer.identify_growth_indicators(growth_text)
        
        assert len(growth_indicators) > 0
        assert all(isinstance(indicator, dict) for indicator in growth_indicators)
        
        # Should identify growth-related terms
        indicator_text = " ".join([ind.get("description", "").lower() for ind in growth_indicators])
        assert any(word in indicator_text for word in ["expand", "innovation", "opportunities"])


@integration_test
class TestNLPIntegration:
    """Integration tests for NLP components with real processing."""

    @pytest.fixture
    def qualitative_analyzer(self, mock_supabase_client):
        """Create QualitativeAnalyzer for integration testing.""" 
        with patch("src.database.connection.create_client", return_value=mock_supabase_client):
            from src.nlp.qualitative_analyzer import QualitativeAnalyzer
            return QualitativeAnalyzer()

    def test_full_analysis_workflow(self, qualitative_analyzer, sample_qualitative_sections):
        """Test complete analysis workflow with real sections."""
        filing_id = "test-filing-123"
        ticker = "AAPL"
        
        # Process each section
        results = {}
        for section_name, content in sample_qualitative_sections.items():
            section_result = qualitative_analyzer.analyze_section(
                filing_id=filing_id,
                section_name=section_name,
                content=content,
                ticker=ticker
            )
            results[section_name] = section_result
        
        # Should have results for all sections
        assert len(results) == len(sample_qualitative_sections)
        
        # Each result should have required components
        for section_name, result in results.items():
            assert "sentiment_analysis" in result
            assert "key_themes" in result
            assert "word_count" in result
            assert result["word_count"] > 0

    def test_sentiment_consistency(self, qualitative_analyzer):
        """Test sentiment analysis consistency."""
        test_texts = [
            "Excellent performance with outstanding results and strong growth.",
            "Poor performance with declining revenues and significant losses.",
            "Stable performance with moderate growth and steady results."
        ]
        
        sentiments = []
        for text in test_texts:
            result = qualitative_analyzer.analyze_sentiment(text)
            sentiments.append(result["overall_sentiment"])
        
        # Sentiments should be ordered: positive > neutral > negative
        assert sentiments[0] > sentiments[2]  # Excellent > Stable
        assert sentiments[2] > sentiments[1]  # Stable > Poor

    def test_theme_extraction_relevance(self, qualitative_analyzer):
        """Test that extracted themes are relevant to content."""
        technology_text = """
        Apple continues to innovate in the smartphone and computer markets.
        The company invests heavily in artificial intelligence and machine learning.
        New product development focuses on user experience and design excellence.
        """
        
        themes = qualitative_analyzer.extract_key_themes(technology_text)
        
        # Themes should be relevant to technology/Apple
        relevant_count = 0
        for theme in themes:
            theme_name = theme.get("theme_name", "").lower()
            keywords = theme.get("keywords", [])
            keyword_text = " ".join(keywords).lower()
            
            if any(word in theme_name or word in keyword_text 
                   for word in ["technology", "innovation", "product", "apple", "design"]):
                relevant_count += 1
        
        # At least half of themes should be relevant
        assert relevant_count >= len(themes) // 2

    def test_performance_large_text(self, qualitative_analyzer):
        """Test performance with large text input."""
        # Create large text (simulate real 10-K section)
        large_text = """
        Apple Inc. is a multinational technology company headquartered in Cupertino, California.
        """ * 1000  # Repeat to create large text
        
        start_time = datetime.now()
        
        # Run analysis
        sentiment_result = qualitative_analyzer.analyze_sentiment(large_text)
        themes_result = qualitative_analyzer.extract_key_themes(large_text)
        
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        # Should complete in reasonable time (less than 30 seconds)
        assert processing_time < 30.0
        
        # Should still produce valid results
        assert sentiment_result is not None
        assert themes_result is not None
        assert len(themes_result) > 0

    @requires_api
    @skip_if_no_integration()
    def test_nltk_data_availability(self, qualitative_analyzer):
        """Test that required NLTK data is available."""
        import nltk
        
        # Test required NLTK data
        required_data = [
            'punkt',
            'stopwords', 
            'wordnet',
            'averaged_perceptron_tagger'
        ]
        
        for data_name in required_data:
            try:
                nltk.data.find(f'tokenizers/{data_name}')
            except LookupError:
                try:
                    nltk.data.find(f'corpora/{data_name}')
                except LookupError:
                    try:
                        nltk.data.find(f'taggers/{data_name}')
                    except LookupError:
                        pytest.fail(f"Required NLTK data '{data_name}' not found")

    def test_multilingual_text_handling(self, qualitative_analyzer):
        """Test handling of text with non-English characters."""
        multilingual_text = """
        The company operates globally with présence in múltiple markets.
        Revenue includes transactions in yen (¥), euros (€), and pounds (£).
        """
        
        # Should handle without errors
        sentiment_result = qualitative_analyzer.analyze_sentiment(multilingual_text)
        themes_result = qualitative_analyzer.extract_key_themes(multilingual_text)
        
        assert sentiment_result is not None
        assert themes_result is not None
        assert isinstance(sentiment_result["overall_sentiment"], (int, float))

    def test_edge_case_empty_sections(self, qualitative_analyzer):
        """Test handling of edge cases with empty or minimal content."""
        edge_cases = [
            "",  # Empty
            "N/A",  # Minimal
            "   \n\n   ",  # Whitespace only
            "See attached.",  # Very short
        ]
        
        for text in edge_cases:
            # Should not raise exceptions
            sentiment_result = qualitative_analyzer.analyze_sentiment(text)
            themes_result = qualitative_analyzer.extract_key_themes(text)
            
            assert sentiment_result is not None
            assert themes_result is not None
            assert isinstance(sentiment_result, dict)
            assert isinstance(themes_result, list)