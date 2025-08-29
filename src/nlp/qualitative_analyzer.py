"""10-K 서류의 정성적 요소에 대한 고급 NLP 분석."""

import asyncio
import re
from typing import Dict, List, Tuple, Optional, Set
from dataclasses import dataclass
from collections import Counter, defaultdict
import numpy as np

# NLP libraries
from textblob import TextBlob
import nltk
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from nltk.chunk import ne_chunk
from nltk.tag import pos_tag
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity

# Transformers for advanced sentiment analysis
try:
    from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False
    
from loguru import logger

from src.database.schema import (
    SentimentAnalysis, KeyTheme, RiskFactor, 
    QualitativeScore
)
from src.database.connection import db_client


@dataclass
class ThemeAnalysis:
    """테마 분석 결과."""
    theme_name: str
    keywords: List[str]
    relevance_score: float
    context_snippets: List[str]
    frequency: int


@dataclass
class RiskAnalysis:
    """리스크 요인 분석 결과."""
    category: str
    description: str
    severity: float
    likelihood: Optional[float]
    keywords: List[str]
    mitigation_mentioned: bool


class QualitativeAnalyzer:
    """10-K 서류를 위한 고급 정성적 분석."""
    
    def __init__(self):
        self.stop_words = set(stopwords.words('english'))
        self.sentiment_pipeline = None
        
        # Initialize advanced models if available
        if TRANSFORMERS_AVAILABLE:
            try:
                self.sentiment_pipeline = pipeline(
                    "sentiment-analysis",
                    model="nlptown/bert-base-multilingual-uncased-sentiment",
                    return_all_scores=True
                )
            except Exception as e:
                logger.warning(f"Could not load advanced sentiment model: {e}")
        
        # Business context dictionaries
        self.risk_categories = {
            "market": [
                "market conditions", "economic downturn", "recession", "competition",
                "market share", "customer demand", "pricing pressure", "commoditization"
            ],
            "operational": [
                "supply chain", "manufacturing", "operations", "capacity", "efficiency",
                "quality", "disruption", "technology failure", "system failure"
            ],
            "regulatory": [
                "regulation", "regulatory", "compliance", "legal", "government",
                "policy", "tax", "environmental", "safety", "privacy"
            ],
            "financial": [
                "liquidity", "debt", "credit", "cash flow", "capital", "financing",
                "currency", "interest rate", "credit rating"
            ],
            "strategic": [
                "acquisition", "divestiture", "partnership", "strategy", "innovation",
                "research", "development", "intellectual property", "brand"
            ],
            "cyber": [
                "cybersecurity", "data breach", "hacking", "privacy", "security",
                "information technology", "data protection", "cyber attack"
            ]
        }
        
        self.opportunity_indicators = {
            "growth": [
                "growth", "expansion", "increase", "growing", "develop", "scale",
                "market penetration", "new markets", "emerging markets"
            ],
            "innovation": [
                "innovation", "technology", "digital transformation", "automation",
                "artificial intelligence", "machine learning", "research", "patent"
            ],
            "competitive_advantage": [
                "competitive advantage", "differentiation", "unique", "proprietary",
                "moat", "barrier to entry", "first mover", "market leader"
            ],
            "efficiency": [
                "efficiency", "optimization", "streamline", "cost reduction",
                "productivity", "automation", "lean", "process improvement"
            ]
        }
        
        self.financial_health_indicators = {
            "positive": [
                "strong cash flow", "improved margins", "revenue growth", "profitability",
                "strong balance sheet", "low debt", "high liquidity", "cash generation"
            ],
            "negative": [
                "cash flow concerns", "declining margins", "revenue decline", "losses",
                "high debt", "liquidity issues", "covenant breach", "impairment"
            ]
        }
        
        # Management quality indicators
        self.management_quality_indicators = {
            "positive": [
                "strategic vision", "clear strategy", "strong leadership", "experienced management",
                "execution", "track record", "transparency", "shareholder value"
            ],
            "negative": [
                "management changes", "uncertainty", "lack of clarity", "execution issues",
                "governance concerns", "insider trading", "compensation concerns"
            ]
        }
    
    async def analyze_sentiment(self, text: str, section_id: str, filing_id: str) -> SentimentAnalysis:
        """텍스트에 대한 종합적인 감정 분석을 수행합니다."""
        logger.info(f"Analyzing sentiment for section {section_id}")
        
        # Basic sentiment analysis with TextBlob
        blob = TextBlob(text)
        basic_sentiment = blob.sentiment
        
        # Advanced sentiment if available
        advanced_scores = None
        if self.sentiment_pipeline and len(text) < 10000:  # Limit length for transformer
            try:
                # Truncate text for transformer model
                truncated_text = text[:512] if len(text) > 512 else text
                results = self.sentiment_pipeline(truncated_text)
                
                if results and len(results[0]) > 0:
                    # Convert results to standardized format
                    scores = {result['label'].lower(): result['score'] for result in results[0]}
                    advanced_scores = scores
            except Exception as e:
                logger.warning(f"Advanced sentiment analysis failed: {e}")
        
        # Combine scores
        if advanced_scores:
            # Use advanced scores if available
            positive_score = advanced_scores.get('positive', 0.0)
            negative_score = advanced_scores.get('negative', 0.0)
            neutral_score = 1.0 - positive_score - negative_score
            overall_sentiment = positive_score - negative_score
            confidence = max(positive_score, negative_score, neutral_score)
        else:
            # Use TextBlob scores
            overall_sentiment = basic_sentiment.polarity
            confidence = abs(basic_sentiment.polarity)
            
            # Convert polarity to positive/negative/neutral scores
            if overall_sentiment > 0:
                positive_score = (overall_sentiment + 1) / 2
                negative_score = 0.0
                neutral_score = 1 - positive_score
            elif overall_sentiment < 0:
                negative_score = abs(overall_sentiment + 1) / 2
                positive_score = 0.0
                neutral_score = 1 - negative_score
            else:
                positive_score = 0.33
                negative_score = 0.33
                neutral_score = 0.34
        
        # Determine sentiment label
        if overall_sentiment > 0.1:
            sentiment_label = "positive"
        elif overall_sentiment < -0.1:
            sentiment_label = "negative"
        else:
            sentiment_label = "neutral"
        
        return SentimentAnalysis(
            section_id=section_id,
            filing_id=filing_id,
            overall_sentiment=round(overall_sentiment, 3),
            confidence=round(confidence, 3),
            positive_score=round(positive_score, 3),
            negative_score=round(negative_score, 3),
            neutral_score=round(neutral_score, 3),
            sentiment_label=sentiment_label,
            model_used="textblob+bert" if advanced_scores else "textblob"
        )
    
    def extract_key_themes(self, text: str, section_id: str, filing_id: str, max_themes: int = 10) -> List[KeyTheme]:
        """TF-IDF와 클러스터링을 사용하여 주요 테마를 추출합니다."""
        logger.info(f"Extracting key themes for section {section_id}")
        
        # Preprocess text
        sentences = sent_tokenize(text)
        if len(sentences) < 3:
            return []
        
        # Create TF-IDF vectors
        vectorizer = TfidfVectorizer(
            max_features=100,
            stop_words='english',
            ngram_range=(1, 3),
            min_df=1,
            max_df=0.8
        )
        
        try:
            tfidf_matrix = vectorizer.fit_transform(sentences)
            feature_names = vectorizer.get_feature_names_out()
            
            # Cluster sentences to identify themes
            n_clusters = min(max_themes, max(2, len(sentences) // 5))
            kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            clusters = kmeans.fit_predict(tfidf_matrix)
            
            themes = []
            for cluster_id in range(n_clusters):
                cluster_sentences = [sentences[i] for i in range(len(sentences)) if clusters[i] == cluster_id]
                
                if not cluster_sentences:
                    continue
                
                # Get top terms for this cluster
                cluster_indices = [i for i in range(len(sentences)) if clusters[i] == cluster_id]
                cluster_tfidf = tfidf_matrix[cluster_indices]
                mean_tfidf = cluster_tfidf.mean(axis=0).A1
                
                top_indices = mean_tfidf.argsort()[-10:][::-1]
                top_terms = [feature_names[i] for i in top_indices if mean_tfidf[i] > 0.1]
                
                if not top_terms:
                    continue
                
                # Generate theme name
                theme_name = self._generate_theme_name(top_terms, cluster_sentences)
                
                # Calculate relevance score
                relevance_score = float(np.mean([mean_tfidf[i] for i in top_indices[:5]]))
                
                # Select representative context snippets
                context_snippets = self._select_context_snippets(cluster_sentences, top_terms[:3])
                
                theme = KeyTheme(
                    section_id=section_id,
                    filing_id=filing_id,
                    theme_name=theme_name,
                    theme_description=f"Theme based on terms: {', '.join(top_terms[:5])}",
                    relevance_score=round(relevance_score, 3),
                    keyword_count=len(top_terms),
                    keywords=top_terms,
                    context_snippets=context_snippets
                )
                themes.append(theme)
            
            # Sort by relevance
            themes.sort(key=lambda x: x.relevance_score, reverse=True)
            return themes[:max_themes]
            
        except Exception as e:
            logger.error(f"Error extracting themes: {e}")
            return []
    
    def _generate_theme_name(self, top_terms: List[str], sentences: List[str]) -> str:
        """설명적인 테마 이름을 생성합니다."""
        # Check for business context matches
        for category, keywords in self.opportunity_indicators.items():
            for keyword in keywords:
                if any(keyword.lower() in term.lower() for term in top_terms):
                    return f"{category.replace('_', ' ').title()} Theme"
        
        for category, keywords in self.risk_categories.items():
            for keyword in keywords:
                if any(keyword.lower() in term.lower() for term in top_terms):
                    return f"{category.title()} Risk Theme"
        
        # Default to most prominent term
        return f"{top_terms[0].title()} Theme" if top_terms else "General Theme"
    
    def _select_context_snippets(self, sentences: List[str], key_terms: List[str]) -> List[str]:
        """가장 관련성이 높은 컨텍스트 스니펫을 선택합니다."""
        scored_sentences = []
        
        for sentence in sentences:
            score = sum(1 for term in key_terms if term.lower() in sentence.lower())
            if score > 0 and len(sentence.split()) > 5:
                scored_sentences.append((score, sentence))
        
        # Sort by relevance and select top 3
        scored_sentences.sort(key=lambda x: x[0], reverse=True)
        return [sentence for _, sentence in scored_sentences[:3]]
    
    def analyze_risk_factors(self, text: str, filing_id: str) -> List[RiskFactor]:
        """리스크 요인을 분석하고 분류합니다."""
        logger.info("Analyzing risk factors")
        
        risks = []
        sentences = sent_tokenize(text)
        
        for sentence in sentences:
            if len(sentence.split()) < 5:  # Skip very short sentences
                continue
                
            # Check for risk indicators
            sentence_lower = sentence.lower()
            
            # Skip if sentence doesn't contain risk language
            risk_words = ["risk", "may", "could", "might", "uncertainty", "challenge", 
                         "adverse", "negative", "decline", "loss", "threat", "concern"]
            if not any(word in sentence_lower for word in risk_words):
                continue
            
            # Categorize risk
            risk_category = self._categorize_risk(sentence)
            if not risk_category:
                continue
            
            # Calculate severity
            severity = self._calculate_risk_severity(sentence)
            
            # Extract keywords
            keywords = self._extract_risk_keywords(sentence, risk_category)
            
            # Check for mitigation mentions
            mitigation_mentioned = self._check_mitigation_mentioned(sentence)
            
            risk = RiskFactor(
                filing_id=filing_id,
                risk_category=risk_category,
                risk_description=sentence[:500],  # Limit description length
                risk_severity=round(severity, 3),
                impact_keywords=keywords,
                mitigation_mentioned=mitigation_mentioned
            )
            risks.append(risk)
        
        # Remove duplicates and limit to most significant risks
        unique_risks = self._deduplicate_risks(risks)
        return sorted(unique_risks, key=lambda x: x.risk_severity, reverse=True)[:20]
    
    def _categorize_risk(self, sentence: str) -> Optional[str]:
        """리스크 문장을 분류합니다."""
        sentence_lower = sentence.lower()
        
        category_scores = {}
        for category, keywords in self.risk_categories.items():
            score = sum(1 for keyword in keywords if keyword in sentence_lower)
            if score > 0:
                category_scores[category] = score
        
        if category_scores:
            return max(category_scores, key=category_scores.get)
        return None
    
    def _calculate_risk_severity(self, sentence: str) -> float:
        """언어의 강도를 기반으로 리스크 심각도를 계산합니다."""
        sentence_lower = sentence.lower()
        
        high_severity_words = ["significant", "material", "severe", "substantial", "major", 
                              "critical", "serious", "dramatically", "significantly"]
        medium_severity_words = ["may", "could", "potential", "possible", "risk", "concern"]
        low_severity_words = ["minor", "limited", "manageable", "controlled"]
        
        high_count = sum(1 for word in high_severity_words if word in sentence_lower)
        medium_count = sum(1 for word in medium_severity_words if word in sentence_lower)
        low_count = sum(1 for word in low_severity_words if word in sentence_lower)
        
        severity = 0.5  # Base severity
        severity += high_count * 0.3
        severity += medium_count * 0.1
        severity -= low_count * 0.2
        
        return max(0.1, min(1.0, severity))
    
    def _extract_risk_keywords(self, sentence: str, category: str) -> List[str]:
        """리스크 관련 핵심 용어를 추출합니다."""
        words = word_tokenize(sentence.lower())
        words = [w for w in words if w.isalpha() and w not in self.stop_words and len(w) > 3]
        
        # Include category-specific terms
        category_terms = self.risk_categories.get(category, [])
        keywords = [word for word in words if any(term in word for term in category_terms)]
        
        # Add high-impact words
        impact_words = ["decline", "loss", "failure", "breach", "disruption", "volatility"]
        keywords.extend([word for word in words if word in impact_words])
        
        return list(set(keywords))[:10]  # Limit and deduplicate
    
    def _check_mitigation_mentioned(self, sentence: str) -> bool:
        """리스크 완화 방안이 언급되었는지 확인합니다."""
        mitigation_words = ["mitigate", "manage", "control", "reduce", "prevent", 
                           "address", "hedge", "insurance", "diversify", "monitor"]
        sentence_lower = sentence.lower()
        return any(word in sentence_lower for word in mitigation_words)
    
    def _deduplicate_risks(self, risks: List[RiskFactor]) -> List[RiskFactor]:
        """중복되거나 매우 유사한 리스크를 제거합니다."""
        if len(risks) <= 1:
            return risks
        
        unique_risks = []
        seen_descriptions = set()
        
        for risk in risks:
            # Simple deduplication based on first 100 characters
            description_key = risk.risk_description[:100].lower()
            if description_key not in seen_descriptions:
                seen_descriptions.add(description_key)
                unique_risks.append(risk)
        
        return unique_risks
    
    async def calculate_qualitative_scores(self, filing_id: str, ticker: str, 
                                         fiscal_year: int, sections_data: Dict) -> QualitativeScore:
        """종합적인 정성적 점수를 계산합니다."""
        logger.info(f"Calculating qualitative scores for {ticker} {fiscal_year}")
        
        # Initialize scores
        scores = {
            "business_sentiment": 0.0,
            "risk_factors_sentiment": 0.0,
            "mda_sentiment": 0.0,
            "overall_sentiment": 0.0,
            "risk_score": 0.5,
            "risk_diversity": 0.0,
            "risk_severity": 0.0,
            "growth_indicators": 0.0,
            "innovation_mentions": 0.0,
            "competitive_advantage": 0.0,
            "market_expansion": 0.0,
            "management_tone": 0.0,
            "strategic_clarity": 0.0,
            "transparency_score": 0.0,
            "financial_stress_indicators": 0.0,
            "cash_flow_mentions": 0.0,
            "debt_concerns": 0.0
        }
        
        # Calculate sentiment scores for each section
        section_sentiments = {}
        for section_name, section_content in sections_data.items():
            if section_content:
                blob = TextBlob(section_content)
                section_sentiments[section_name] = blob.sentiment.polarity
        
        scores["business_sentiment"] = section_sentiments.get("business", 0.0)
        scores["risk_factors_sentiment"] = section_sentiments.get("risk_factors", 0.0)
        scores["mda_sentiment"] = section_sentiments.get("mda", 0.0)
        scores["overall_sentiment"] = np.mean(list(section_sentiments.values())) if section_sentiments else 0.0
        
        # Analyze opportunity indicators
        all_text = " ".join(sections_data.values()).lower()
        
        for category, indicators in self.opportunity_indicators.items():
            count = sum(1 for indicator in indicators if indicator in all_text)
            normalized_score = min(1.0, count / len(indicators))
            
            if category == "growth":
                scores["growth_indicators"] = normalized_score
            elif category == "innovation":
                scores["innovation_mentions"] = normalized_score
            elif category == "competitive_advantage":
                scores["competitive_advantage"] = normalized_score
        
        # Financial health indicators
        positive_financial = sum(1 for indicator in self.financial_health_indicators["positive"] 
                               if indicator in all_text)
        negative_financial = sum(1 for indicator in self.financial_health_indicators["negative"] 
                               if indicator in all_text)
        
        scores["financial_stress_indicators"] = min(1.0, negative_financial / 8)
        scores["cash_flow_mentions"] = 1.0 if "cash flow" in all_text else 0.0
        scores["debt_concerns"] = min(1.0, all_text.count("debt") / 10)
        
        # Management quality
        positive_mgmt = sum(1 for indicator in self.management_quality_indicators["positive"] 
                          if indicator in all_text)
        negative_mgmt = sum(1 for indicator in self.management_quality_indicators["negative"] 
                          if indicator in all_text)
        
        scores["management_tone"] = max(0.0, min(1.0, (positive_mgmt - negative_mgmt) / 8 + 0.5))
        scores["strategic_clarity"] = 1.0 if "strategy" in all_text else 0.5
        scores["transparency_score"] = min(1.0, all_text.count("transparent") / 5 + 0.5)
        
        # Risk analysis
        risk_count = all_text.count("risk")
        scores["risk_score"] = min(1.0, risk_count / 50)  # Normalize risk mentions
        
        # Calculate composite scores
        qualitative_score = (
            scores["overall_sentiment"] * 20 +  # Sentiment component
            (1 - scores["risk_score"]) * 30 +   # Risk component (inverted)
            scores["growth_indicators"] * 15 +   # Growth component
            scores["innovation_mentions"] * 10 + # Innovation component
            scores["competitive_advantage"] * 15 + # Competitive component
            scores["management_tone"] * 10       # Management component
        )
        
        investment_attractiveness = max(0.0, min(1.0, qualitative_score / 100))
        
        return QualitativeScore(
            filing_id=filing_id,
            ticker=ticker,
            fiscal_year=fiscal_year,
            business_sentiment=round(scores["business_sentiment"], 3),
            risk_factors_sentiment=round(scores["risk_factors_sentiment"], 3),
            mda_sentiment=round(scores["mda_sentiment"], 3),
            overall_sentiment=round(scores["overall_sentiment"], 3),
            risk_score=round(scores["risk_score"], 3),
            risk_diversity=round(scores["risk_diversity"], 3),
            risk_severity=round(scores["risk_severity"], 3),
            growth_indicators=round(scores["growth_indicators"], 3),
            innovation_mentions=round(scores["innovation_mentions"], 3),
            competitive_advantage=round(scores["competitive_advantage"], 3),
            market_expansion=round(scores["market_expansion"], 3),
            management_tone=round(scores["management_tone"], 3),
            strategic_clarity=round(scores["strategic_clarity"], 3),
            transparency_score=round(scores["transparency_score"], 3),
            financial_stress_indicators=round(scores["financial_stress_indicators"], 3),
            cash_flow_mentions=round(scores["cash_flow_mentions"], 3),
            debt_concerns=round(scores["debt_concerns"], 3),
            qualitative_score=round(qualitative_score, 2),
            investment_attractiveness=round(investment_attractiveness, 3)
        )


async def main():
    """정성적 분석 기능을 테스트합니다."""
    analyzer = QualitativeAnalyzer()
    
    # Test text
    sample_text = """
    The company has shown strong growth in recent years, with revenue increasing by 25% 
    year-over-year. Our innovative technology platform provides significant competitive 
    advantages in the market. However, we face risks from increasing competition and 
    potential regulatory changes that could adversely affect our business.
    """
    
    # Test sentiment analysis
    sentiment = await analyzer.analyze_sentiment(sample_text, "test_section", "test_filing")
    print(f"Sentiment: {sentiment.sentiment_label} ({sentiment.overall_sentiment})")
    
    # Test theme extraction
    themes = analyzer.extract_key_themes(sample_text, "test_section", "test_filing")
    print(f"Themes: {[theme.theme_name for theme in themes]}")


if __name__ == "__main__":
    asyncio.run(main())