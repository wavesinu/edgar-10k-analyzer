"""정성적 분석을 기반으로 한 투자 점수 및 순위 시스템."""

import asyncio
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from datetime import datetime, date
from enum import Enum
import pandas as pd

from loguru import logger

from src.database.schema import (
    InvestmentAnalysis, InvestmentRecommendation, 
    QualitativeScore
)
from src.database.connection import db_client


class ScoreWeights:
    """다양한 점수 구성요소에 대한 기본 가중치."""
    SENTIMENT_WEIGHT = 0.25
    RISK_WEIGHT = 0.35
    GROWTH_WEIGHT = 0.20
    MANAGEMENT_WEIGHT = 0.10
    FINANCIAL_HEALTH_WEIGHT = 0.10


@dataclass 
class ScoringComponents:
    """투자 분석을 위한 개별 점수 구성요소."""
    sentiment_score: float
    risk_score: float
    growth_score: float
    management_score: float
    financial_health_score: float
    composite_score: float
    
    
@dataclass
class PeerComparison:
    """동종업계 비교 지표."""
    peer_ticker: str
    peer_score: float
    relative_performance: float  # -1 to 1, where 1 is much better
    

@dataclass
class InvestmentInsight:
    """뒷받침 증거가 있는 투자 인사이트."""
    category: str  # strength, concern, risk, opportunity
    description: str
    importance: float  # 0-1 scale
    evidence: List[str]


class InvestmentScorer:
    """정성적 분석을 위한 고급 투자 점수 시스템."""
    
    def __init__(self):
        self.score_weights = ScoreWeights()
        
        # Industry benchmarks (these would ideally come from external data)
        self.industry_benchmarks = {
            "technology": {
                "innovation_threshold": 0.7,
                "growth_threshold": 0.6,
                "risk_tolerance": 0.6
            },
            "healthcare": {
                "innovation_threshold": 0.8,
                "growth_threshold": 0.5,
                "risk_tolerance": 0.4
            },
            "finance": {
                "innovation_threshold": 0.4,
                "growth_threshold": 0.4,
                "risk_tolerance": 0.3
            },
            "default": {
                "innovation_threshold": 0.5,
                "growth_threshold": 0.5,
                "risk_tolerance": 0.5
            }
        }
        
        # Recommendation thresholds
        self.recommendation_thresholds = {
            "strong_buy": 0.80,
            "buy": 0.65,
            "hold": 0.45,
            "sell": 0.30,
            "strong_sell": 0.0
        }
    
    def calculate_scoring_components(self, qualitative_score: QualitativeScore) -> ScoringComponents:
        """개별 점수 구성요소를 계산합니다."""
        
        # Sentiment Component (0-1 scale)
        # Convert sentiment from -1,1 to 0,1 scale and weight by confidence
        sentiment_normalized = (qualitative_score.overall_sentiment + 1) / 2
        sentiment_score = sentiment_normalized
        
        # Risk Component (0-1 scale, inverted so lower risk = higher score)
        risk_score = 1.0 - qualitative_score.risk_score
        
        # Growth Component (0-1 scale)
        growth_score = (
            qualitative_score.growth_indicators * 0.4 +
            qualitative_score.innovation_mentions * 0.3 +
            qualitative_score.competitive_advantage * 0.2 +
            qualitative_score.market_expansion * 0.1
        )
        
        # Management Component (0-1 scale)
        management_score = (
            qualitative_score.management_tone * 0.4 +
            qualitative_score.strategic_clarity * 0.3 +
            qualitative_score.transparency_score * 0.3
        )
        
        # Financial Health Component (0-1 scale, inverted stress indicators)
        financial_health_score = (
            (1.0 - qualitative_score.financial_stress_indicators) * 0.4 +
            qualitative_score.cash_flow_mentions * 0.3 +
            (1.0 - qualitative_score.debt_concerns) * 0.3
        )
        
        # Composite Score
        composite_score = (
            sentiment_score * self.score_weights.SENTIMENT_WEIGHT +
            risk_score * self.score_weights.RISK_WEIGHT +
            growth_score * self.score_weights.GROWTH_WEIGHT +
            management_score * self.score_weights.MANAGEMENT_WEIGHT +
            financial_health_score * self.score_weights.FINANCIAL_HEALTH_WEIGHT
        )
        
        return ScoringComponents(
            sentiment_score=round(sentiment_score, 3),
            risk_score=round(risk_score, 3),
            growth_score=round(growth_score, 3),
            management_score=round(management_score, 3),
            financial_health_score=round(financial_health_score, 3),
            composite_score=round(composite_score, 3)
        )
    
    def determine_recommendation(self, composite_score: float, 
                               components: ScoringComponents) -> Tuple[InvestmentRecommendation, float]:
        """복합 점수를 기반으로 투자 추천을 결정합니다."""
        
        # Base recommendation from composite score
        if composite_score >= self.recommendation_thresholds["strong_buy"]:
            base_recommendation = InvestmentRecommendation.STRONG_BUY
        elif composite_score >= self.recommendation_thresholds["buy"]:
            base_recommendation = InvestmentRecommendation.BUY
        elif composite_score >= self.recommendation_thresholds["hold"]:
            base_recommendation = InvestmentRecommendation.HOLD
        elif composite_score >= self.recommendation_thresholds["sell"]:
            base_recommendation = InvestmentRecommendation.SELL
        else:
            base_recommendation = InvestmentRecommendation.STRONG_SELL
        
        # Calculate confidence based on component consistency
        component_values = [
            components.sentiment_score,
            components.risk_score,
            components.growth_score,
            components.management_score,
            components.financial_health_score
        ]
        
        # Confidence is higher when components are consistent
        component_std = np.std(component_values)
        confidence = max(0.5, 1.0 - (component_std * 2))  # Higher std = lower confidence
        
        # Adjust for extreme scores
        if composite_score > 0.9 or composite_score < 0.1:
            confidence = min(1.0, confidence + 0.1)
        
        return base_recommendation, round(confidence, 3)
    
    def generate_investment_insights(self, qualitative_score: QualitativeScore, 
                                   components: ScoringComponents) -> List[InvestmentInsight]:
        """실행 가능한 투자 인사이트를 생성합니다."""
        insights = []
        
        # Sentiment insights
        if components.sentiment_score > 0.7:
            insights.append(InvestmentInsight(
                category="strength",
                description="Positive management tone and business outlook",
                importance=0.8,
                evidence=[f"Overall sentiment score: {components.sentiment_score}"]
            ))
        elif components.sentiment_score < 0.3:
            insights.append(InvestmentInsight(
                category="concern",
                description="Negative or cautious management tone",
                importance=0.7,
                evidence=[f"Low sentiment score: {components.sentiment_score}"]
            ))
        
        # Risk insights
        if components.risk_score < 0.3:
            insights.append(InvestmentInsight(
                category="risk",
                description="High risk profile with significant uncertainty",
                importance=0.9,
                evidence=[f"High risk indicators: {qualitative_score.risk_score}"]
            ))
        elif components.risk_score > 0.7:
            insights.append(InvestmentInsight(
                category="strength",
                description="Low risk profile with manageable uncertainties",
                importance=0.6,
                evidence=[f"Low risk score: {qualitative_score.risk_score}"]
            ))
        
        # Growth insights
        if components.growth_score > 0.6:
            insights.append(InvestmentInsight(
                category="opportunity",
                description="Strong growth potential and market opportunities",
                importance=0.8,
                evidence=[
                    f"Growth indicators: {qualitative_score.growth_indicators}",
                    f"Innovation mentions: {qualitative_score.innovation_mentions}"
                ]
            ))
        elif components.growth_score < 0.3:
            insights.append(InvestmentInsight(
                category="concern",
                description="Limited growth opportunities mentioned",
                importance=0.6,
                evidence=[f"Low growth score: {components.growth_score}"]
            ))
        
        # Management insights
        if components.management_score > 0.7:
            insights.append(InvestmentInsight(
                category="strength",
                description="Strong management team with clear strategy",
                importance=0.7,
                evidence=[
                    f"Management tone: {qualitative_score.management_tone}",
                    f"Strategic clarity: {qualitative_score.strategic_clarity}"
                ]
            ))
        elif components.management_score < 0.4:
            insights.append(InvestmentInsight(
                category="concern", 
                description="Management concerns or lack of strategic clarity",
                importance=0.6,
                evidence=[f"Low management score: {components.management_score}"]
            ))
        
        # Financial health insights
        if qualitative_score.financial_stress_indicators > 0.6:
            insights.append(InvestmentInsight(
                category="risk",
                description="Financial stress indicators present",
                importance=0.8,
                evidence=[f"Financial stress score: {qualitative_score.financial_stress_indicators}"]
            ))
        
        if qualitative_score.debt_concerns > 0.6:
            insights.append(InvestmentInsight(
                category="concern",
                description="Significant debt concerns mentioned",
                importance=0.7,
                evidence=[f"Debt concerns score: {qualitative_score.debt_concerns}"]
            ))
        
        # Sort by importance
        insights.sort(key=lambda x: x.importance, reverse=True)
        return insights[:10]  # Return top 10 insights
    
    async def calculate_peer_comparison(self, ticker: str, composite_score: float, 
                                      fiscal_year: int) -> List[PeerComparison]:
        """동종업계 비교 지표를 계산합니다."""
        # Get peer companies' scores for the same fiscal year
        try:
            # This would ideally use industry classification, but we'll use all companies for now
            all_analyses = await db_client.get_investment_recommendations(limit=100)
            
            peer_comparisons = []
            
            for analysis in all_analyses:
                if analysis.get("ticker") != ticker and analysis.get("qualitative_score"):
                    peer_score = float(analysis["qualitative_score"]) / 100.0  # Normalize to 0-1
                    relative_performance = (composite_score - peer_score) / 0.5  # Normalize to -1,1
                    relative_performance = max(-1.0, min(1.0, relative_performance))
                    
                    peer_comparisons.append(PeerComparison(
                        peer_ticker=analysis["ticker"],
                        peer_score=peer_score,
                        relative_performance=round(relative_performance, 3)
                    ))
            
            # Sort by relative performance
            peer_comparisons.sort(key=lambda x: x.relative_performance, reverse=True)
            return peer_comparisons[:10]  # Return top 10 peers
            
        except Exception as e:
            logger.error(f"Error calculating peer comparison: {e}")
            return []
    
    def calculate_historical_trend(self, historical_scores: List[Dict]) -> Dict[str, float]:
        """과거 추세 분석을 계산합니다."""
        if len(historical_scores) < 2:
            return {}
        
        # Sort by fiscal year
        historical_scores.sort(key=lambda x: x.get("fiscal_year", 0))
        
        # Calculate trends
        scores = [score.get("qualitative_score", 0) for score in historical_scores]
        years = [score.get("fiscal_year", 0) for score in historical_scores]
        
        if len(scores) < 2:
            return {}
        
        # Simple linear trend
        trend_slope = (scores[-1] - scores[0]) / (years[-1] - years[0]) if years[-1] != years[0] else 0
        
        # Volatility (standard deviation)
        volatility = np.std(scores) if len(scores) > 1 else 0
        
        # Direction
        direction = 1 if trend_slope > 0.1 else (-1 if trend_slope < -0.1 else 0)
        
        return {
            "trend_slope": round(trend_slope, 3),
            "volatility": round(volatility, 3),
            "direction": direction,
            "latest_score": scores[-1],
            "years_analyzed": len(scores)
        }
    
    async def create_investment_analysis(self, qualitative_score: QualitativeScore) -> InvestmentAnalysis:
        """종합적인 투자 분석을 생성합니다."""
        logger.info(f"Creating investment analysis for {qualitative_score.ticker} {qualitative_score.fiscal_year}")
        
        # Calculate scoring components
        components = self.calculate_scoring_components(qualitative_score)
        
        # Determine recommendation
        recommendation, confidence = self.determine_recommendation(
            components.composite_score, components
        )
        
        # Generate insights
        insights = self.generate_investment_insights(qualitative_score, components)
        
        # Get peer comparison
        peer_comparisons = await self.calculate_peer_comparison(
            qualitative_score.ticker, 
            components.composite_score,
            qualitative_score.fiscal_year
        )
        
        # Get historical trend (get past analyses for this ticker)
        historical_analyses = await db_client.get_company_analysis_history(qualitative_score.ticker)
        historical_trend = self.calculate_historical_trend(historical_analyses)
        
        # Prepare insights for database storage
        key_strengths = [insight.description for insight in insights if insight.category == "strength"]
        key_concerns = [insight.description for insight in insights if insight.category == "concern"]
        risk_factors = [insight.description for insight in insights if insight.category == "risk"]
        growth_opportunities = [insight.description for insight in insights if insight.category == "opportunity"]
        
        # Prepare peer comparison for storage
        peer_comparison_dict = {}
        if peer_comparisons:
            peer_comparison_dict = {
                "avg_peer_score": np.mean([p.peer_score for p in peer_comparisons]),
                "percentile_rank": len([p for p in peer_comparisons if p.relative_performance > 0]) / len(peer_comparisons),
                "top_peers": [{"ticker": p.peer_ticker, "score": p.peer_score} for p in peer_comparisons[:3]]
            }
        
        return InvestmentAnalysis(
            filing_id=qualitative_score.filing_id,
            ticker=qualitative_score.ticker,
            fiscal_year=qualitative_score.fiscal_year,
            qualitative_score=components.composite_score * 100,  # Convert to 0-100 scale
            sentiment_weight=self.score_weights.SENTIMENT_WEIGHT,
            risk_weight=self.score_weights.RISK_WEIGHT,
            opportunity_weight=self.score_weights.GROWTH_WEIGHT,
            recommendation=recommendation,
            confidence=confidence,
            target_price_adjustment=self._calculate_target_price_adjustment(components),
            key_strengths=key_strengths,
            key_concerns=key_concerns,
            risk_factors=risk_factors,
            growth_opportunities=growth_opportunities,
            peer_comparison=peer_comparison_dict,
            historical_trend=historical_trend,
            analyst_notes=f"Analysis based on {components.composite_score:.2f} composite score"
        )
    
    def _calculate_target_price_adjustment(self, components: ScoringComponents) -> float:
        """제안된 목표가 조정 비율을 계산합니다."""
        # Base adjustment on composite score deviation from neutral (0.5)
        base_adjustment = (components.composite_score - 0.5) * 20  # ±10% base adjustment
        
        # Adjust for specific factors
        if components.growth_score > 0.7:
            base_adjustment += 5  # Premium for strong growth
        
        if components.risk_score < 0.3:
            base_adjustment -= 10  # Discount for high risk
        
        if components.sentiment_score > 0.8:
            base_adjustment += 3  # Premium for very positive sentiment
        
        # Cap adjustment at ±25%
        return round(max(-25.0, min(25.0, base_adjustment)), 1)
    
    async def batch_score_companies(self, fiscal_year: int) -> List[InvestmentAnalysis]:
        """회계연도 내 모든 기업의 투자 점수를 일괄 처리합니다."""
        logger.info(f"Batch scoring companies for fiscal year {fiscal_year}")
        
        # This would get all qualitative scores for the fiscal year
        # For now, we'll simulate this functionality
        analyses = []
        
        try:
            # Get all investment analyses (would be filtered by fiscal year in practice)
            existing_analyses = await db_client.get_investment_recommendations(limit=100)
            
            logger.info(f"Found {len(existing_analyses)} existing analyses")
            
            # This would iterate through qualitative scores and create analyses
            # Implementation would depend on the actual data structure
            
        except Exception as e:
            logger.error(f"Error in batch scoring: {e}")
        
        return analyses
    
    def generate_market_summary(self, analyses: List[InvestmentAnalysis]) -> Dict[str, Any]:
        """시장 전반에 대한 투자 요약을 생성합니다."""
        if not analyses:
            return {}
        
        recommendations = [analysis.recommendation for analysis in analyses]
        scores = [analysis.qualitative_score for analysis in analyses]
        
        summary = {
            "total_companies": len(analyses),
            "average_score": round(np.mean(scores), 2),
            "score_std": round(np.std(scores), 2),
            "recommendation_distribution": {
                "strong_buy": len([r for r in recommendations if r == InvestmentRecommendation.STRONG_BUY]),
                "buy": len([r for r in recommendations if r == InvestmentRecommendation.BUY]),
                "hold": len([r for r in recommendations if r == InvestmentRecommendation.HOLD]),
                "sell": len([r for r in recommendations if r == InvestmentRecommendation.SELL]),
                "strong_sell": len([r for r in recommendations if r == InvestmentRecommendation.STRONG_SELL])
            },
            "top_performers": sorted(analyses, key=lambda x: x.qualitative_score, reverse=True)[:10],
            "bottom_performers": sorted(analyses, key=lambda x: x.qualitative_score)[:5]
        }
        
        return summary


async def main():
    """투자 점수 기능을 테스트합니다."""
    scorer = InvestmentScorer()
    
    # Create mock qualitative score
    from src.database.schema import QualitativeScore
    
    mock_score = QualitativeScore(
        filing_id="test_filing",
        ticker="AAPL",
        fiscal_year=2024,
        business_sentiment=0.3,
        risk_factors_sentiment=-0.2,
        mda_sentiment=0.1,
        overall_sentiment=0.1,
        risk_score=0.4,
        risk_diversity=0.5,
        risk_severity=0.3,
        growth_indicators=0.7,
        innovation_mentions=0.8,
        competitive_advantage=0.6,
        market_expansion=0.4,
        management_tone=0.6,
        strategic_clarity=0.7,
        transparency_score=0.8,
        financial_stress_indicators=0.2,
        cash_flow_mentions=0.8,
        debt_concerns=0.3,
        qualitative_score=65.5,
        investment_attractiveness=0.7
    )
    
    # Test scoring
    components = scorer.calculate_scoring_components(mock_score)
    print(f"Scoring Components:")
    print(f"  Sentiment: {components.sentiment_score}")
    print(f"  Risk: {components.risk_score}")
    print(f"  Growth: {components.growth_score}")
    print(f"  Management: {components.management_score}")
    print(f"  Financial Health: {components.financial_health_score}")
    print(f"  Composite: {components.composite_score}")
    
    # Test recommendation
    recommendation, confidence = scorer.determine_recommendation(components.composite_score, components)
    print(f"Recommendation: {recommendation} (confidence: {confidence})")
    
    # Test insights
    insights = scorer.generate_investment_insights(mock_score, components)
    print(f"Top Insights:")
    for insight in insights[:3]:
        print(f"  {insight.category}: {insight.description}")


if __name__ == "__main__":
    asyncio.run(main())