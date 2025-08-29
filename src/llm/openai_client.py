"""고급 금융 분석을 위한 OpenAI ChatGPT 통합."""

import asyncio
import json
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
import tiktoken

import openai
from openai import AsyncOpenAI
from loguru import logger

from config.settings import settings


@dataclass
class LLMAnalysisRequest:
    """LLM 분석을 위한 요청 구조."""
    company_ticker: str
    fiscal_year: int
    sections_data: Dict[str, str]  # section_name -> content
    quantitative_scores: Dict[str, float]
    analysis_type: str  # "comprehensive", "risk_assessment", "investment_recommendation", etc.
    context: Optional[Dict[str, Any]] = None


@dataclass
class LLMAnalysisResponse:
    """LLM 분석에서 반환되는 응답 구조."""
    analysis_id: str
    company_ticker: str
    fiscal_year: int
    analysis_type: str
    
    # LLM Generated Content
    executive_summary: str
    investment_thesis: str
    key_strengths: List[str]
    key_risks: List[str]
    competitive_analysis: str
    management_assessment: str
    financial_outlook: str
    investment_recommendation: str
    confidence_level: str
    target_price_rationale: Optional[str]
    
    # Metadata
    model_used: str
    tokens_used: int
    processing_time: float
    timestamp: datetime
    
    # Structured Scores (LLM-enhanced)
    llm_sentiment_score: float
    llm_risk_score: float
    llm_growth_potential: float
    llm_management_quality: float
    llm_competitive_position: float
    llm_overall_score: float


class OpenAIFinancialAnalyst:
    """10-K 분석을 위한 OpenAI 기반 금융 분석가."""
    
    def __init__(self):
        self.client = AsyncOpenAI(api_key=settings.openai_api_key)
        self.model = settings.openai_model
        self.max_tokens = settings.openai_max_tokens
        self.temperature = settings.openai_temperature
        
        # Initialize tokenizer for token counting
        try:
            self.encoder = tiktoken.encoding_for_model(self.model)
        except KeyError:
            self.encoder = tiktoken.get_encoding("cl100k_base")
    
    def count_tokens(self, text: str) -> int:
        """텍스트의 토큰 수를 계산합니다."""
        return len(self.encoder.encode(text))
    
    def truncate_text(self, text: str, max_tokens: int) -> str:
        """토큰 제한에 맞게 텍스트를 자릅니다."""
        tokens = self.encoder.encode(text)
        if len(tokens) <= max_tokens:
            return text
        
        truncated_tokens = tokens[:max_tokens]
        return self.encoder.decode(truncated_tokens)
    
    def prepare_context(self, request: LLMAnalysisRequest) -> str:
        """LLM을 위한 컨텍스트 정보를 준비합니다."""
        context = f"""
COMPANY ANALYSIS REQUEST
========================
Company: {request.company_ticker}
Fiscal Year: {request.fiscal_year}
Analysis Type: {request.analysis_type}

QUANTITATIVE SCORES SUMMARY:
"""
        
        for metric, score in request.quantitative_scores.items():
            context += f"- {metric.replace('_', ' ').title()}: {score:.2f}\n"
        
        if request.context:
            context += f"\nADDITIONAL CONTEXT:\n"
            for key, value in request.context.items():
                context += f"- {key}: {value}\n"
        
        return context
    
    async def analyze_comprehensive(self, request: LLMAnalysisRequest) -> LLMAnalysisResponse:
        """ChatGPT를 사용하여 종합적인 투자 분석을 수행합니다."""
        start_time = datetime.now()
        logger.info(f"Starting comprehensive LLM analysis for {request.company_ticker} {request.fiscal_year}")
        
        # Prepare the analysis prompt
        context = self.prepare_context(request)
        
        # Truncate sections to fit within token limits
        max_section_tokens = 1500  # Reserve tokens for context and response
        processed_sections = {}
        
        for section_name, content in request.sections_data.items():
            if content and len(content.strip()) > 0:
                truncated_content = self.truncate_text(content, max_section_tokens)
                processed_sections[section_name] = truncated_content
        
        sections_text = ""
        for section_name, content in processed_sections.items():
            sections_text += f"\n\n=== {section_name.replace('_', ' ').upper()} SECTION ===\n"
            sections_text += content[:3000]  # Additional safety truncation
        
        prompt = f"""You are a senior financial analyst specializing in qualitative analysis of SEC 10-K filings. 
Your task is to provide a comprehensive investment analysis based on the qualitative factors extracted from a company's 10-K filing.

{context}

10-K FILING SECTIONS:
{sections_text}

Please provide a comprehensive investment analysis with the following structure:

1. EXECUTIVE SUMMARY (2-3 sentences)
2. INVESTMENT THESIS (main investment argument)
3. KEY STRENGTHS (3-5 bullet points)
4. KEY RISKS (3-5 bullet points) 
5. COMPETITIVE ANALYSIS (market position assessment)
6. MANAGEMENT ASSESSMENT (leadership quality evaluation)
7. FINANCIAL OUTLOOK (forward-looking perspective)
8. INVESTMENT RECOMMENDATION (Strong Buy/Buy/Hold/Sell/Strong Sell with rationale)
9. CONFIDENCE LEVEL (High/Medium/Low with explanation)
10. TARGET PRICE RATIONALE (if applicable)

Additionally, provide numerical scores (0.0-1.0) for:
- LLM Sentiment Score
- LLM Risk Score  
- LLM Growth Potential
- LLM Management Quality
- LLM Competitive Position
- LLM Overall Score

Format your response as a structured JSON with the following keys:
{{
    "executive_summary": "...",
    "investment_thesis": "...",
    "key_strengths": ["...", "...", "..."],
    "key_risks": ["...", "...", "..."],
    "competitive_analysis": "...",
    "management_assessment": "...",
    "financial_outlook": "...",
    "investment_recommendation": "...",
    "confidence_level": "...",
    "target_price_rationale": "...",
    "llm_sentiment_score": 0.0,
    "llm_risk_score": 0.0,
    "llm_growth_potential": 0.0,
    "llm_management_quality": 0.0,
    "llm_competitive_position": 0.0,
    "llm_overall_score": 0.0
}}

Important: Base your analysis on factual information from the 10-K filing. Be objective and highlight both positive and negative aspects. Consider the quantitative scores provided as additional context."""

        try:
            # Make API call to OpenAI
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system", 
                        "content": "You are a senior financial analyst with 20+ years of experience in equity research and investment analysis. You specialize in qualitative analysis of SEC filings and provide objective, data-driven investment recommendations."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                max_tokens=self.max_tokens,
                temperature=self.temperature,
                response_format={"type": "json_object"}
            )
            
            # Parse response
            response_content = response.choices[0].message.content
            tokens_used = response.usage.total_tokens
            
            # Parse JSON response
            analysis_data = json.loads(response_content)
            
            # Calculate processing time
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # Create response object
            llm_response = LLMAnalysisResponse(
                analysis_id=f"{request.company_ticker}_{request.fiscal_year}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                company_ticker=request.company_ticker,
                fiscal_year=request.fiscal_year,
                analysis_type=request.analysis_type,
                
                executive_summary=analysis_data.get("executive_summary", ""),
                investment_thesis=analysis_data.get("investment_thesis", ""),
                key_strengths=analysis_data.get("key_strengths", []),
                key_risks=analysis_data.get("key_risks", []),
                competitive_analysis=analysis_data.get("competitive_analysis", ""),
                management_assessment=analysis_data.get("management_assessment", ""),
                financial_outlook=analysis_data.get("financial_outlook", ""),
                investment_recommendation=analysis_data.get("investment_recommendation", ""),
                confidence_level=analysis_data.get("confidence_level", ""),
                target_price_rationale=analysis_data.get("target_price_rationale"),
                
                model_used=self.model,
                tokens_used=tokens_used,
                processing_time=processing_time,
                timestamp=datetime.now(),
                
                llm_sentiment_score=float(analysis_data.get("llm_sentiment_score", 0.5)),
                llm_risk_score=float(analysis_data.get("llm_risk_score", 0.5)),
                llm_growth_potential=float(analysis_data.get("llm_growth_potential", 0.5)),
                llm_management_quality=float(analysis_data.get("llm_management_quality", 0.5)),
                llm_competitive_position=float(analysis_data.get("llm_competitive_position", 0.5)),
                llm_overall_score=float(analysis_data.get("llm_overall_score", 0.5))
            )
            
            logger.info(f"LLM analysis completed for {request.company_ticker}: {tokens_used} tokens, {processing_time:.2f}s")
            return llm_response
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response as JSON: {e}")
            raise
        except Exception as e:
            logger.error(f"Error in LLM analysis: {e}")
            raise
    
    async def analyze_risk_assessment(self, request: LLMAnalysisRequest) -> Dict[str, Any]:
        """집중적인 리스크 평가 분석."""
        context = self.prepare_context(request)
        
        # Focus on risk factors section
        risk_content = request.sections_data.get("risk_factors", "")
        if not risk_content:
            raise ValueError("Risk factors section not available for analysis")
        
        risk_content = self.truncate_text(risk_content, 2000)
        
        prompt = f"""You are a risk assessment specialist. Analyze the following risk factors section from a 10-K filing:

{context}

RISK FACTORS SECTION:
{risk_content}

Provide a structured risk assessment in JSON format:
{{
    "risk_summary": "Overall risk assessment summary",
    "risk_categories": {{
        "market_risk": {{"severity": 0.0, "description": "..."}},
        "operational_risk": {{"severity": 0.0, "description": "..."}},
        "regulatory_risk": {{"severity": 0.0, "description": "..."}},
        "financial_risk": {{"severity": 0.0, "description": "..."}},
        "strategic_risk": {{"severity": 0.0, "description": "..."}}
    }},
    "top_risks": ["...", "...", "..."],
    "risk_mitigation_quality": 0.0,
    "overall_risk_score": 0.0
}}"""

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a senior risk analyst specializing in corporate risk assessment."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=2000,
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            return json.loads(response.choices[0].message.content)
            
        except Exception as e:
            logger.error(f"Error in risk assessment: {e}")
            raise
    
    async def generate_investment_summary(self, request: LLMAnalysisRequest, 
                                        comprehensive_analysis: LLMAnalysisResponse) -> str:
        """대시보드/보고서를 위한 간결한 투자 요약을 생성합니다."""
        
        prompt = f"""Based on the comprehensive analysis of {request.company_ticker}, generate a concise 2-3 sentence investment summary that captures the key investment thesis and recommendation.

Company: {request.company_ticker}
Recommendation: {comprehensive_analysis.investment_recommendation}
Overall Score: {comprehensive_analysis.llm_overall_score:.2f}

Key Strengths: {', '.join(comprehensive_analysis.key_strengths[:3])}
Key Risks: {', '.join(comprehensive_analysis.key_risks[:3])}

Generate a professional, concise summary suitable for executive reporting."""

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a financial analyst creating executive summaries."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=200,
                temperature=0.2
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            logger.error(f"Error generating investment summary: {e}")
            return f"Investment analysis for {request.company_ticker} completed with {comprehensive_analysis.investment_recommendation} recommendation."
    
    async def compare_companies(self, company_analyses: List[LLMAnalysisResponse]) -> Dict[str, Any]:
        """LLM 분석을 사용하여 여러 기업을 비교합니다."""
        if len(company_analyses) < 2:
            raise ValueError("At least 2 company analyses required for comparison")
        
        comparison_data = []
        for analysis in company_analyses:
            comparison_data.append({
                "ticker": analysis.company_ticker,
                "recommendation": analysis.investment_recommendation,
                "overall_score": analysis.llm_overall_score,
                "key_strengths": analysis.key_strengths[:2],  # Top 2 strengths
                "key_risks": analysis.key_risks[:2],          # Top 2 risks
                "investment_thesis": analysis.investment_thesis[:200]  # Truncated thesis
            })
        
        prompt = f"""Compare the following companies based on their investment profiles:

{json.dumps(comparison_data, indent=2)}

Provide a comparative analysis in JSON format:
{{
    "ranking": ["TICKER1", "TICKER2", "..."],
    "best_opportunity": {{"ticker": "...", "rationale": "..."}},
    "highest_risk": {{"ticker": "...", "rationale": "..."}},
    "most_undervalued": {{"ticker": "...", "rationale": "..."}},
    "sector_leader": {{"ticker": "...", "rationale": "..."}},
    "comparative_summary": "...",
    "investment_recommendations": {{
        "conservative_investor": "...",
        "growth_investor": "...",
        "value_investor": "..."
    }}
}}"""

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a portfolio manager comparing investment opportunities."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=1500,
                temperature=0.2,
                response_format={"type": "json_object"}
            )
            
            return json.loads(response.choices[0].message.content)
            
        except Exception as e:
            logger.error(f"Error in company comparison: {e}")
            raise


# Settings integration
async def get_openai_settings():
    """구성에서 OpenAI 설정을 가져옵니다."""
    return {
        "api_key": getattr(settings, 'openai_api_key', None),
        "model": getattr(settings, 'openai_model', 'gpt-4-turbo-preview'),
        "max_tokens": getattr(settings, 'openai_max_tokens', 4000),
        "temperature": getattr(settings, 'openai_temperature', 0.3)
    }


async def main():
    """OpenAI 통합을 테스트합니다."""
    # Test basic functionality
    analyst = OpenAIFinancialAnalyst()
    
    # Mock request
    test_request = LLMAnalysisRequest(
        company_ticker="AAPL",
        fiscal_year=2024,
        sections_data={
            "business": "Apple Inc. designs, manufactures and markets smartphones, personal computers, tablets, wearables and accessories...",
            "risk_factors": "The Company's business can be affected by numerous factors, many of which are beyond the Company's control..."
        },
        quantitative_scores={
            "sentiment_score": 0.65,
            "risk_score": 0.45,
            "growth_score": 0.75
        },
        analysis_type="comprehensive"
    )
    
    try:
        # This would require actual API key
        print("OpenAI integration module loaded successfully")
        print(f"Model: {analyst.model}")
        print(f"Max tokens: {analyst.max_tokens}")
        
    except Exception as e:
        print(f"Error testing OpenAI integration: {e}")


if __name__ == "__main__":
    asyncio.run(main())