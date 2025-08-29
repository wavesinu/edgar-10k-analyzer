"""EDGAR 10-K 분석을 위한 데이터베이스 스키마 및 모델."""

from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
from datetime import datetime, date
from enum import Enum


class AnalysisStatus(str, Enum):
    """분석 프로세스의 상태."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress" 
    COMPLETED = "completed"
    FAILED = "failed"


class InvestmentRecommendation(str, Enum):
    """투자 추천 카테고리."""
    STRONG_BUY = "strong_buy"
    BUY = "buy"
    HOLD = "hold"
    SELL = "sell"
    STRONG_SELL = "strong_sell"


class Company(BaseModel):
    """회사 정보 모델."""
    id: Optional[str] = None
    ticker: str
    cik: str
    company_name: str
    exchange: str = "NASDAQ"
    sector: Optional[str] = None
    industry: Optional[str] = None
    market_cap: Optional[float] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class Filing(BaseModel):
    """10-K 파일링 모델."""
    id: Optional[str] = None
    company_id: str
    ticker: str
    cik: str
    accession_number: str
    form_type: str = "10-K"
    filing_date: date
    report_date: date
    fiscal_year: int
    fiscal_quarter: Optional[int] = None
    edgar_url: str
    html_url: Optional[str] = None
    xml_url: Optional[str] = None
    pdf_url: Optional[str] = None
    file_size: Optional[int] = None
    status: AnalysisStatus = AnalysisStatus.PENDING
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class QualitativeSection(BaseModel):
    """10-K에서 추출된 정성적 섹션."""
    id: Optional[str] = None
    filing_id: str
    section_name: str  # 예: "business", "risk_factors", "mda"
    section_title: str  # 예: "Item 1. Business"
    content: str
    word_count: int
    char_count: int
    created_at: Optional[datetime] = None


class SentimentAnalysis(BaseModel):
    """감정 분석 결과."""
    id: Optional[str] = None
    section_id: str
    filing_id: str
    overall_sentiment: float  # -1.0 (매우 부정적) ~ 1.0 (매우 긍정적)
    confidence: float  # 0.0 to 1.0
    positive_score: float
    negative_score: float
    neutral_score: float
    sentiment_label: str  # "positive", "negative", "neutral"
    model_used: str = "textblob"
    created_at: Optional[datetime] = None


class KeyTheme(BaseModel):
    """정성적 섹션에서 식별된 핵심 주제."""
    id: Optional[str] = None
    section_id: str
    filing_id: str
    theme_name: str
    theme_description: Optional[str] = None
    relevance_score: float  # 0.0 to 1.0
    keyword_count: int
    keywords: List[str] = []
    context_snippets: List[str] = []
    created_at: Optional[datetime] = None


class RiskFactor(BaseModel):
    """식별되고 분류된 위험 요소."""
    id: Optional[str] = None
    filing_id: str
    risk_category: str  # e.g., "market", "operational", "regulatory"
    risk_description: str
    risk_severity: float  # 0.0 to 1.0
    risk_likelihood: Optional[float] = None
    impact_keywords: List[str] = []
    mitigation_mentioned: bool = False
    created_at: Optional[datetime] = None


class QualitativeScore(BaseModel):
    """파일링에 대한 전반적인 정성적 점수."""
    id: Optional[str] = None
    filing_id: str
    ticker: str
    fiscal_year: int
    
    # Sentiment Scores
    business_sentiment: float
    risk_factors_sentiment: float
    mda_sentiment: float
    overall_sentiment: float
    
    # Risk Scores
    risk_score: float  # 0.0 (low risk) to 1.0 (high risk)
    risk_diversity: float  # How diverse the risks are
    risk_severity: float  # Average severity of identified risks
    
    # Opportunity Scores
    growth_indicators: float  # 0.0 to 1.0
    innovation_mentions: float
    competitive_advantage: float
    market_expansion: float
    
    # Management Quality Indicators
    management_tone: float
    strategic_clarity: float
    transparency_score: float
    
    # Financial Health Indicators from Text
    financial_stress_indicators: float
    cash_flow_mentions: float
    debt_concerns: float
    
    # Overall Composite Scores
    qualitative_score: float  # 0.0 to 100.0
    investment_attractiveness: float  # 0.0 to 1.0
    
    created_at: Optional[datetime] = None


class InvestmentAnalysis(BaseModel):
    """최종 투자 분석 및 추천."""
    id: Optional[str] = None
    filing_id: str
    ticker: str
    fiscal_year: int
    
    # Scoring Components
    qualitative_score: float  # From QualitativeScore
    sentiment_weight: float = 0.3
    risk_weight: float = 0.4
    opportunity_weight: float = 0.3
    
    # Final Recommendation
    recommendation: InvestmentRecommendation
    confidence: float  # 0.0 to 1.0
    target_price_adjustment: Optional[float] = None  # Percentage adjustment
    
    # Supporting Analysis
    key_strengths: List[str] = []
    key_concerns: List[str] = []
    risk_factors: List[str] = []
    growth_opportunities: List[str] = []
    
    # Comparative Analysis
    peer_comparison: Optional[Dict[str, float]] = None
    historical_trend: Optional[Dict[str, float]] = None
    
    # Metadata
    analysis_date: datetime = Field(default_factory=datetime.utcnow)
    analyst_notes: Optional[str] = None
    created_at: Optional[datetime] = None


# Database table creation SQL
CREATE_TABLES_SQL = """
-- Companies table
CREATE TABLE IF NOT EXISTS companies (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ticker VARCHAR(10) UNIQUE NOT NULL,
    cik VARCHAR(10) UNIQUE NOT NULL,
    company_name TEXT NOT NULL,
    exchange VARCHAR(20) DEFAULT 'NASDAQ',
    sector TEXT,
    industry TEXT,
    market_cap DECIMAL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Filings table
CREATE TABLE IF NOT EXISTS filings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID REFERENCES companies(id),
    ticker VARCHAR(10) NOT NULL,
    cik VARCHAR(10) NOT NULL,
    accession_number VARCHAR(25) UNIQUE NOT NULL,
    form_type VARCHAR(10) DEFAULT '10-K',
    filing_date DATE NOT NULL,
    report_date DATE NOT NULL,
    fiscal_year INTEGER NOT NULL,
    fiscal_quarter INTEGER,
    edgar_url TEXT NOT NULL,
    html_url TEXT,
    xml_url TEXT,
    pdf_url TEXT,
    file_size BIGINT,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(ticker, fiscal_year)
);

-- Qualitative sections table
CREATE TABLE IF NOT EXISTS qualitative_sections (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    filing_id UUID REFERENCES filings(id) ON DELETE CASCADE,
    section_name VARCHAR(50) NOT NULL,
    section_title TEXT NOT NULL,
    content TEXT NOT NULL,
    word_count INTEGER,
    char_count INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Sentiment analysis table
CREATE TABLE IF NOT EXISTS sentiment_analysis (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    section_id UUID REFERENCES qualitative_sections(id) ON DELETE CASCADE,
    filing_id UUID REFERENCES filings(id) ON DELETE CASCADE,
    overall_sentiment DECIMAL(4,3),
    confidence DECIMAL(4,3),
    positive_score DECIMAL(4,3),
    negative_score DECIMAL(4,3),
    neutral_score DECIMAL(4,3),
    sentiment_label VARCHAR(20),
    model_used VARCHAR(50) DEFAULT 'textblob',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Key themes table
CREATE TABLE IF NOT EXISTS key_themes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    section_id UUID REFERENCES qualitative_sections(id) ON DELETE CASCADE,
    filing_id UUID REFERENCES filings(id) ON DELETE CASCADE,
    theme_name VARCHAR(100) NOT NULL,
    theme_description TEXT,
    relevance_score DECIMAL(4,3),
    keyword_count INTEGER,
    keywords TEXT[], -- Array of keywords
    context_snippets TEXT[], -- Array of context snippets
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Risk factors table
CREATE TABLE IF NOT EXISTS risk_factors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    filing_id UUID REFERENCES filings(id) ON DELETE CASCADE,
    risk_category VARCHAR(50),
    risk_description TEXT NOT NULL,
    risk_severity DECIMAL(4,3),
    risk_likelihood DECIMAL(4,3),
    impact_keywords TEXT[],
    mitigation_mentioned BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Qualitative scores table
CREATE TABLE IF NOT EXISTS qualitative_scores (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    filing_id UUID REFERENCES filings(id) ON DELETE CASCADE,
    ticker VARCHAR(10) NOT NULL,
    fiscal_year INTEGER NOT NULL,
    
    -- Sentiment scores
    business_sentiment DECIMAL(4,3),
    risk_factors_sentiment DECIMAL(4,3),
    mda_sentiment DECIMAL(4,3),
    overall_sentiment DECIMAL(4,3),
    
    -- Risk scores
    risk_score DECIMAL(4,3),
    risk_diversity DECIMAL(4,3),
    risk_severity DECIMAL(4,3),
    
    -- Opportunity scores
    growth_indicators DECIMAL(4,3),
    innovation_mentions DECIMAL(4,3),
    competitive_advantage DECIMAL(4,3),
    market_expansion DECIMAL(4,3),
    
    -- Management quality
    management_tone DECIMAL(4,3),
    strategic_clarity DECIMAL(4,3),
    transparency_score DECIMAL(4,3),
    
    -- Financial health indicators
    financial_stress_indicators DECIMAL(4,3),
    cash_flow_mentions DECIMAL(4,3),
    debt_concerns DECIMAL(4,3),
    
    -- Composite scores
    qualitative_score DECIMAL(5,2), -- 0.00 to 100.00
    investment_attractiveness DECIMAL(4,3),
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(ticker, fiscal_year)
);

-- Investment analysis table
CREATE TABLE IF NOT EXISTS investment_analysis (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    filing_id UUID REFERENCES filings(id) ON DELETE CASCADE,
    ticker VARCHAR(10) NOT NULL,
    fiscal_year INTEGER NOT NULL,
    
    qualitative_score DECIMAL(5,2),
    sentiment_weight DECIMAL(3,2) DEFAULT 0.30,
    risk_weight DECIMAL(3,2) DEFAULT 0.40,
    opportunity_weight DECIMAL(3,2) DEFAULT 0.30,
    
    recommendation VARCHAR(20) NOT NULL,
    confidence DECIMAL(4,3),
    target_price_adjustment DECIMAL(5,2),
    
    key_strengths TEXT[],
    key_concerns TEXT[],
    risk_factors TEXT[],
    growth_opportunities TEXT[],
    
    peer_comparison JSONB,
    historical_trend JSONB,
    
    analysis_date TIMESTAMPTZ DEFAULT NOW(),
    analyst_notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(ticker, fiscal_year)
);

-- Indexes for better performance
CREATE INDEX IF NOT EXISTS idx_companies_ticker ON companies(ticker);
CREATE INDEX IF NOT EXISTS idx_companies_cik ON companies(cik);
CREATE INDEX IF NOT EXISTS idx_filings_ticker_year ON filings(ticker, fiscal_year);
CREATE INDEX IF NOT EXISTS idx_filings_status ON filings(status);
CREATE INDEX IF NOT EXISTS idx_qualitative_sections_filing_id ON qualitative_sections(filing_id);
CREATE INDEX IF NOT EXISTS idx_sentiment_filing_id ON sentiment_analysis(filing_id);
CREATE INDEX IF NOT EXISTS idx_investment_analysis_ticker ON investment_analysis(ticker);
CREATE INDEX IF NOT EXISTS idx_investment_analysis_recommendation ON investment_analysis(recommendation);

-- Row Level Security (RLS) policies can be added here if needed
-- ALTER TABLE companies ENABLE ROW LEVEL SECURITY;
-- etc.
"""