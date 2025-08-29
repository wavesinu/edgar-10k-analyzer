"""Supabase 데이터베이스 연결 및 운영."""

import asyncio
from typing import Dict, List, Optional, Any
from supabase import create_client, Client
from loguru import logger
import json

from config.settings import settings
from .schema import (
    Company, Filing, QualitativeSection, SentimentAnalysis,
    KeyTheme, RiskFactor, QualitativeScore, InvestmentAnalysis,
    CREATE_TABLES_SQL
)


class SupabaseClient:
    """EDGAR 분석을 위한 Supabase 데이터베이스 클라이언트."""
    
    def __init__(self):
        self.client: Optional[Client] = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Supabase 클라이언트 초기화."""
        try:
            self.client = create_client(
                supabase_url=settings.supabase_url,
                supabase_key=settings.supabase_key
            )
            logger.info("Supabase 클라이언트 초기화 성공")
        except Exception as e:
            logger.error(f"Supabase 클라이언트 초기화 실패: {e}")
            raise
    
    
    # 회사 운영
    async def insert_company(self, company: Company) -> Dict[str, Any]:
        """새 회사 레코드 삽입."""
        try:
            data = company.dict(exclude_none=True, exclude={"id"})
            response = self.client.table("companies").insert(data).execute()
            logger.info(f"회사 삽입 완료: {company.ticker}")
            return response.data[0] if response.data else {}
        except Exception as e:
            logger.error(f"회사 {company.ticker} 삽입 오류: {e}")
            raise
    
    async def get_company_by_ticker(self, ticker: str) -> Optional[Dict[str, Any]]:
        """티커 심볼로 회사 가져오기."""
        try:
            response = self.client.table("companies").select("*").eq("ticker", ticker).execute()
            return response.data[0] if response.data else None
        except Exception as e:
            logger.error(f"회사 {ticker} 가져오기 오류: {e}")
            return None
    
    async def upsert_company(self, company: Company) -> Dict[str, Any]:
        """회사 레코드 삽입 또는 업데이트."""
        try:
            data = company.dict(exclude_none=True, exclude={"id"})
            response = self.client.table("companies").upsert(data, on_conflict="ticker").execute()
            logger.info(f"회사 업서트 완료: {company.ticker}")
            return response.data[0] if response.data else {}
        except Exception as e:
            logger.error(f"회사 {company.ticker} 업서트 오류: {e}")
            raise
    
    # 파일링 운영
    async def insert_filing(self, filing: Filing) -> Dict[str, Any]:
        """새 파일링 레코드 삽입."""
        try:
            data = filing.dict(exclude_none=True, exclude={"id"})
            response = self.client.table("filings").insert(data).execute()
            logger.info(f"파일링 삽입 완료: {filing.ticker} {filing.fiscal_year}")
            return response.data[0] if response.data else {}
        except Exception as e:
            logger.error(f"파일링 {filing.ticker} {filing.fiscal_year} 삽입 오류: {e}")
            raise
    
    async def get_filing_by_ticker_year(self, ticker: str, fiscal_year: int) -> Optional[Dict[str, Any]]:
        """티커와 회계연도로 파일링 가져오기."""
        try:
            response = (self.client.table("filings")
                       .select("*")
                       .eq("ticker", ticker)
                       .eq("fiscal_year", fiscal_year)
                       .execute())
            return response.data[0] if response.data else None
        except Exception as e:
            logger.error(f"파일링 {ticker} {fiscal_year} 가져오기 오류: {e}")
            return None
    
    async def update_filing_status(self, filing_id: str, status: str) -> bool:
        """파일링 처리 상태 업데이트."""
        try:
            response = (self.client.table("filings")
                       .update({"status": status, "updated_at": "NOW()"})
                       .eq("id", filing_id)
                       .execute())
            return len(response.data) > 0
        except Exception as e:
            logger.error(f"파일링 상태 업데이트 오류: {e}")
            return False
    
    # 정성적 섹션 운영
    async def insert_qualitative_section(self, section: QualitativeSection) -> Dict[str, Any]:
        """정성적 섹션 삽입."""
        try:
            data = section.dict(exclude_none=True, exclude={"id"})
            response = self.client.table("qualitative_sections").insert(data).execute()
            return response.data[0] if response.data else {}
        except Exception as e:
            logger.error(f"정성적 섹션 삽입 오류: {e}")
            raise
    
    async def get_sections_by_filing(self, filing_id: str) -> List[Dict[str, Any]]:
        """파일링의 모든 정성적 섹션 가져오기."""
        try:
            response = (self.client.table("qualitative_sections")
                       .select("*")
                       .eq("filing_id", filing_id)
                       .execute())
            return response.data or []
        except Exception as e:
            logger.error(f"파일링 {filing_id}의 섹션 가져오기 오류: {e}")
            return []
    
    # 감정 분석 운영
    async def insert_sentiment_analysis(self, sentiment: SentimentAnalysis) -> Dict[str, Any]:
        """감정 분석 결과 삽입."""
        try:
            data = sentiment.dict(exclude_none=True, exclude={"id"})
            response = self.client.table("sentiment_analysis").insert(data).execute()
            return response.data[0] if response.data else {}
        except Exception as e:
            logger.error(f"감정 분석 삽입 오류: {e}")
            raise
    
    # 핵심 주제 운영
    async def insert_key_theme(self, theme: KeyTheme) -> Dict[str, Any]:
        """핵심 주제 삽입."""
        try:
            data = theme.dict(exclude_none=True, exclude={"id"})
            response = self.client.table("key_themes").insert(data).execute()
            return response.data[0] if response.data else {}
        except Exception as e:
            logger.error(f"핵심 주제 삽입 오류: {e}")
            raise
    
    # 위험 요소 운영
    async def insert_risk_factor(self, risk: RiskFactor) -> Dict[str, Any]:
        """위험 요소 삽입."""
        try:
            data = risk.dict(exclude_none=True, exclude={"id"})
            response = self.client.table("risk_factors").insert(data).execute()
            return response.data[0] if response.data else {}
        except Exception as e:
            logger.error(f"위험 요소 삽입 오류: {e}")
            raise
    
    # 정성적 점수 운영
    async def insert_qualitative_score(self, score: QualitativeScore) -> Dict[str, Any]:
        """정성적 점수 결과 삽입."""
        try:
            data = score.dict(exclude_none=True, exclude={"id"})
            response = self.client.table("qualitative_scores").upsert(data, on_conflict="ticker,fiscal_year").execute()
            return response.data[0] if response.data else {}
        except Exception as e:
            logger.error(f"정성적 점수 삽입 오류: {e}")
            raise
    
    # 투자 분석 운영
    async def insert_investment_analysis(self, analysis: InvestmentAnalysis) -> Dict[str, Any]:
        """투자 분석 및 추천 삽입."""
        try:
            data = analysis.dict(exclude_none=True, exclude={"id"})
            # 복잡한 필드를 저장을 위해 JSON 문자열로 변환
            if data.get("peer_comparison"):
                data["peer_comparison"] = json.dumps(data["peer_comparison"])
            if data.get("historical_trend"):
                data["historical_trend"] = json.dumps(data["historical_trend"])
            
            response = self.client.table("investment_analysis").upsert(data, on_conflict="ticker,fiscal_year").execute()
            return response.data[0] if response.data else {}
        except Exception as e:
            logger.error(f"투자 분석 삽입 오류: {e}")
            raise
    
    async def get_investment_recommendations(self, limit: int = 50) -> List[Dict[str, Any]]:
        """최신 투자 추천 가져오기."""
        try:
            response = (self.client.table("investment_analysis")
                       .select("ticker, recommendation, confidence, qualitative_score, analysis_date")
                       .order("analysis_date", desc=True)
                       .limit(limit)
                       .execute())
            return response.data or []
        except Exception as e:
            logger.error(f"투자 추천 가져오기 오류: {e}")
            return []
    
    async def get_company_analysis_history(self, ticker: str) -> List[Dict[str, Any]]:
        """특정 회사의 분석 내역 가져오기."""
        try:
            response = (self.client.table("investment_analysis")
                       .select("*")
                       .eq("ticker", ticker)
                       .order("fiscal_year", desc=True)
                       .execute())
            return response.data or []
        except Exception as e:
            logger.error(f"{ticker}의 분석 내역 가져오기 오류: {e}")
            return []
    
    # 유틸리티 메소드
    async def get_processing_stats(self) -> Dict[str, int]:
        """처리 통계 가져오기."""
        try:
            # 상태별 파일링 수 카운트
            response = (self.client.table("filings")
                       .select("status", count="exact")
                       .execute())
            
            stats = {
                "total_companies": 0,
                "total_filings": 0,
                "pending_filings": 0,
                "completed_filings": 0,
                "failed_filings": 0
            }
            
            # 회사 수 가져오기
            company_response = self.client.table("companies").select("*", count="exact").execute()
            stats["total_companies"] = company_response.count or 0
            
            # 파일링 수 가져오기
            filing_response = self.client.table("filings").select("*", count="exact").execute()
            stats["total_filings"] = filing_response.count or 0
            
            return stats
        except Exception as e:
            logger.error(f"처리 통계 가져오기 오류: {e}")
            return {}


# 전역 데이터베이스 클라이언트 인스턴스
db_client = SupabaseClient()