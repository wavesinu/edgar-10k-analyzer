"""NASDAQ 기업의 CIK 번호 매핑 유틸리티."""

import json
import asyncio
import aiohttp
import ssl
from typing import Dict, List, Optional, Tuple
from loguru import logger
import time

from config.settings import settings


class CompanyMapper:
    """SEC 데이터를 사용하여 회사 티커를 CIK 번호로 매핑하는 클래스."""
    
    def __init__(self):
        self.base_url = "https://www.sec.gov"
        self.headers = {
            "User-Agent": settings.user_agent,
            "Accept": "application/json"
        }
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """비동기 컨텍스트 매니저 진입."""
        # SSL 컨텍스트 설정 (개발/테스트용)
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        self.session = aiohttp.ClientSession(
            headers=self.headers,
            timeout=aiohttp.ClientTimeout(total=30),
            connector=connector
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """비동기 컨텍스트 매니저 종료."""
        if self.session:
            await self.session.close()
    
    async def get_company_tickers(self) -> Dict[str, Dict]:
        """SEC에서 회사 티커와 CIK 매핑 데이터 가져오기."""
        url = f"{self.base_url}/files/company_tickers.json"
        
        try:
            logger.info("SEC에서 회사 티커 데이터 가져오는 중...")
            async with self.session.get(url) as response:
                response.raise_for_status()
                data = await response.json()
                logger.info(f"{len(data)}개 회사 매핑 데이터 수신 완료")
                return data
        except Exception as e:
            logger.error(f"회사 티커 데이터 가져오기 오류: {e}")
            raise
    
    def format_cik(self, cik: int) -> str:
        """CIK 번호를 앞에 0을 채워 10자리로 포맷팅."""
        return f"{cik:010d}"
    
    async def map_tickers_to_cik(self, tickers: List[str]) -> Dict[str, Dict]:
        """티커 목록을 해당 CIK 정보로 매핑."""
        company_data = await self.get_company_tickers()
        
        # 티커에서 회사 정보로의 매핑 생성
        ticker_mapping = {}
        for key, company_info in company_data.items():
            ticker = company_info.get("ticker", "").upper()
            if ticker:
                ticker_mapping[ticker] = {
                    "cik": self.format_cik(company_info["cik_str"]),
                    "title": company_info["title"],
                    "ticker": ticker
                }
        
        # 요청된 티커에 대한 매칭 결과 찾기
        results = {}
        missing_tickers = []
        
        for ticker in tickers:
            ticker_upper = ticker.upper()
            if ticker_upper in ticker_mapping:
                results[ticker_upper] = ticker_mapping[ticker_upper]
            else:
                missing_tickers.append(ticker_upper)
        
        if missing_tickers:
            logger.warning(f"다음 티커들의 CIK를 찾을 수 없음: {missing_tickers}")
        
        logger.info(f"{len(results)}/{len(tickers)}개 티커를 CIK로 성공적으로 매핑")
        return results
    
    async def get_company_submissions(self, cik: str) -> Dict:
        """SEC에서 회사 제출 내역 가져오기."""
        # submissions API는 data.sec.gov 사용
        submissions_url = "https://data.sec.gov"
        url = f"{submissions_url}/submissions/CIK{cik}.json"
        
        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            logger.error(f"CIK {cik}의 제출 내역 가져오기 오류: {e}")
            return {}
    
    async def get_latest_10k_filing(self, cik: str) -> Optional[Dict]:
        """회사의 가장 최근 10-K 파일링 가져오기."""
        submissions = await self.get_company_submissions(cik)
        
        if not submissions:
            return None
        
        recent_filings = submissions.get("filings", {}).get("recent", {})
        if not recent_filings:
            return None
        
        # 가장 최근의 10-K 파일링 찾기
        forms = recent_filings.get("form", [])
        accession_numbers = recent_filings.get("accessionNumber", [])
        filing_dates = recent_filings.get("filingDate", [])
        report_dates = recent_filings.get("reportDate", [])
        
        for i, form in enumerate(forms):
            if form == "10-K":
                return {
                    "form": form,
                    "accessionNumber": accession_numbers[i],
                    "filingDate": filing_dates[i],
                    "reportDate": report_dates[i],
                    "cik": cik
                }
        
        return None


async def build_company_database() -> Dict[str, Dict]:
    """CIK 매핑이 포함된 상위 NASDAQ 기업들의 종합 데이터베이스 구축."""
    
    async with CompanyMapper() as mapper:
        # 상위 NASDAQ 기업들의 CIK 매핑 가져오기
        company_mappings = await mapper.map_tickers_to_cik(settings.top_nasdaq_tickers)
        
        # 최신 10-K 파일링 정보로 강화
        enhanced_mappings = {}
        
        for ticker, info in company_mappings.items():
            cik = info["cik"]
            logger.info(f"{ticker}의 최신 10-K 파일링 가져오는 중 (CIK: {cik})")
            
            latest_10k = await mapper.get_latest_10k_filing(cik)
            
            enhanced_info = {
                **info,
                "latest_10k": latest_10k
            }
            
            enhanced_mappings[ticker] = enhanced_info
            
            # 요청 속도 제한
            await asyncio.sleep(settings.request_delay)
        
        return enhanced_mappings


if __name__ == "__main__":
    async def main():
        companies = await build_company_database()
        
        # JSON 파일로 저장
        output_file = "data/nasdaq_companies.json"
        with open(output_file, "w") as f:
            json.dump(companies, f, indent=2)
        
        logger.info(f"회사 데이터베이스가 {output_file}에 저장됨")
        
        # 요약 정보 출력
        print(f"\n=== NASDAQ 상위 50개 기업 매핑 ===")
        print(f"매핑된 총 기업 수: {len(companies)}")
        
        companies_with_10k = sum(1 for c in companies.values() if c.get("latest_10k"))
        print(f"10-K 파일링이 있는 기업 수: {companies_with_10k}")
        
        print(f"\n샘플 항목들:")
        for ticker, info in list(companies.items())[:5]:
            print(f"  {ticker}: {info['title'][:50]}... (CIK: {info['cik']})")
    
    asyncio.run(main())