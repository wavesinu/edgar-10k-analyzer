"""10-K 파일링을 가져오고 처리하는 EDGAR API 클라이언트."""

import asyncio
import aiohttp
import json
import re
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, date
from pathlib import Path
from bs4 import BeautifulSoup
from loguru import logger

from config.settings import settings
from src.database.schema import Filing, Company
from src.database.connection import db_client


class EdgarClient:
    """SEC EDGAR API와 상호작용하고 10-K 파일링을 가져오는 클라이언트."""
    
    def __init__(self):
        self.base_url = "https://data.sec.gov"
        self.edgar_archives = "https://www.sec.gov/Archives/edgar/data"
        self.headers = {
            "User-Agent": settings.user_agent,
            "Accept": "application/json, text/html, application/xhtml+xml, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """비동기 컨텍스트 매니저 진입."""
        connector = aiohttp.TCPConnector(limit=settings.max_concurrent_requests)
        self.session = aiohttp.ClientSession(
            headers=self.headers,
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=60, connect=30)
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """비동기 컨텍스트 매니저 종료."""
        if self.session:
            await self.session.close()
    
    async def get_company_submissions(self, cik: str) -> Dict[str, Any]:
        """SEC에서 회사 제출 내역 가져오기."""
        url = f"{self.base_url}/submissions/CIK{cik.zfill(10)}.json"
        
        try:
            logger.info(f"CIK {cik}의 제출 내역 가져오는 중")
            async with self.session.get(url) as response:
                response.raise_for_status()
                data = await response.json()
                logger.info(f"CIK {cik}의 최신 파일링 {len(data.get('filings', {}).get('recent', {}).get('form', []))}개 수신")
                return data
        except aiohttp.ClientResponseError as e:
            logger.error(f"CIK {cik}의 제출 내역 가져오기 HTTP 오류: {e.status} - {e.message}")
            return {}
        except Exception as e:
            logger.error(f"CIK {cik}의 제출 내역 가져오기 오류: {e}")
            return {}
    
    def extract_10k_filings(self, submissions: Dict[str, Any], limit: int = 5) -> List[Dict[str, Any]]:
        """제출 데이터에서 10-K 파일링 추출."""
        recent_filings = submissions.get("filings", {}).get("recent", {})
        if not recent_filings:
            return []
        
        forms = recent_filings.get("form", [])
        accession_numbers = recent_filings.get("accessionNumber", [])
        filing_dates = recent_filings.get("filingDate", [])
        report_dates = recent_filings.get("reportDate", [])
        acceptance_dates = recent_filings.get("acceptanceDateTime", [])
        
        filings = []
        count = 0
        
        for i, form in enumerate(forms):
            if count >= limit:
                break
                
            if form == "10-K":
                try:
                    filing_date = datetime.strptime(filing_dates[i], "%Y-%m-%d").date()
                    report_date = datetime.strptime(report_dates[i], "%Y-%m-%d").date()
                    
                    filing = {
                        "form": form,
                        "accessionNumber": accession_numbers[i],
                        "filingDate": filing_date,
                        "reportDate": report_date,
                        "acceptanceDateTime": acceptance_dates[i],
                        "fiscalYear": report_date.year,
                        "index": i
                    }
                    filings.append(filing)
                    count += 1
                except (IndexError, ValueError) as e:
                    logger.warning(f"인덱스 {i}에서 파일링 데이터 파싱 오류: {e}")
                    continue
        
        logger.info(f"{len(filings)}개의 10-K 파일링을 발견")
        return filings
    
    def build_filing_urls(self, cik: str, accession_number: str) -> Dict[str, str]:
        """다양한 파일링 형식의 URL 구축."""
        # URL 경로를 위해 액세션 번호에서 대시 제거
        accession_clean = accession_number.replace("-", "")
        cik_clean = cik.zfill(10)
        
        base_path = f"{self.edgar_archives}/{cik_clean}/{accession_clean}"
        
        return {
            "edgar_url": f"{self.base_url}/submissions/CIK{cik_clean}.json",
            "html_url": f"{base_path}/{accession_number}.htm",
            "xml_url": f"{base_path}/{accession_number}.xml", 
            "index_url": f"{base_path}/{accession_number}-index.htm"
        }
    
    async def fetch_filing_content(self, url: str) -> Optional[str]:
        """URL에서 파일링 컨텐트 가져오기."""
        try:
            logger.debug(f"다음 주소에서 컨텐트 가져오는 중: {url}")
            async with self.session.get(url) as response:
                if response.status == 404:
                    logger.warning(f"URL에서 파일링을 찾을 수 없음: {url}")
                    return None
                response.raise_for_status()
                content = await response.text()
                logger.debug(f"{url}에서 {len(content)}자 성공적으로 가져온다")
                return content
        except aiohttp.ClientResponseError as e:
            logger.error(f"{url} 가져오기 HTTP 오류: {e.status} - {e.message}")
            return None
        except Exception as e:
            logger.error(f"{url}에서 컨텐트 가져오기 오류: {e}")
            return None
    
    async def get_filing_html_content(self, cik: str, accession_number: str) -> Optional[str]:
        """특정 파일링의 HTML 컨텐트 가져오기."""
        urls = self.build_filing_urls(cik, accession_number)
        
        # 먼저 HTML 시도
        content = await self.fetch_filing_content(urls["html_url"])
        if content:
            return content
        
        # HTML을 사용할 수 없는 경우 인덱스 페이지 시도
        content = await self.fetch_filing_content(urls["index_url"])
        if content:
            # 인덱스를 파싱하여 메인 문서 링크 찾기
            soup = BeautifulSoup(content, 'html.parser')
            # 메인 10-K 문서 링크 찾기
            for link in soup.find_all('a', href=True):
                href = link['href']
                if '10-k.htm' in href.lower() or '10k.htm' in href.lower():
                    full_url = f"https://www.sec.gov{href}" if href.startswith('/') else href
                    return await self.fetch_filing_content(full_url)
        
        logger.warning(f"CIK {cik}, 액세션 {accession_number}의 HTML 컨텐트를 가져올 수 없음")
        return None
    
    def extract_document_sections(self, html_content: str) -> Dict[str, str]:
        """10-K HTML 컨텐트에서 핀심 섹션 추출."""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        sections = {
            "business": "",
            "risk_factors": "",
            "mda": "",  # Management Discussion and Analysis
            "financial_statements": ""
        }
        
        # 처리를 위해 텍스트로 변환
        text = soup.get_text()
        
        # 섹션 패턴 정의 (대소문자 구분 없음)
        section_patterns = {
            "business": [
                r"item\s+1\s*[.\-–—]\s*business",
                r"item\s+1\b.*?business",
                r"business\s+overview",
                r"our\s+business"
            ],
            "risk_factors": [
                r"item\s+1a\s*[.\-–—]\s*risk\s+factors",
                r"item\s+1a\b.*?risk\s+factors", 
                r"risk\s+factors",
                r"risks?\s+related\s+to"
            ],
            "mda": [
                r"item\s+7\s*[.\-–—]\s*management[''']?s\s+discussion\s+and\s+analysis",
                r"item\s+7\b.*?management.*?discussion.*?analysis",
                r"management[''']?s\s+discussion\s+and\s+analysis",
                r"md&a"
            ],
            "financial_statements": [
                r"item\s+8\s*[.\-–—]\s*financial\s+statements",
                r"item\s+8\b.*?financial\s+statements",
                r"consolidated\s+financial\s+statements",
                r"financial\s+statements\s+and\s+supplementary\s+data"
            ]
        }
        
        # 각 섹션 추출
        for section_name, patterns in section_patterns.items():
            section_text = self._extract_section_text(text, patterns)
            if section_text:
                sections[section_name] = section_text[:50000]  # 섹션 크기 제한
        
        return sections
    
    def _extract_section_text(self, text: str, patterns: List[str]) -> str:
        """정규표현식 패턴을 사용하여 특정 섹션의 텍스트 추출."""
        text_lower = text.lower()
        
        for pattern in patterns:
            match = re.search(pattern, text_lower, re.IGNORECASE | re.MULTILINE)
            if match:
                start_pos = match.start()
                
                # 섹션의 끝 찾기 (다음 항목 또는 문서 끝)
                end_patterns = [
                    r"item\s+\d+[a-z]?\s*[.\-–—]",
                    r"part\s+\d+",
                    r"signatures",
                    r"exhibits"
                ]
                
                end_pos = len(text)
                for end_pattern in end_patterns:
                    end_match = re.search(end_pattern, text_lower[start_pos + 1000:], re.IGNORECASE)
                    if end_match:
                        end_pos = start_pos + 1000 + end_match.start()
                        break
                
                section_text = text[start_pos:end_pos].strip()
                if len(section_text) > 500:  # 충분한 컨텐트가 있는지 확인
                    return section_text
        
        return ""
    
    async def process_company_filings(self, company_data: Dict[str, Any], max_filings: int = 3) -> List[Filing]:
        """회사의 10-K 파일링 처리."""
        ticker = company_data["ticker"]
        cik = company_data["cik"]
        
        logger.info(f"{ticker} (CIK: {cik})의 파일링 처리 중")
        
        # 회사 제출 내역 가져오기
        submissions = await self.get_company_submissions(cik)
        if not submissions:
            logger.warning(f"{ticker}의 제출 내역을 찾을 수 없음")
            return []
        
        # 10-K 파일링 추출
        filing_data = self.extract_10k_filings(submissions, limit=max_filings)
        if not filing_data:
            logger.warning(f"{ticker}의 10-K 파일링을 찾을 수 없음")
            return []
        
        # 데이터베이스에 회사가 존재하는지 확인
        company = Company(
            ticker=ticker,
            cik=cik,
            company_name=company_data.get("title", ticker),
            exchange="NASDAQ"
        )
        await db_client.upsert_company(company)
        
        # 각 파일링 처리
        processed_filings = []
        for filing_info in filing_data:
            try:
                accession_number = filing_info["accessionNumber"]
                urls = self.build_filing_urls(cik, accession_number)
                
                # 파일링 레코드 생성
                filing = Filing(
                    company_id="",  # 데이터베이스에서 설정됨
                    ticker=ticker,
                    cik=cik,
                    accession_number=accession_number,
                    form_type="10-K",
                    filing_date=filing_info["filingDate"],
                    report_date=filing_info["reportDate"],
                    fiscal_year=filing_info["fiscalYear"],
                    edgar_url=urls["edgar_url"],
                    html_url=urls["html_url"],
                    xml_url=urls["xml_url"]
                )
                
                # 파일링이 이미 존재하는지 확인
                existing_filing = await db_client.get_filing_by_ticker_year(ticker, filing.fiscal_year)
                if existing_filing:
                    logger.info(f"{ticker} {filing.fiscal_year}의 파일링이 이미 존재")
                    continue
                
                # 데이터베이스에 파일링 삽입
                filing_record = await db_client.insert_filing(filing)
                filing.id = filing_record.get("id")
                
                processed_filings.append(filing)
                
                logger.info(f"파일링 처리 완료: {ticker} {filing.fiscal_year}")
                
                # 요청 속도 제한
                await asyncio.sleep(settings.request_delay)
                
            except Exception as e:
                logger.error(f"파일링 {filing_info.get('accessionNumber', 'unknown')} 처리 오류: {e}")
                continue
        
        logger.info(f"{ticker}의 {len(processed_filings)}개 파일링을 성공적으로 처리")
        return processed_filings
    
    async def download_and_parse_filing(self, filing: Filing) -> Dict[str, str]:
        """특정 파일링을 다운로드하고 파싱하여 섹션 추출."""
        logger.info(f"파일링 다운로드 및 파싱 중: {filing.ticker} {filing.fiscal_year}")
        
        try:
            # 상태를 진행 중으로 업데이트
            if filing.id:
                await db_client.update_filing_status(filing.id, "in_progress")
            
            # HTML 컨텐트 다운로드
            html_content = await self.get_filing_html_content(filing.cik, filing.accession_number)
            
            if not html_content:
                logger.warning(f"{filing.ticker} {filing.fiscal_year}에 대해 사용 가능한 HTML 컨텐트 없음")
                if filing.id:
                    await db_client.update_filing_status(filing.id, "failed")
                return {}
            
            # 섹션 추출
            sections = self.extract_document_sections(html_content)
            
            # 빈 섹션 필터링
            sections = {k: v for k, v in sections.items() if v and len(v.strip()) > 100}
            
            if sections:
                if filing.id:
                    await db_client.update_filing_status(filing.id, "completed")
                logger.info(f"{filing.ticker} {filing.fiscal_year}의 {len(sections)}개 섹션을 성공적으로 파싱")
            else:
                if filing.id:
                    await db_client.update_filing_status(filing.id, "failed")
                logger.warning(f"{filing.ticker} {filing.fiscal_year}에서 유효한 섹션을 추출하지 못함")
            
            return sections
            
        except Exception as e:
            logger.error(f"파일링 {filing.ticker} {filing.fiscal_year} 다운로드/파싱 오류: {e}")
            if filing.id:
                await db_client.update_filing_status(filing.id, "failed")
            return {}


async def main():
    """EDGAR 클라이언트 기능 테스트."""
    # 샘플 회사로 테스트
    test_company = {
        "ticker": "AAPL",
        "cik": "0000320193",
        "title": "Apple Inc."
    }
    
    async with EdgarClient() as client:
        filings = await client.process_company_filings(test_company, max_filings=2)
        
        if filings:
            # 첫 번째 파일링 다운로드 및 파싱 테스트
            sections = await client.download_and_parse_filing(filings[0])
            
            print(f"\n=== {filings[0].ticker} {filings[0].fiscal_year}의 추출된 섹션들 ===")
            for section_name, content in sections.items():
                print(f"\n{section_name.upper()}: {len(content)}자")
                print(f"미리보기: {content[:200]}...")


if __name__ == "__main__":
    asyncio.run(main())