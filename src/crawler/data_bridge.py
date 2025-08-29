"""추출 툴킷과 분석 파이프라인 간의 데이터 브릿지."""

import asyncio
import json
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Union
from pathlib import Path

from loguru import logger
from bs4 import BeautifulSoup

from config.settings import settings
from src.database.connection import db_client
from src.database.schema import Filing, Company, QualitativeSection
from src.crawler.enhanced_edgar_crawler import EnhancedEdgarCrawler
from src.crawler.advanced_extractor import AdvancedTextExtractor


class EdgarDataBridge:
    """EDGAR 크롤러와 분석 파이프라인 간의 브릿지."""
    
    def __init__(self):
        self.crawler = None
        self.extractor = AdvancedTextExtractor(remove_tables=True)
        self.data_dir = Path(settings.data_dir)
    
    async def __aenter__(self):
        """Async context manager entry."""
        self.crawler = await EnhancedEdgarCrawler().__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.crawler:
            await self.crawler.__aexit__(exc_type, exc_val, exc_tb)
    
    async def crawl_and_analyze_companies(
        self,
        company_list: List[str],
        max_filings_per_company: int = 3,
        items_to_extract: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """완전한 파이프라인: 파일링 크롤링 및 분석을 위한 섹션 추출."""
        logger.info(f"Starting integrated crawl and analysis for {len(company_list)} companies")
        
        if items_to_extract is None:
            items_to_extract = ["1", "1A", "3", "7", "7A", "8"]  # Key sections for analysis
        
        start_time = datetime.now()
        stats = {
            'companies_processed': 0,
            'filings_processed': 0,
            'sections_extracted': 0,
            'analysis_ready': 0,
            'errors': 0
        }
        
        # Process each company
        for ticker in company_list:
            try:
                logger.info(f"Processing company: {ticker}")
                
                # Get company data
                company_data = await self._get_company_data(ticker)
                if not company_data:
                    logger.warning(f"Could not get data for {ticker}")
                    stats['errors'] += 1
                    continue
                
                # Process filings for this company
                company_stats = await self._process_company_filings(
                    company_data, max_filings_per_company, items_to_extract
                )
                
                # Update overall stats
                stats['companies_processed'] += 1
                stats['filings_processed'] += company_stats['filings_processed']
                stats['sections_extracted'] += company_stats['sections_extracted']
                stats['analysis_ready'] += company_stats['analysis_ready']
                stats['errors'] += company_stats['errors']
                
                # Rate limiting
                await asyncio.sleep(settings.request_delay)
                
            except Exception as e:
                logger.error(f"Error processing company {ticker}: {e}")
                stats['errors'] += 1
                continue
        
        # Calculate final stats
        end_time = datetime.now()
        stats['duration'] = end_time - start_time
        stats['success_rate'] = stats['analysis_ready'] / max(stats['filings_processed'], 1)
        
        logger.info(f"Pipeline completed: {stats}")
        return stats
    
    async def _get_company_data(self, ticker: str) -> Optional[Dict[str, Any]]:
        """SEC API에서 회사 데이터 가져오기."""
        try:
            # First try to get from database
            existing_company = await db_client.get_company_by_ticker(ticker)
            if existing_company:
                return {
                    'ticker': ticker,
                    'cik': existing_company.get('cik', ''),
                    'title': existing_company.get('company_name', ticker),
                    'exchange': existing_company.get('exchange', 'NASDAQ')
                }
            
            # If not in database, fetch from SEC
            return await self._fetch_company_from_sec(ticker)
            
        except Exception as e:
            logger.error(f"Error getting company data for {ticker}: {e}")
            return None
    
    async def _fetch_company_from_sec(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetch company data from SEC company tickers API."""
        try:
            company_tickers_url = 'https://www.sec.gov/files/company_tickers.json'
            
            async with self.crawler.session.get(company_tickers_url) as response:
                response.raise_for_status()
                data = await response.json()
                
                # Find company by ticker
                for company_info in data.values():
                    if company_info['ticker'].upper() == ticker.upper():
                        return {
                            'ticker': ticker.upper(),
                            'cik': str(company_info['cik_str']).zfill(10),
                            'title': company_info['title'],
                            'exchange': 'NASDAQ'  # Default
                        }
            
            logger.warning(f"Company {ticker} not found in SEC database")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching company data from SEC for {ticker}: {e}")
            return None
    
    async def _process_company_filings(
        self,
        company_data: Dict[str, Any],
        max_filings: int,
        items_to_extract: List[str]
    ) -> Dict[str, int]:
        """회사의 모든 파일링 처리."""
        stats = {
            'filings_processed': 0,
            'sections_extracted': 0,
            'analysis_ready': 0,
            'errors': 0
        }
        
        try:
            # Get filings list from crawler
            filings = await self.crawler.process_company_filings(company_data, max_filings)
            
            if not filings:
                logger.warning(f"No filings found for {company_data['ticker']}")
                return stats
            
            # Process each filing
            for filing in filings:
                try:
                    stats['filings_processed'] += 1
                    
                    # Download and extract sections
                    sections = await self._extract_filing_sections(filing, items_to_extract)
                    
                    if sections:
                        # Save sections to database
                        await self._save_filing_sections(filing, sections)
                        
                        stats['sections_extracted'] += len(sections)
                        stats['analysis_ready'] += 1
                        
                        logger.info(f"Processed {filing.ticker} {filing.fiscal_year}: {len(sections)} sections")
                    else:
                        logger.warning(f"No sections extracted for {filing.ticker} {filing.fiscal_year}")
                        stats['errors'] += 1
                
                except Exception as e:
                    logger.error(f"Error processing filing {filing.ticker} {filing.fiscal_year}: {e}")
                    stats['errors'] += 1
                    continue
        
        except Exception as e:
            logger.error(f"Error processing filings for {company_data['ticker']}: {e}")
            stats['errors'] += 1
        
        return stats
    
    async def _extract_filing_sections(
        self,
        filing: Filing,
        items_to_extract: List[str]
    ) -> Dict[str, str]:
        """파일링에서 섹션 추출."""
        try:
            # Download filing content
            html_content = await self.crawler.get_filing_html_content(filing.cik, filing.accession_number)
            
            if not html_content:
                logger.warning(f"Could not download content for {filing.ticker} {filing.fiscal_year}")
                return {}
            
            # Extract sections using advanced extractor
            sections = await self.extractor.extract_filing_sections_async(
                html_content, items_to_extract
            )
            
            # Filter out empty sections
            valid_sections = {k: v for k, v in sections.items() if v and len(v.strip()) > 100}
            
            return valid_sections
            
        except Exception as e:
            logger.error(f"Error extracting sections from {filing.ticker} {filing.fiscal_year}: {e}")
            return {}
    
    async def _save_filing_sections(self, filing: Filing, sections: Dict[str, str]) -> None:
        """추출된 섹션을 데이터베이스에 저장."""
        try:
            for section_name, content in sections.items():
                # Create filing section record
                filing_section = QualitativeSection(
                    filing_id=filing.id,
                    section_name=section_name,
                    content=content,
                    word_count=len(content.split()),
                    extracted_at=datetime.utcnow()
                )
                
                # Save to database
                await db_client.insert_filing_section(filing_section)
            
            # Update filing status
            if filing.id:
                await db_client.update_filing_status(filing.id, "sections_extracted")
            
            logger.debug(f"Saved {len(sections)} sections for {filing.ticker} {filing.fiscal_year}")
            
        except Exception as e:
            logger.error(f"Error saving sections for {filing.ticker} {filing.fiscal_year}: {e}")
            raise
    
    async def migrate_legacy_data(self, legacy_data_dir: Path) -> Dict[str, Any]:
        """레거시 추출 툴킷 형식에서 데이터 마이그레이션."""
        logger.info(f"Migrating legacy data from {legacy_data_dir}")
        
        stats = {'migrated_filings': 0, 'errors': 0}
        
        try:
            # Find all JSON files from legacy extraction
            extracted_files = list((legacy_data_dir / "EXTRACTED_FILINGS").glob("*.json"))
            
            for json_file in extracted_files:
                try:
                    # Load legacy JSON data
                    with open(json_file, 'r', encoding='utf-8') as f:
                        legacy_data = json.load(f)
                    
                    # Convert to new format
                    await self._migrate_single_filing(legacy_data)
                    stats['migrated_filings'] += 1
                    
                except Exception as e:
                    logger.error(f"Error migrating {json_file}: {e}")
                    stats['errors'] += 1
                    continue
            
            logger.info(f"Migration completed: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error during migration: {e}")
            return stats
    
    async def _migrate_single_filing(self, legacy_data: Dict[str, Any]) -> None:
        """Migrate a single filing from legacy format."""
        try:
            # Extract metadata
            ticker = self._extract_ticker_from_filename(legacy_data.get('filename', ''))
            
            # Create company record
            company = Company(
                ticker=ticker,
                cik=legacy_data.get('cik', '').zfill(10),
                company_name=legacy_data.get('company', ticker),
                exchange='NASDAQ'
            )
            await db_client.upsert_company(company)
            
            # Parse dates
            filing_date = self._parse_legacy_date(legacy_data.get('filing_date'))
            report_date = self._parse_legacy_date(legacy_data.get('period_of_report'))
            
            # Create filing record
            filing = Filing(
                company_id="",  # Will be set by database
                ticker=ticker,
                cik=legacy_data.get('cik', '').zfill(10),
                accession_number=self._extract_accession_from_filename(legacy_data.get('filename', '')),
                form_type=legacy_data.get('filing_type', '10-K'),
                filing_date=filing_date,
                report_date=report_date,
                fiscal_year=report_date.year if report_date else None,
                edgar_url=legacy_data.get('filing_html_index', ''),
                html_url=legacy_data.get('htm_filing_link', ''),
                xml_url=""
            )
            
            filing_record = await db_client.insert_filing(filing)
            filing.id = filing_record.get('id')
            
            # Migrate sections
            section_mapping = {
                'item_1': 'business',
                'item_1A': 'risk_factors',
                'item_3': 'legal_proceedings',
                'item_7': 'mda',
                'item_7A': 'quantitative_qualitative_disclosures',
                'item_8': 'financial_statements'
            }
            
            for legacy_key, section_name in section_mapping.items():
                content = legacy_data.get(legacy_key, '')
                if content and len(content.strip()) > 100:
                    filing_section = QualitativeSection(
                        filing_id=filing.id,
                        section_name=section_name,
                        content=content,
                        word_count=len(content.split()),
                        extracted_at=datetime.utcnow()
                    )
                    await db_client.insert_filing_section(filing_section)
            
            logger.debug(f"Migrated filing: {ticker} {filing.fiscal_year}")
            
        except Exception as e:
            logger.error(f"Error migrating filing: {e}")
            raise
    
    def _extract_ticker_from_filename(self, filename: str) -> str:
        """Extract ticker from legacy filename format."""
        try:
            # Format: CIK_FORMTYPE_YEAR_ACCESSION.ext
            parts = filename.split('_')
            if len(parts) >= 2:
                # Try to identify ticker part
                for part in parts[1:]:
                    if part.isalpha() and 1 <= len(part) <= 5:
                        return part.upper()
            return "UNKNOWN"
        except Exception:
            return "UNKNOWN"
    
    def _extract_accession_from_filename(self, filename: str) -> str:
        """Extract accession number from legacy filename."""
        try:
            # Format: CIK_FORMTYPE_YEAR_ACCESSION.ext
            parts = filename.split('_')
            if len(parts) >= 4:
                return parts[-1].split('.')[0]  # Remove extension
            return ""
        except Exception:
            return ""
    
    def _parse_legacy_date(self, date_str: Optional[str]) -> Optional[date]:
        """Parse date from legacy format."""
        if not date_str:
            return None
        
        try:
            # Try different date formats
            formats = ['%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y']
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue
            
            logger.warning(f"Could not parse date: {date_str}")
            return None
            
        except Exception:
            return None


# Convenience functions
async def run_integrated_pipeline(
    companies: List[str],
    max_filings_per_company: int = 3,
    items_to_extract: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Run the complete integrated pipeline."""
    async with EdgarDataBridge() as bridge:
        return await bridge.crawl_and_analyze_companies(
            companies, max_filings_per_company, items_to_extract
        )


async def migrate_legacy_extraction_data(legacy_data_path: str) -> Dict[str, Any]:
    """Migrate data from legacy extraction toolkit."""
    async with EdgarDataBridge() as bridge:
        return await bridge.migrate_legacy_data(Path(legacy_data_path))


if __name__ == "__main__":
    # Test the data bridge
    test_companies = ["AAPL", "MSFT", "GOOGL"]
    stats = asyncio.run(run_integrated_pipeline(test_companies, max_filings_per_company=1))
    print(f"Integration test results: {stats}")