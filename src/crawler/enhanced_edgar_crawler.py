"""비동기 지원 및 통합 기능을 갖춘 향상된 EDGAR 크롤러."""

import asyncio
import aiohttp
import itertools
import json
import math
import os
import pandas as pd
import re
import requests
import tempfile
import zipfile
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path

from bs4 import BeautifulSoup
from loguru import logger
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, HTTPError, ConnectionError, Timeout, RetryError
from tqdm import tqdm
from urllib3.util import Retry

# Import from the integrated system
from config.settings import settings
from src.database.connection import db_client
from src.database.schema import Filing, Company


class EnhancedEdgarCrawler:
    """비동기 지원 및 데이터베이스 통합을 갖춘 향상된 EDGAR 크롤러."""
    
    def __init__(self):
        self.base_url = "https://www.sec.gov/Archives/edgar/full-index/"
        self.headers = {'User-agent': settings.user_agent}
        self.dataset_dir = Path(settings.data_dir)
        self.session: Optional[aiohttp.ClientSession] = None
        
        # 디렉토리 존재 확인
        self.dataset_dir.mkdir(exist_ok=True)
        (self.dataset_dir / "RAW_FILINGS").mkdir(exist_ok=True)
        (self.dataset_dir / "INDICES").mkdir(exist_ok=True)
    
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
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    async def download_indices_async(
        self,
        start_year: int,
        end_year: int,
        quarters: List[int],
        skip_present_indices: bool = True
    ) -> List[str]:
        """Download EDGAR Index files asynchronously."""
        logger.info('Downloading EDGAR Index files')
        
        # Validate quarters
        for quarter in quarters:
            if quarter not in [1, 2, 3, 4]:
                raise ValueError(f'Invalid quarter "{quarter}"')
        
        indices_folder = self.dataset_dir / "INDICES"
        downloaded_files = []
        
        tasks = []
        for year in range(start_year, end_year + 1):
            for quarter in quarters:
                # Skip future quarters
                if year == datetime.now().year and quarter > math.ceil(datetime.now().month / 3):
                    continue
                
                index_filename = f'{year}_QTR{quarter}.tsv'
                filepath = indices_folder / index_filename
                
                # Skip if file exists and skip_present_indices is True
                if skip_present_indices and filepath.exists():
                    logger.info(f'Skipping {index_filename}')
                    downloaded_files.append(str(filepath))
                    continue
                
                # Create download task
                url = f'{self.base_url}/{year}/QTR{quarter}/master.zip'
                task = self._download_single_index(url, filepath)
                tasks.append(task)
        
        # Execute downloads concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Failed to download index: {result}")
            else:
                downloaded_files.append(result)
        
        return [f for f in downloaded_files if f]
    
    async def _download_single_index(self, url: str, filepath: Path) -> str:
        """Download a single index file."""
        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                content = await response.read()
                
                # Process the zip file
                with tempfile.TemporaryFile() as tmp:
                    tmp.write(content)
                    tmp.seek(0)
                    
                    with zipfile.ZipFile(tmp) as z:
                        with z.open("master.idx") as f:
                            lines = [line.decode('latin-1') for line in itertools.islice(f, 11, None)]
                            lines = [line.strip() + '|' + line.split('|')[-1].replace('.txt', '-index.html') for line in lines]
                
                # Save the processed index file
                with open(filepath, 'w+', encoding='utf-8') as f:
                    f.write(''.join(lines))
                    
                logger.info(f'{filepath.name} downloaded')
                return str(filepath)
                
        except Exception as e:
            logger.error(f'Failed downloading "{filepath.name}" - {e}')
            raise
    
    def get_specific_indices(
        self,
        tsv_filenames: List[str],
        filing_types: List[str],
        cik_tickers: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Extract specific indices from downloaded TSV files."""
        logger.info("Processing indices for specific filing types")
        
        # Get CIKs from tickers if provided
        ciks = []
        if cik_tickers:
            ciks = self._convert_tickers_to_ciks(cik_tickers)
        
        # Process each TSV file
        dfs_list = []
        for filepath in tsv_filenames:
            try:
                df = pd.read_csv(
                    filepath,
                    sep='|',
                    header=None,
                    dtype=str,
                    names=[
                        'CIK', 'Company', 'Type', 'Date', 'complete_text_file_link', 'html_index',
                        'Filing Date', 'Period of Report', 'SIC', 'htm_file_link',
                        'State of Inc', 'State location', 'Fiscal Year End', 'filename'
                    ]
                )
                
                # Add SEC Archives URL prefix
                df['complete_text_file_link'] = 'https://www.sec.gov/Archives/' + df['complete_text_file_link'].astype(str)
                df['html_index'] = 'https://www.sec.gov/Archives/' + df['html_index'].astype(str)
                
                # Filter by filing type
                df = df[df.Type.isin(filing_types)]
                
                # Filter by CIKs if provided
                if ciks:
                    df = df[df.CIK.isin(ciks)]
                
                dfs_list.append(df)
                
            except Exception as e:
                logger.error(f"Error processing {filepath}: {e}")
                continue
        
        # Combine all dataframes
        if dfs_list:
            result_df = pd.concat(dfs_list, ignore_index=True) if len(dfs_list) > 1 else dfs_list[0]
            logger.info(f"Found {len(result_df)} matching filings")
            return result_df
        else:
            logger.warning("No valid indices found")
            return pd.DataFrame()
    
    def _convert_tickers_to_ciks(self, cik_tickers: List[str]) -> List[str]:
        """Convert tickers to CIKs using SEC company tickers API."""
        logger.info("Converting tickers to CIKs")
        
        company_tickers_url = 'https://www.sec.gov/files/company_tickers.json'
        
        try:
            response = requests.get(company_tickers_url, headers=self.headers, timeout=30)
            response.raise_for_status()
            company_tickers = response.json()
            
            # Create ticker to CIK mapping
            ticker2cik = {company['ticker']: str(company['cik_str']).zfill(10) 
                         for company in company_tickers.values()}
            
            ciks = []
            for c_t in cik_tickers:
                if isinstance(c_t, int) or c_t.isdigit():
                    # Already a CIK
                    ciks.append(str(c_t).zfill(10))
                else:
                    # Ticker - convert to CIK
                    if c_t.upper() in ticker2cik:
                        ciks.append(ticker2cik[c_t.upper()])
                    else:
                        logger.warning(f'Could not find CIK for ticker "{c_t}"')
            
            logger.info(f"Converted {len(ciks)} tickers/CIKs")
            return ciks
            
        except Exception as e:
            logger.error(f"Error converting tickers to CIKs: {e}")
            return []
    
    async def crawl_and_download_filing(self, filing_info: pd.Series) -> Optional[Dict[str, Any]]:
        """Crawl and download a specific filing."""
        try:
            html_index = filing_info['html_index']
            
            # Get filing details from HTML index
            async with self.session.get(html_index) as response:
                if response.status == 404:
                    logger.warning(f"Filing not found: {html_index}")
                    return None
                    
                response.raise_for_status()
                content = await response.text()
            
            # Parse HTML to extract filing details
            soup = BeautifulSoup(content, 'html.parser')
            
            # Extract filing metadata
            filing_metadata = self._extract_filing_metadata(soup, filing_info)
            
            # Find and download the main document
            document_url = self._find_main_document_url(soup, filing_info)
            if document_url:
                document_content = await self._download_document_content(document_url)
                if document_content:
                    # Save to database-compatible format
                    filing_record = await self._save_filing_to_database(filing_metadata, document_content)
                    return filing_record
            
            return None
            
        except Exception as e:
            logger.error(f"Error crawling filing {filing_info.get('html_index', 'unknown')}: {e}")
            return None
    
    def _extract_filing_metadata(self, soup: BeautifulSoup, filing_info: pd.Series) -> Dict[str, Any]:
        """Extract metadata from filing HTML."""
        metadata = {
            'ticker': None,
            'cik': filing_info.get('CIK', '').zfill(10),
            'company_name': filing_info.get('Company', ''),
            'form_type': filing_info.get('Type', ''),
            'filing_date': filing_info.get('Date', ''),
            'html_index': filing_info.get('html_index', ''),
        }
        
        # Extract additional metadata from HTML
        try:
            # Look for filing date and period of report
            info_elements = soup.find_all('div', {'class': ['infoHead', 'info']})
            for element in info_elements:
                if element.get('class') == ['infoHead']:
                    if 'Filing Date' in element.text:
                        next_sibling = element.find_next_sibling()
                        if next_sibling:
                            metadata['filing_date'] = next_sibling.text.strip()
                    elif 'Period of Report' in element.text:
                        next_sibling = element.find_next_sibling()
                        if next_sibling:
                            metadata['period_of_report'] = next_sibling.text.strip()
        except Exception as e:
            logger.debug(f"Error extracting metadata: {e}")
        
        return metadata
    
    def _find_main_document_url(self, soup: BeautifulSoup, filing_info: pd.Series) -> Optional[str]:
        """Find the main document URL from filing index."""
        try:
            # Look for document format files table
            for table in soup.find_all('table'):
                if table.get('summary') == 'Document Format Files':
                    for row in table.find_all('tr')[1:]:  # Skip header
                        cells = row.find_all('td')
                        if len(cells) >= 4:
                            form_type = cells[3].text.strip()
                            if form_type == filing_info.get('Type', ''):
                                link_cell = cells[2]
                                link = link_cell.find('a')
                                if link and link.get('href'):
                                    href = link['href']
                                    if href.startswith('/'):
                                        return f"https://www.sec.gov{href}"
                                    return href
        except Exception as e:
            logger.debug(f"Error finding document URL: {e}")
        
        return None
    
    async def _download_document_content(self, url: str) -> Optional[str]:
        """Download document content from URL."""
        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                content = await response.text()
                return content
        except Exception as e:
            logger.error(f"Error downloading document from {url}: {e}")
            return None
    
    async def _save_filing_to_database(self, metadata: Dict[str, Any], content: str) -> Dict[str, Any]:
        """Save filing to database in standardized format."""
        try:
            # Create company record if not exists
            company = Company(
                ticker=metadata.get('ticker', ''),
                cik=metadata['cik'],
                company_name=metadata['company_name'],
                exchange='NASDAQ'  # Default
            )
            await db_client.upsert_company(company)
            
            # Parse filing date
            filing_date = None
            if metadata.get('filing_date'):
                try:
                    filing_date = datetime.strptime(metadata['filing_date'], '%Y-%m-%d').date()
                except ValueError:
                    try:
                        filing_date = datetime.strptime(metadata['filing_date'], '%m/%d/%Y').date()
                    except ValueError:
                        logger.warning(f"Could not parse filing date: {metadata['filing_date']}")
            
            # Create filing record
            filing = Filing(
                company_id="",  # Will be set by database
                ticker=metadata.get('ticker', ''),
                cik=metadata['cik'],
                accession_number=self._extract_accession_number(metadata['html_index']),
                form_type=metadata['form_type'],
                filing_date=filing_date,
                report_date=filing_date,  # Use same date if period not available
                fiscal_year=filing_date.year if filing_date else None,
                edgar_url=metadata['html_index'],
                html_url=metadata['html_index'],
                xml_url=""
            )
            
            # Insert into database
            filing_record = await db_client.insert_filing(filing)
            
            # Save content sections (basic extraction)
            sections = self._extract_basic_sections(content)
            
            return {
                'filing_id': filing_record.get('id'),
                'metadata': metadata,
                'sections': sections,
                'content_length': len(content)
            }
            
        except Exception as e:
            logger.error(f"Error saving filing to database: {e}")
            return {}
    
    def _extract_accession_number(self, html_index: str) -> str:
        """Extract accession number from HTML index URL."""
        try:
            # URL format: .../data/CIK/ACCESSION_NUMBER/ACCESSION_NUMBER-index.htm
            parts = html_index.split('/')
            for part in parts:
                if '-index.htm' in part:
                    return part.replace('-index.htm', '')
            return ""
        except Exception:
            return ""
    
    def _extract_basic_sections(self, content: str) -> Dict[str, str]:
        """Extract basic sections from filing content."""
        # This is a simplified version - the full extraction logic
        # from extract_items.py would be integrated here
        sections = {
            'business': '',
            'risk_factors': '',
            'mda': '',
            'financial_statements': ''
        }
        
        # Basic text extraction
        soup = BeautifulSoup(content, 'html.parser')
        text = soup.get_text()
        
        # Simple pattern matching (would be enhanced with full extraction logic)
        patterns = {
            'business': r'item\s+1\s*[.\-–—]\s*business',
            'risk_factors': r'item\s+1a\s*[.\-–—]\s*risk\s+factors',
            'mda': r'item\s+7\s*[.\-–—]\s*management.*?discussion.*?analysis',
        }
        
        for section, pattern in patterns.items():
            match = re.search(pattern, text.lower())
            if match:
                # Extract section (simplified - would need full logic)
                start = match.start()
                end = start + 10000  # Limit section size
                sections[section] = text[start:end]
        
        return sections
    
    async def run_crawling_pipeline(
        self,
        start_year: int = 2022,
        end_year: int = 2023,
        quarters: List[int] = [1, 2, 3, 4],
        filing_types: List[str] = ["10-K"],
        cik_tickers: Optional[List[str]] = None,
        max_filings: int = 100
    ) -> Dict[str, Any]:
        """Run the complete crawling pipeline."""
        logger.info("Starting enhanced EDGAR crawling pipeline")
        
        start_time = datetime.now()
        stats = {
            'start_time': start_time,
            'indices_downloaded': 0,
            'filings_processed': 0,
            'filings_saved': 0,
            'errors': 0
        }
        
        try:
            # Download indices
            logger.info("Downloading EDGAR indices...")
            index_files = await self.download_indices_async(start_year, end_year, quarters)
            stats['indices_downloaded'] = len(index_files)
            
            if not index_files:
                logger.error("No index files downloaded")
                return stats
            
            # Get specific indices
            logger.info("Processing indices for specific filings...")
            df = self.get_specific_indices(index_files, filing_types, cik_tickers)
            
            if df.empty:
                logger.warning("No matching filings found")
                return stats
            
            # Limit number of filings
            if len(df) > max_filings:
                df = df.head(max_filings)
                logger.info(f"Limited to {max_filings} filings")
            
            # Process filings
            logger.info(f"Processing {len(df)} filings...")
            tasks = []
            for _, filing_info in df.iterrows():
                task = self.crawl_and_download_filing(filing_info)
                tasks.append(task)
                stats['filings_processed'] += 1
                
                # Process in batches to avoid overwhelming the system
                if len(tasks) >= 10:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for result in results:
                        if isinstance(result, Exception):
                            stats['errors'] += 1
                            logger.error(f"Filing processing error: {result}")
                        elif result:
                            stats['filings_saved'] += 1
                    tasks = []
                    
                    # Rate limiting
                    await asyncio.sleep(1)
            
            # Process remaining tasks
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception):
                        stats['errors'] += 1
                    elif result:
                        stats['filings_saved'] += 1
        
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            stats['errors'] += 1
        
        # Calculate final stats
        end_time = datetime.now()
        stats['end_time'] = end_time
        stats['duration'] = end_time - start_time
        stats['success_rate'] = stats['filings_saved'] / max(stats['filings_processed'], 1)
        
        logger.info(f"Pipeline completed: {stats['filings_saved']}/{stats['filings_processed']} filings processed successfully")
        return stats


# Convenience functions for backward compatibility
async def run_edgar_crawler(
    start_year: int = 2022,
    end_year: int = 2023,
    quarters: List[int] = [1, 2, 3, 4],
    filing_types: List[str] = ["10-K"],
    cik_tickers: Optional[List[str]] = None,
    max_filings: int = 100
) -> Dict[str, Any]:
    """Run the enhanced EDGAR crawler."""
    async with EnhancedEdgarCrawler() as crawler:
        return await crawler.run_crawling_pipeline(
            start_year=start_year,
            end_year=end_year,
            quarters=quarters,
            filing_types=filing_types,
            cik_tickers=cik_tickers,
            max_filings=max_filings
        )


if __name__ == "__main__":
    # Test the enhanced crawler
    asyncio.run(run_edgar_crawler(
        start_year=2023,
        end_year=2023,
        quarters=[4],
        cik_tickers=["AAPL", "MSFT"],
        max_filings=5
    ))