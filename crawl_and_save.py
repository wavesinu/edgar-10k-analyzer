#!/usr/bin/env python3
"""실제 EDGAR 데이터 크롤링 및 파일 저장 스크립트"""

import requests
import json
import os
import sys
from pathlib import Path
from datetime import datetime

# src 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent / "src"))

# 필수 환경 변수 설정
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "test-key")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "test-service-key") 
os.environ.setdefault("OPENAI_API_KEY", "sk-test")
os.environ.setdefault("USER_AGENT", "ISU JO (ourwavelets@gmail.com)")

from config.settings import settings


class EdgarDataCrawler:
    """EDGAR 데이터 크롤링 및 로컬 파일 저장 클래스"""
    
    def __init__(self):
        self.base_url = "https://data.sec.gov/submissions"
        self.filing_base_url = "https://www.sec.gov/Archives/edgar/data"
        self.headers = {
            "User-Agent": settings.user_agent,
            "Accept": "application/json"
        }
        
        # 데이터 저장용 디렉토리 생성
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        
        # 회사 정보 및 파일링별 서브디렉토리 생성
        self.companies_dir = self.data_dir / "companies"
        self.companies_dir.mkdir(exist_ok=True)
        
        self.filings_dir = self.data_dir / "filings"
        self.filings_dir.mkdir(exist_ok=True)
        
    def save_company_info(self, company_data):
        """회사 기본 정보를 JSON 파일로 저장"""
        company_file = self.companies_dir / f"{company_data['cik']}_info.json"
        
        with open(company_file, 'w', encoding='utf-8') as f:
            json.dump(company_data, f, indent=2, ensure_ascii=False)
            
        print(f"💾 Company info saved: {company_file}")
        
    def download_filing_content(self, cik, accession_number):
        """SEC EDGAR에서 실제 10-K 파일링 내용 다운로드"""
        # CIK 번호에서 앞의 0 제거하여 URL 생성
        clean_cik = str(int(cik))
        
        # SEC EDGAR 파일링 다운로드 URL 생성
        filing_url = f"{self.filing_base_url}/{clean_cik}/{accession_number.replace('-', '')}/{accession_number}.txt"
        
        try:
            print(f"📥 Downloading: {filing_url}")
            response = requests.get(filing_url, headers=self.headers, timeout=60)
            response.raise_for_status()
            
            # 다운로드된 파일링 내용을 로컬 파일로 저장
            filing_file = self.filings_dir / f"{cik}_{accession_number}.txt"
            
            with open(filing_file, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            print(f"💾 Filing saved: {filing_file}")
            print(f"📊 File size: {len(response.text):,} characters")
            
            return {
                'success': True,
                'file_path': str(filing_file),
                'content_length': len(response.text),
                'url': filing_url
            }
            
        except Exception as e:
            print(f"❌ Download failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'url': filing_url
            }
    
    def crawl_company(self, cik, company_name=None):
        """특정 회사의 전체 데이터 크롤링 및 저장 처리"""
        print(f"\n🏢 Crawling company: {cik}")
        
        # 1단계: SEC API에서 회사 기본 정보 수집
        url = f"{self.base_url}/CIK{cik.zfill(10)}.json"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            company_info = {
                'cik': cik,
                'name': data.get('name', company_name or 'Unknown'),
                'sic': data.get('sic'),
                'sicDescription': data.get('sicDescription'),
                'crawled_at': datetime.now().isoformat(),
                'filings': []
            }
            
            # 2단계: 최근 10-K 파일링 목록에서 필요한 파일링 찾기
            recent_filings = data.get('filings', {}).get('recent', {})
            forms = recent_filings.get('form', [])
            accession_numbers = recent_filings.get('accessionNumber', [])
            filing_dates = recent_filings.get('filingDate', [])
            
            ten_k_count = 0
            for i, form in enumerate(forms):
                if form == '10-K' and ten_k_count < 2:  # 최신 10-K 파일링 2개만 처리
                    if i < len(accession_numbers) and i < len(filing_dates):
                        accession = accession_numbers[i]
                        filing_date = filing_dates[i]
                        
                        # 3단계: 해당 파일링의 실제 내용 다운로드
                        download_result = self.download_filing_content(cik, accession)
                        
                        filing_info = {
                            'accessionNumber': accession,
                            'filingDate': filing_date,
                            'form': form,
                            'download_result': download_result
                        }
                        
                        company_info['filings'].append(filing_info)
                        ten_k_count += 1
            
            # 4단계: 수집된 모든 정보를 JSON 파일로 저장
            self.save_company_info(company_info)
            
            return company_info
            
        except Exception as e:
            print(f"❌ Company crawling failed: {e}")
            return None


def main():
    """메인 크롤링 프로세스 실행 함수"""
    print("🚀 EDGAR Data Crawler & Saver")
    print("=" * 60)
    
    # 크롤링 대상 회사들 정의 (CIK 번호, 회사명)
    companies = [
        ("0000320193", "Apple Inc."),
        ("0000789019", "Microsoft Corp."),
        ("0001652044", "Alphabet Inc."),
    ]
    
    crawler = EdgarDataCrawler()
    results = []
    
    for cik, name in companies:
        result = crawler.crawl_company(cik, name)
        results.append(result)
        
        # API 요청 제한을 위한 대기 시간
        import time
        time.sleep(1)
    
    # 크롤링 결과 요약 및 통계 출력
    print("\n" + "=" * 60)
    print("📊 CRAWLING SUMMARY")
    print("=" * 60)
    
    successful = [r for r in results if r is not None]
    total_filings = sum(len(r.get('filings', [])) for r in successful)
    
    print(f"✅ Companies crawled: {len(successful)}")
    print(f"📄 Total filings downloaded: {total_filings}")
    print(f"📁 Data saved in: {crawler.data_dir.absolute()}")
    
    # 실제 다운로드된 파일 목록 및 크기 정보
    print(f"\n📋 Downloaded files:")
    for txt_file in crawler.filings_dir.glob("*.txt"):
        size = txt_file.stat().st_size
        print(f"   📄 {txt_file.name} ({size:,} bytes)")
    
    print(f"\n✨ Crawling completed!")


if __name__ == "__main__":
    main()