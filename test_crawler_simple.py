#!/usr/bin/env python3
"""Simple crawler test script without database dependencies."""

import asyncio
import requests
import sys
import os
import ssl
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Set required environment variables for testing
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "test-key")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "test-service-key") 
os.environ.setdefault("OPENAI_API_KEY", "sk-test")
os.environ.setdefault("USER_AGENT", "ISU JO (ourwavelets@gmail.com)")

from config.settings import settings


class SimpleEdgarCrawler:
    """Simplified EDGAR crawler for testing."""
    
    def __init__(self):
        self.base_url = "https://data.sec.gov/submissions"
        self.headers = {
            "User-Agent": settings.user_agent,
            "Accept": "application/json"
        }
        
    def get_company_info(self, cik: str):
        """Get company submission information from SEC API."""
        url = f"{self.base_url}/CIK{cik.zfill(10)}.json"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Extract company name and recent filings
            company_name = data.get('name', 'Unknown')
            recent_filings = data.get('filings', {}).get('recent', {})
            
            # Filter for 10-K filings
            forms = recent_filings.get('form', [])
            accession_numbers = recent_filings.get('accessionNumber', [])
            filing_dates = recent_filings.get('filingDate', [])
            
            ten_k_filings = []
            for i, form in enumerate(forms):
                if form == '10-K' and i < len(accession_numbers) and i < len(filing_dates):
                    ten_k_filings.append({
                        'accessionNumber': accession_numbers[i],
                        'filingDate': filing_dates[i],
                        'form': form
                    })
            
            return {
                'cik': cik,
                'companyName': company_name,
                'tenKFilings': ten_k_filings[:3],  # Limit to 3 most recent
                'success': True
            }
            
        except Exception as e:
            return {
                'cik': cik,
                'error': str(e),
                'success': False
            }
    
    def test_crawl_companies(self, company_ciks):
        """Test crawling for a list of company CIKs."""
        print(f"🔄 Testing EDGAR crawling for {len(company_ciks)} companies...")
        print(f"📡 Using User-Agent: {settings.user_agent}")
        print("=" * 60)
        
        results = []
        
        for cik in company_ciks:
            print(f"\n📊 Fetching data for CIK: {cik}")
            result = self.get_company_info(cik)
            results.append(result)
            
            if result['success']:
                print(f"✅ Company: {result['companyName']}")
                print(f"   10-K Filings found: {len(result['tenKFilings'])}")
                
                for filing in result['tenKFilings']:
                    print(f"   - {filing['filingDate']}: {filing['accessionNumber']}")
            else:
                print(f"❌ Error: {result['error']}")
            
            # Add small delay to respect rate limits
            import time
            time.sleep(0.1)
        
        return results


def main():
    """Main test function."""
    print("🚀 EDGAR Crawler Test")
    print("=" * 60)
    
    # Test with a few well-known company CIKs
    test_companies = [
        "0000320193",  # Apple Inc.
        "0000789019",  # Microsoft Corp.
        "0001652044",  # Alphabet Inc.
        "0001018724",  # Amazon.com Inc.
        "0001318605",  # Tesla Inc.
    ]
    
    crawler = SimpleEdgarCrawler()
    results = crawler.test_crawl_companies(test_companies)
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 CRAWLING TEST SUMMARY")
    print("=" * 60)
    
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]
    
    print(f"✅ Successful: {len(successful)}")
    print(f"❌ Failed: {len(failed)}")
    print(f"📈 Success Rate: {len(successful)/len(results)*100:.1f}%")
    
    if failed:
        print(f"\n🔍 Failed CIKs:")
        for result in failed:
            print(f"   - {result['cik']}: {result['error']}")
    
    total_filings = sum(len(r.get('tenKFilings', [])) for r in successful)
    print(f"\n📋 Total 10-K filings found: {total_filings}")
    
    print(f"\n✨ Crawling test completed!")


if __name__ == "__main__":
    main()