#!/usr/bin/env python3
"""Test fetching actual 10-K filing content."""

import requests
import sys
import os
from pathlib import Path
from bs4 import BeautifulSoup
import re

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Set required environment variables for testing
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "test-key")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "test-service-key") 
os.environ.setdefault("OPENAI_API_KEY", "sk-test")
os.environ.setdefault("USER_AGENT", "ISU JO (ourwavelets@gmail.com)")

from config.settings import settings


def fetch_filing_content(accession_number, cik):
    """Fetch the actual 10-K filing content."""
    # Construct the filing URL
    accession_clean = accession_number.replace('-', '')
    base_url = "https://www.sec.gov/Archives/edgar/data"
    filing_url = f"{base_url}/{int(cik)}/{accession_clean}/{accession_number}-index.htm"
    
    headers = {
        "User-Agent": settings.user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    }
    
    try:
        # Get the index page first
        print(f"📄 Fetching index: {filing_url}")
        response = requests.get(filing_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Parse the index to find the main 10-K document
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for 10-K document link in the table
        main_doc_link = None
        tables = soup.find_all('table')
        
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 4:  # Standard SEC filing table has multiple columns
                    # Check if this is a 10-K document
                    for i, cell in enumerate(cells):
                        cell_text = cell.get_text().strip().lower()
                        if '10-k' in cell_text and 'exhibit' not in cell_text:
                            # Look for the link in this row
                            for link in row.find_all('a', href=True):
                                href = link['href']
                                if href.endswith('.htm') or href.endswith('.html'):
                                    main_doc_link = href
                                    break
                            if main_doc_link:
                                break
                if main_doc_link:
                    break
            if main_doc_link:
                break
        
        if not main_doc_link:
            # Fallback: try common naming patterns
            possible_names = [
                f"{accession_number}.htm",
                f"{accession_number}.txt", 
                f"aapl-{accession_number.split('-')[-1]}.htm",
                f"aapl{accession_number.split('-')[-1]}.htm"
            ]
            main_doc_link = possible_names[0]
        
        # Clean up the URL construction
        if main_doc_link.startswith('http'):
            doc_url = main_doc_link
        elif main_doc_link.startswith('/'):
            doc_url = f"https://www.sec.gov{main_doc_link}"
        else:
            # Handle iXBRL format - convert to direct document access
            if 'ix?doc=' in main_doc_link:
                # Extract the actual document path from iXBRL viewer
                doc_path = main_doc_link.split('ix?doc=')[-1]
                doc_url = f"https://www.sec.gov{doc_path}"
            else:
                doc_url = f"{base_url}/{int(cik)}/{accession_clean}/{main_doc_link}"
        
        print(f"📋 Fetching document: {doc_url}")
        
        # Get the actual document
        doc_response = requests.get(doc_url, headers=headers, timeout=30)
        doc_response.raise_for_status()
        
        content = doc_response.text
        
        # Extract some key sections for verification
        sections_found = {}
        
        # Common 10-K section patterns
        section_patterns = {
            'Item 1': r'(?i)item\s*1[^a-zA-Z]*business',
            'Item 1A': r'(?i)item\s*1a[^a-zA-Z]*risk\s*factors',
            'Item 7': r'(?i)item\s*7[^a-zA-Z]*management.?s\s*discussion',
            'Item 7A': r'(?i)item\s*7a[^a-zA-Z]*quantitative\s*and\s*qualitative',
        }
        
        for section_name, pattern in section_patterns.items():
            match = re.search(pattern, content)
            if match:
                # Extract a sample of text after the match
                start_pos = match.end()
                sample_text = content[start_pos:start_pos+500].strip()
                # Clean up whitespace
                sample_text = re.sub(r'\s+', ' ', sample_text)
                sections_found[section_name] = sample_text[:200] + "..." if len(sample_text) > 200 else sample_text
        
        return {
            'success': True,
            'content_length': len(content),
            'sections_found': sections_found,
            'doc_url': doc_url
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'filing_url': filing_url
        }


def main():
    """Test filing content extraction."""
    print("🚀 EDGAR Filing Content Test")
    print("=" * 60)
    
    # Test with Apple's latest 10-K
    test_filing = {
        'cik': '0000320193',
        'accession_number': '0000320193-24-000123',
        'company': 'Apple Inc.'
    }
    
    # Also try direct text file access
    print(f"📊 Testing content extraction for {test_filing['company']}")
    print(f"   CIK: {test_filing['cik']}")
    print(f"   Accession: {test_filing['accession_number']}")
    
    # Test direct text file access first
    print(f"\n🔍 Trying direct text file access...")
    accession_clean = test_filing['accession_number'].replace('-', '')
    text_url = f"https://www.sec.gov/Archives/edgar/data/{int(test_filing['cik'])}/{accession_clean}/{test_filing['accession_number']}.txt"
    
    headers = {
        "User-Agent": settings.user_agent,
        "Accept": "text/plain,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    }
    
    try:
        print(f"📄 Fetching text file: {text_url}")
        response = requests.get(text_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        content = response.text
        
        # Extract some key sections for verification
        sections_found = {}
        
        # Common 10-K section patterns
        section_patterns = {
            'Item 1': r'(?i)item\s*1[^a-zA-Z]*business',
            'Item 1A': r'(?i)item\s*1a[^a-zA-Z]*risk\s*factors',
            'Item 7': r'(?i)item\s*7[^a-zA-Z]*management.?s\s*discussion',
            'Item 7A': r'(?i)item\s*7a[^a-zA-Z]*quantitative\s*and\s*qualitative',
        }
        
        for section_name, pattern in section_patterns.items():
            match = re.search(pattern, content)
            if match:
                # Extract a sample of text after the match
                start_pos = match.end()
                sample_text = content[start_pos:start_pos+500].strip()
                # Clean up whitespace
                sample_text = re.sub(r'\s+', ' ', sample_text)
                sections_found[section_name] = sample_text[:200] + "..." if len(sample_text) > 200 else sample_text
        
        print("\n" + "=" * 60)
        print("📋 DIRECT TEXT FILE RESULTS")
        print("=" * 60)
        
        print("✅ Successfully fetched text filing!")
        print(f"📄 Content length: {len(content):,} characters")
        print(f"🔗 Document URL: {text_url}")
        
        if sections_found:
            print(f"\n📚 Sections detected: {len(sections_found)}")
            for section, sample in sections_found.items():
                print(f"\n{section}:")
                print(f"   {sample}")
        else:
            print("⚠️  No standard sections detected - showing content sample:")
            sample = content[:1000]
            print(f"   {sample}...")
            
        return  # Exit early on success
        
    except Exception as e:
        print(f"❌ Direct text access failed: {e}")
        print("🔄 Falling back to HTML parsing method...")
    
        result = fetch_filing_content(test_filing['accession_number'], test_filing['cik'])
        
        print("\n" + "=" * 60)
        print("📋 HTML PARSING FALLBACK RESULTS")
        print("=" * 60)
        
        if result['success']:
            print("✅ Successfully fetched filing content!")
            print(f"📄 Content length: {result['content_length']:,} characters")
            print(f"🔗 Document URL: {result['doc_url']}")
            
            if result['sections_found']:
                print(f"\n📚 Sections detected: {len(result['sections_found'])}")
                for section, sample in result['sections_found'].items():
                    print(f"\n{section}:")
                    print(f"   {sample}")
            else:
                print("⚠️  No standard sections detected")
                
        else:
            print(f"❌ Failed to fetch filing content:")
            print(f"   Error: {result['error']}")
            print(f"   URL: {result['filing_url']}")
    
    print(f"\n✨ Filing content test completed!")


if __name__ == "__main__":
    main()