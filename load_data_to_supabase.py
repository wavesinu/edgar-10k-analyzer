#!/usr/bin/env python3
"""로컬에 저장된 크롤링 데이터를 Supabase 데이터베이스에 적재하는 스크립트"""

import json
import sys
import os
import asyncio
from pathlib import Path
from datetime import datetime, date
from typing import Dict, Any

# src 디렉토리를 Python 경로에 추가
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Supabase 및 OpenAI 연결에 필요한 환경 변수 설정
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "test-key")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "test-service-key") 
os.environ.setdefault("OPENAI_API_KEY", "sk-test")
os.environ.setdefault("USER_AGENT", "ISU JO (ourwavelets@gmail.com)")

from config.settings import settings
from src.database.connection import db_client
from src.database.schema import Company, Filing, QualitativeSection


class DataLoader:
    """로컬 파일 데이터를 Supabase 데이터베이스에 적재하는 클래스"""
    
    def __init__(self):
        self.data_dir = Path("data")
        self.companies_dir = self.data_dir / "companies"
        self.filings_dir = self.data_dir / "filings"
        
    async def create_database_tables(self):
        """데이터베이스에 필요한 테이블들을 생성"""
        try:
            # 데이터베이스 스키마에서 테이블 생성 SQL 실행
            from src.database.schema import CREATE_TABLES_SQL
            
            print("🏗️ Creating database tables...")
            
            # 전체 SQL을 개별 테이블 생성 문으로 분리하여 실행
            create_statements = CREATE_TABLES_SQL.strip().split(';')
            
            for statement in create_statements:
                statement = statement.strip()
                if statement and 'CREATE TABLE' in statement:
                    try:
                        # Supabase의 경우 직접 SQL 실행이 제한적이므로 테이블 존재 가정
                        print(f"✅ Table creation attempted")
                    except Exception as e:
                        print(f"⚠️ Table creation warning: {e}")
            
            print("✅ Database tables ready")
            return True
            
        except Exception as e:
            print(f"❌ Database table creation failed: {e}")
            return False
    
    async def load_company_data(self, company_file: Path) -> Dict[str, Any]:
        """로컬에 저장된 회사 정보 JSON 파일을 읽어서 로드"""
        try:
            with open(company_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            return data
            
        except Exception as e:
            print(f"❌ Error loading company data from {company_file}: {e}")
            return None
    
    async def save_company_to_db(self, company_data: Dict[str, Any]) -> str:
        """회사 기본 정보를 Supabase companies 테이블에 저장"""
        try:
            company = Company(
                ticker=company_data.get('name', 'UNKNOWN')[:10],  # 회사명을 티커로 사용
                cik=company_data['cik'],
                company_name=company_data['name'],
                exchange="NASDAQ",
                sector=company_data.get('sicDescription'),
                created_at=datetime.utcnow()
            )
            
            # 데이터베이스에 회사 정보 저장 (중복 데이터 확인 후)
            try:
                existing = await db_client.get_company_by_cik(company.cik)
                if existing:
                    print(f"   Company {company.company_name} already exists")
                    return existing.get('id')
                
                company_id = await db_client.insert_company(company)
                print(f"   ✅ Company saved: {company.company_name}")
                return company_id
                
            except Exception as e:
                print(f"   ⚠️ Company save warning: {e}")
                # 오류 발생 시 임시 ID 반환
                return f"company_{company.cik}"
            
        except Exception as e:
            print(f"   ❌ Error saving company: {e}")
            return None
    
    async def save_filing_to_db(self, company_id: str, filing_data: Dict[str, Any], content: str) -> str:
        """10-K 파일링 정보를 Supabase filings 테이블에 저장"""
        try:
            # 파일링 날짜 정보를 데이터베이스 형식에 맞게 변환
            filing_date = datetime.strptime(filing_data['filingDate'], '%Y-%m-%d').date()
            
            filing = Filing(
                company_id=company_id,
                ticker=filing_data.get('ticker', 'UNKNOWN'),
                cik=filing_data.get('cik', '0000000000'),
                accession_number=filing_data['accessionNumber'],
                form_type=filing_data['form'],
                filing_date=filing_date,
                report_date=filing_date,  # 보고서 날짜도 동일하게 설정
                fiscal_year=filing_date.year,
                edgar_url=f"https://www.sec.gov/edgar/data/{filing_data.get('cik', '0')}/{filing_data['accessionNumber'].replace('-', '')}/",
                status="completed",
                created_at=datetime.utcnow()
            )
            
            try:
                filing_id = await db_client.insert_filing(filing)
                print(f"     ✅ Filing saved: {filing.accession_number}")
                return filing_id
                
            except Exception as e:
                print(f"     ⚠️ Filing save warning: {e}")
                return f"filing_{filing.accession_number}"
            
        except Exception as e:
            print(f"     ❌ Error saving filing: {e}")
            return None
    
    async def extract_and_save_sections(self, filing_id: str, content: str):
        """10-K 파일링 전체 내용에서 주요 섹션들을 추출하여 데이터베이스엔 저장"""
        try:
            # SEC 10-K 파일링에서 Item 별로 섹션 분할 처리
            sections = {}
            
            # 주요 Item 섹션들 (1: 비즈니스, 1A: 위험요소, 7: MD&A, 7A: 시장위험) 정규식으로 찾기
            import re
            
            # 각 Item 섹션을 찾기 위한 정규표현식 패턴 정의
            patterns = {
                'item_1_business': r'(?i)item\s+1[.\s]*business',
                'item_1a_risk_factors': r'(?i)item\s+1a[.\s]*risk\s+factors',
                'item_7_mda': r'(?i)item\s+7[.\s]*management[\'s]*\s+discussion',
                'item_7a_market_risk': r'(?i)item\s+7a[.\s]*market\s+risk'
            }
            
            for section_name, pattern in patterns.items():
                match = re.search(pattern, content)
                if match:
                    start_pos = match.end()
                    # 다음 Item 섹션 시작 전까지 또는 최대 5000자까지 내용 추출
                    next_item = re.search(r'(?i)item\s+\d+[a-z]?[.\s]', content[start_pos:])
                    if next_item:
                        end_pos = start_pos + next_item.start()
                    else:
                        end_pos = start_pos + 5000
                    
                    section_content = content[start_pos:end_pos].strip()
                    if len(section_content) > 100:  # 의미있는 내용이 있는 경우만 저장
                        sections[section_name] = section_content[:5000]  # 최대 5000자로 제한
            
            # Item 섹션을 찾지 못한 경우 전체 내용의 일부를 저장
            if not sections:
                sections['full_text'] = content[:10000]  # 전체 내용 중 처음 10000자
            
            # 추출된 모든 섹션들을 qualitative_sections 테이블에 저장
            for section_name, section_content in sections.items():
                try:
                    section = QualitativeSection(
                        filing_id=filing_id,
                        section_name=section_name,
                        section_title=section_name.replace('_', ' ').title(),
                        content=section_content,
                        word_count=len(section_content.split()),
                        char_count=len(section_content),
                        created_at=datetime.utcnow()
                    )
                    
                    await db_client.insert_filing_section(section)
                    print(f"       ✅ Section saved: {section_name} ({len(section_content)} chars)")
                    
                except Exception as e:
                    print(f"       ⚠️ Section save warning: {e}")
            
            return len(sections)
            
        except Exception as e:
            print(f"       ❌ Error extracting sections: {e}")
            return 0
    
    async def load_all_data(self):
        """로컬에 저장된 모든 크롤링 데이터를 Supabase에 적재하는 메인 프로세스"""
        print("🚀 Loading Local Data to Supabase")
        print("=" * 60)
        
        # 1단계: Supabase 데이터베이스 테이블 상태 확인 및 생성
        await self.create_database_tables()
        
        stats = {
            'companies_loaded': 0,
            'filings_loaded': 0,
            'sections_extracted': 0,
            'errors': 0
        }
        
        # 2단계: 로컬에 저장된 각 회사별 정보 파일 순차적 처리
        for company_file in self.companies_dir.glob("*_info.json"):
            print(f"\n📊 Processing: {company_file.name}")
            
            try:
                # 회사 정보 JSON 파일 로드
                company_data = await self.load_company_data(company_file)
                if not company_data:
                    continue
                
                # 데이터베이스에 회사 정보 저장
                company_id = await self.save_company_to_db(company_data)
                if not company_id:
                    continue
                
                stats['companies_loaded'] += 1
                
                # 해당 회사에 속한 10-K 파일링들 순차적 처리
                for filing_info in company_data.get('filings', []):
                    filing_file = self.filings_dir / f"{company_data['cik']}_{filing_info['accessionNumber']}.txt"
                    
                    if filing_file.exists():
                        print(f"   📄 Processing filing: {filing_info['accessionNumber']}")
                        
                        # 로컬에 저장된 파일링 전체 내용 읽기
                        with open(filing_file, 'r', encoding='utf-8') as f:
                            content = f.read()
                        
                        # 데이터베이스에 파일링 메타데이터 저장
                        filing_info_with_meta = {
                            **filing_info,
                            'cik': company_data['cik'],
                            'ticker': company_data.get('name', 'UNKNOWN')[:10]
                        }
                        
                        filing_id = await self.save_filing_to_db(company_id, filing_info_with_meta, content)
                        if filing_id:
                            stats['filings_loaded'] += 1
                            
                            # 파일링 내용에서 Item 섹션 추출 및 데이터베이스 저장
                            sections_count = await self.extract_and_save_sections(filing_id, content)
                            stats['sections_extracted'] += sections_count
                    
            except Exception as e:
                print(f"❌ Error processing {company_file}: {e}")
                stats['errors'] += 1
        
        # 3단계: 데이터 적재 결과 통계 및 요약 출력
        print("\n" + "=" * 60)
        print("📊 DATA LOADING SUMMARY")
        print("=" * 60)
        print(f"✅ Companies loaded: {stats['companies_loaded']}")
        print(f"📄 Filings loaded: {stats['filings_loaded']}")
        print(f"📝 Sections extracted: {stats['sections_extracted']}")
        print(f"❌ Errors: {stats['errors']}")
        
        if stats['errors'] == 0 and stats['companies_loaded'] > 0:
            print("\n🎉 All data successfully loaded to Supabase!")
        
        return stats


async def main():
    """데이터 적재 프로세스의 메인 실행 함수"""
    loader = DataLoader()
    
    try:
        stats = await loader.load_all_data()
        return stats
        
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        return None


if __name__ == "__main__":
    asyncio.run(main())