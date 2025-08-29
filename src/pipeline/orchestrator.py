"""EDGAR 10-K 분석을 위한 메인 파이프라인 오케스트레이터."""

import asyncio
import json
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass
import schedule
import time
from concurrent.futures import ThreadPoolExecutor
from loguru import logger

from config.settings import settings
from src.api.company_mapping import CompanyMapper, build_company_database
from src.api.edgar_client import EdgarClient
from src.nlp.text_processor import TextProcessor
from src.nlp.qualitative_analyzer import QualitativeAnalyzer
from src.nlp.investment_scorer import InvestmentScorer
from src.database.connection import db_client
from src.database.schema import (
    Company, Filing, QualitativeSection, 
    SentimentAnalysis, KeyTheme, RiskFactor,
    QualitativeScore, InvestmentAnalysis
)


@dataclass
class PipelineStats:
    """파이프라인 실행 통계."""
    start_time: datetime
    end_time: Optional[datetime] = None
    companies_processed: int = 0
    filings_processed: int = 0
    sections_extracted: int = 0
    analyses_completed: int = 0
    errors: int = 0
    error_details: List[str] = None
    
    def __post_init__(self):
        if self.error_details is None:
            self.error_details = []
    
    @property
    def duration(self) -> Optional[timedelta]:
        if self.end_time:
            return self.end_time - self.start_time
        return None
    
    @property
    def success_rate(self) -> float:
        total = self.filings_processed
        if total == 0:
            return 0.0
        return (total - self.errors) / total


class EdgarAnalysisPipeline:
    """EDGAR 10-K 분석을 위한 메인 파이프라인 오케스트레이터."""
    
    def __init__(self):
        self.stats = PipelineStats(start_time=datetime.now())
        self.text_processor = TextProcessor()
        self.qualitative_analyzer = QualitativeAnalyzer()
        self.investment_scorer = InvestmentScorer()
        
    async def initialize_company_database(self) -> Dict[str, Any]:
        """NASDAQ 상위 50개 기업으로 회사 데이터베이스 초기화."""
        logger.info("회사 데이터베이스 초기화 중...")
        
        try:
            # 회사 데이터베이스 구축
            companies = await build_company_database()
            
            # 데이터 디렉토리에 저장
            data_dir = Path("data")
            data_dir.mkdir(exist_ok=True)
            
            output_file = data_dir / "nasdaq_companies.json"
            with open(output_file, "w") as f:
                json.dump(companies, f, indent=2, default=str)
            
            logger.info(f"회사 데이터베이스가 {output_file}에 저장됨")
            logger.info(f"{len(companies)}개 회사 초기화 완료")
            
            return companies
            
        except Exception as e:
            logger.error(f"회사 데이터베이스 초기화 오류: {e}")
            raise
    
    async def process_company_filings(self, company_data: Dict[str, Any], 
                                    max_filings_per_company: int = 3) -> List[Filing]:
        """단일 회사의 파일링 처리."""
        ticker = company_data["ticker"]
        logger.info(f"{ticker}의 파일링 처리 중")
        
        try:
            async with EdgarClient() as edgar_client:
                filings = await edgar_client.process_company_filings(
                    company_data, max_filings=max_filings_per_company
                )
                
                self.stats.companies_processed += 1
                self.stats.filings_processed += len(filings)
                
                logger.info(f"{ticker}의 {len(filings)}개 파일링 처리 완료")
                return filings
                
        except Exception as e:
            logger.error(f"{ticker}의 파일링 처리 오류: {e}")
            self.stats.errors += 1
            self.stats.error_details.append(f"{ticker}: {str(e)}")
            return []
    
    async def extract_and_analyze_filing(self, filing: Filing) -> Optional[str]:
        """단일 파일링에서 섹션 추출 및 분석 수행."""
        logger.info(f"파일링 분석 중: {filing.ticker} {filing.fiscal_year}")
        
        try:
            # 파일링 다운로드 및 파싱
            async with EdgarClient() as edgar_client:
                html_content = await edgar_client.get_filing_html_content(
                    filing.cik, filing.accession_number
                )
                
                if not html_content:
                    logger.warning(f"{filing.ticker} {filing.fiscal_year}에 대한 컨텐트 없음")
                    return None
            
            # 섹션 추출 및 저장
            section_ids = await self.text_processor.process_and_store_sections(
                filing.id, html_content
            )
            
            if not section_ids:
                logger.warning(f"{filing.ticker} {filing.fiscal_year}에서 추출된 섹션 없음")
                return None
            
            self.stats.sections_extracted += len(section_ids)
            
            # 분석을 위해 저장된 섹션 가져오기
            sections_data = {}
            for section_id in section_ids:
                sections = await db_client.get_sections_by_filing(filing.id)
                for section in sections:
                    sections_data[section["section_name"]] = section["content"]
            
            # 각 섹션에 대해 정성적 분석 수행
            sentiment_analyses = []
            key_themes = []
            risk_factors = []
            
            for section_id in section_ids:
                section_data = next((s for s in sections if s["id"] == section_id), None)
                if not section_data:
                    continue
                
                content = section_data["content"]
                
                # 감정 분석
                sentiment = await self.qualitative_analyzer.analyze_sentiment(
                    content, section_id, filing.id
                )
                await db_client.insert_sentiment_analysis(sentiment)
                sentiment_analyses.append(sentiment)
                
                # 주제 추출
                themes = self.qualitative_analyzer.extract_key_themes(
                    content, section_id, filing.id
                )
                for theme in themes:
                    await db_client.insert_key_theme(theme)
                    key_themes.extend(themes)
                
                # 위험 분석 (주로 위험 요소 섹션에 대해)
                if section_data.get("section_name") == "risk_factors":
                    risks = self.qualitative_analyzer.analyze_risk_factors(
                        content, filing.id
                    )
                    for risk in risks:
                        await db_client.insert_risk_factor(risk)
                        risk_factors.extend(risks)
            
            # 정성적 점수 계산
            qualitative_score = await self.qualitative_analyzer.calculate_qualitative_scores(
                filing.id, filing.ticker, filing.fiscal_year, sections_data
            )
            await db_client.insert_qualitative_score(qualitative_score)
            
            # 투자 분석 생성
            investment_analysis = await self.investment_scorer.create_investment_analysis(
                qualitative_score
            )
            await db_client.insert_investment_analysis(investment_analysis)
            
            self.stats.analyses_completed += 1
            logger.info(f"{filing.ticker} {filing.fiscal_year} 분석 완료")
            
            return filing.id
            
        except Exception as e:
            logger.error(f"Error analyzing filing {filing.ticker} {filing.fiscal_year}: {e}")
            self.stats.errors += 1
            self.stats.error_details.append(f"{filing.ticker} {filing.fiscal_year}: {str(e)}")
            
            # Update filing status to failed
            if filing.id:
                await db_client.update_filing_status(filing.id, "failed")
            
            return None
    
    async def run_full_pipeline(self, max_companies: Optional[int] = None,
                               max_filings_per_company: int = 2) -> PipelineStats:
        """전체 분석 파이프라인 실행."""
        logger.info("Starting EDGAR 10-K Analysis Pipeline")
        self.stats = PipelineStats(start_time=datetime.now())
        
        try:
            # Step 1: Initialize company database
            logger.info("Step 1: Initializing company database")
            companies = await self.initialize_company_database()
            
            if max_companies:
                # Limit to first N companies for testing
                companies = dict(list(companies.items())[:max_companies])
                logger.info(f"Limited to {len(companies)} companies for testing")
            
            # Step 2: Process filings for each company
            logger.info("Step 2: Processing company filings")
            all_filings = []
            
            for ticker, company_data in companies.items():
                filings = await self.process_company_filings(
                    company_data, max_filings_per_company
                )
                all_filings.extend(filings)
                
                # Rate limiting between companies
                await asyncio.sleep(settings.request_delay)
            
            logger.info(f"Total filings to process: {len(all_filings)}")
            
            # Step 3: Extract and analyze each filing
            logger.info("Step 3: Extracting and analyzing filings")
            
            # Process filings in batches to avoid overwhelming the system
            batch_size = settings.batch_size
            for i in range(0, len(all_filings), batch_size):
                batch = all_filings[i:i + batch_size]
                logger.info(f"Processing batch {i//batch_size + 1}/{(len(all_filings)-1)//batch_size + 1}")
                
                # Process batch concurrently
                tasks = [self.extract_and_analyze_filing(filing) for filing in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Count successful analyses
                successful = sum(1 for r in results if r and not isinstance(r, Exception))
                logger.info(f"Batch completed: {successful}/{len(batch)} successful")
                
                # Brief pause between batches
                await asyncio.sleep(1)
            
            # Step 4: Generate summary statistics
            logger.info("Step 4: Generating summary statistics")
            await self.generate_pipeline_summary()
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            self.stats.errors += 1
            self.stats.error_details.append(f"Pipeline error: {str(e)}")
        
        finally:
            self.stats.end_time = datetime.now()
            logger.info("Pipeline execution completed")
            self.log_pipeline_stats()
        
        return self.stats
    
    async def generate_pipeline_summary(self):
        """파이프라인 실행 요약 생성 및 로그 기록."""
        try:
            # Get processing statistics from database
            db_stats = await db_client.get_processing_stats()
            
            # Get latest investment recommendations
            recommendations = await db_client.get_investment_recommendations(limit=20)
            
            logger.info("=== PIPELINE EXECUTION SUMMARY ===")
            logger.info(f"Duration: {self.stats.duration}")
            logger.info(f"Companies processed: {self.stats.companies_processed}")
            logger.info(f"Filings processed: {self.stats.filings_processed}")
            logger.info(f"Sections extracted: {self.stats.sections_extracted}")
            logger.info(f"Analyses completed: {self.stats.analyses_completed}")
            logger.info(f"Success rate: {self.stats.success_rate:.2%}")
            logger.info(f"Errors: {self.stats.errors}")
            
            if recommendations:
                logger.info("\n=== TOP INVESTMENT RECOMMENDATIONS ===")
                for rec in recommendations[:10]:
                    logger.info(f"{rec.get('ticker', 'N/A')}: {rec.get('recommendation', 'N/A')} "
                              f"(Score: {rec.get('qualitative_score', 0):.1f})")
            
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
    
    def log_pipeline_stats(self):
        """상세한 파이프라인 통계 로그 기록."""
        logger.info(f"Pipeline Statistics:")
        logger.info(f"  Start Time: {self.stats.start_time}")
        logger.info(f"  End Time: {self.stats.end_time}")
        logger.info(f"  Duration: {self.stats.duration}")
        logger.info(f"  Companies: {self.stats.companies_processed}")
        logger.info(f"  Filings: {self.stats.filings_processed}")
        logger.info(f"  Sections: {self.stats.sections_extracted}")
        logger.info(f"  Analyses: {self.stats.analyses_completed}")
        logger.info(f"  Errors: {self.stats.errors}")
        logger.info(f"  Success Rate: {self.stats.success_rate:.2%}")
        
        if self.stats.error_details:
            logger.error("Error Details:")
            for error in self.stats.error_details[-10:]:  # Show last 10 errors
                logger.error(f"  {error}")


class PipelineScheduler:
    """주기적으로 파이프라인을 실행하는 스케줄러."""
    
    def __init__(self):
        self.pipeline = EdgarAnalysisPipeline()
        self.is_running = False
    
    def schedule_daily_run(self, hour: int = 2, minute: int = 0):
        """일일 파이프라인 실행 예약."""
        schedule.every().day.at(f"{hour:02d}:{minute:02d}").do(self.run_scheduled_pipeline)
        logger.info(f"Scheduled daily pipeline run at {hour:02d}:{minute:02d}")
    
    def schedule_weekly_run(self, day: str = "monday", hour: int = 2, minute: int = 0):
        """주간 파이프라인 실행 예약."""
        getattr(schedule.every(), day.lower()).at(f"{hour:02d}:{minute:02d}").do(self.run_scheduled_pipeline)
        logger.info(f"Scheduled weekly pipeline run on {day} at {hour:02d}:{minute:02d}")
    
    def run_scheduled_pipeline(self):
        """별도의 스레드에서 파이프라인 실행."""
        if self.is_running:
            logger.warning("Pipeline is already running, skipping scheduled run")
            return
        
        def run_pipeline():
            self.is_running = True
            try:
                # Run with limited scope for scheduled runs
                asyncio.run(self.pipeline.run_full_pipeline(
                    max_companies=10,  # Limit for scheduled runs
                    max_filings_per_company=1
                ))
            finally:
                self.is_running = False
        
        thread = ThreadPoolExecutor(max_workers=1)
        thread.submit(run_pipeline)
    
    def start_scheduler(self):
        """스케줄러 루프 시작."""
        logger.info("Starting pipeline scheduler...")
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute


async def main():
    """파이프라인의 메인 진입점."""
    import argparse
    
    parser = argparse.ArgumentParser(description="EDGAR 10-K Analysis Pipeline")
    parser.add_argument("--mode", choices=["full", "test", "schedule"], default="test",
                       help="Pipeline execution mode")
    parser.add_argument("--companies", type=int, default=5,
                       help="Number of companies to process (test mode)")
    parser.add_argument("--filings", type=int, default=1,
                       help="Number of filings per company")
    
    args = parser.parse_args()
    
    if args.mode == "schedule":
        scheduler = PipelineScheduler()
        scheduler.schedule_weekly_run("sunday", 2, 0)  # Sunday 2 AM
        scheduler.start_scheduler()
    
    else:
        pipeline = EdgarAnalysisPipeline()
        
        if args.mode == "full":
            stats = await pipeline.run_full_pipeline(
                max_companies=None,
                max_filings_per_company=args.filings
            )
        else:  # test mode
            stats = await pipeline.run_full_pipeline(
                max_companies=args.companies,
                max_filings_per_company=args.filings
            )
        
        print(f"\nPipeline completed in {stats.duration}")
        print(f"Success rate: {stats.success_rate:.2%}")
        print(f"Companies: {stats.companies_processed}, Filings: {stats.filings_processed}")
        print(f"Analyses: {stats.analyses_completed}, Errors: {stats.errors}")


if __name__ == "__main__":
    asyncio.run(main())