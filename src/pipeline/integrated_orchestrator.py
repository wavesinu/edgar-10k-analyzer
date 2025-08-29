"""์ถ์ถ ๋ฐ ๋ถ์ ํ์ดํ๋ผ์ธ์ ๊ฒฐํฉํ ํตํฉ ์ค์ผ์คํธ๋ ์ดํฐ."""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path

from loguru import logger
from rich.console import Console
from rich.progress import Progress, TaskID, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn

from config.settings import settings
from src.database.connection import db_client
from src.crawler.data_bridge import EdgarDataBridge
from src.nlp.qualitative_analyzer import QualitativeAnalyzer
from src.llm.investment_advisor import LLMInvestmentAdvisor
from src.reporting.dashboard import InvestmentDashboard


console = Console()


class IntegratedPipeline:
    """EDGAR ํฌ๋กค๋ง, ์ถ์ถ, AI ๋ถ์์ ๊ฒฐํฉํ ํตํฉ ํ์ดํ๋ผ์ธ."""
    
    def __init__(self):
        self.data_bridge = None
        self.qualitative_analyzer = QualitativeAnalyzer()
        self.investment_advisor = LLMInvestmentAdvisor()
        self.dashboard = InvestmentDashboard()
        
    async def __aenter__(self):
        """๋น๋๊ธฐ ์ปจํ์คํธ ๋งค๋์  ์งํ."""
        self.data_bridge = await EdgarDataBridge().__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """๋น๋๊ธฐ ์ปจํ์คํธ ๋งค๋์  ์ข๋ฃ."""
        if self.data_bridge:
            await self.data_bridge.__aexit__(exc_type, exc_val, exc_tb)
    
    async def run_full_pipeline(
        self,
        companies: Optional[List[str]] = None,
        max_companies: Optional[int] = None,
        max_filings_per_company: int = 3,
        force_recrawl: bool = False,
        enable_ai_analysis: bool = True
    ) -> Dict[str, Any]:
        """์ ์ฒด ํตํฉ ํ์ดํ๋ผ์ธ ์คํ."""
        start_time = datetime.now()
        
        # Determine companies to process
        if companies is None:
            companies = settings.top_nasdaq_tickers
            if max_companies:
                companies = companies[:max_companies]
        
        logger.info(f"Starting integrated pipeline for {len(companies)} companies")
        console.print(f"🚀 Processing {len(companies)} companies with up to {max_filings_per_company} filings each", style="bold blue")
        
        pipeline_stats = {
            'start_time': start_time,
            'companies_requested': len(companies),
            'companies_processed': 0,
            'filings_crawled': 0,
            'sections_extracted': 0,
            'analyses_completed': 0,
            'ai_insights_generated': 0,
            'errors': 0,
            'success_rate': 0.0
        }
        
        # Progress tracking
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
            expand=True
        ) as progress:
            
            # Main progress task
            main_task = progress.add_task(
                "Processing companies...", 
                total=len(companies)
            )
            
            # Phase 1: Data Collection and Extraction
            crawl_task = progress.add_task(
                "Phase 1: Crawling and extraction...", 
                total=len(companies)
            )
            
            crawl_stats = await self._run_crawling_phase(
                companies, max_filings_per_company, force_recrawl, progress, crawl_task
            )
            
            pipeline_stats.update({
                'companies_processed': crawl_stats['companies_processed'],
                'filings_crawled': crawl_stats['filings_processed'],
                'sections_extracted': crawl_stats['sections_extracted'],
                'errors': crawl_stats['errors']
            })
            
            progress.update(main_task, advance=crawl_stats['companies_processed'])
            
            # Phase 2: AI Analysis (if enabled)
            if enable_ai_analysis and crawl_stats['analysis_ready'] > 0:
                analysis_task = progress.add_task(
                    "Phase 2: AI analysis and insights...", 
                    total=crawl_stats['analysis_ready']
                )
                
                analysis_stats = await self._run_analysis_phase(
                    companies, progress, analysis_task
                )
                
                pipeline_stats.update({
                    'analyses_completed': analysis_stats['analyses_completed'],
                    'ai_insights_generated': analysis_stats['insights_generated'],
                })
                pipeline_stats['errors'] += analysis_stats['errors']
            
            # Phase 3: Generate Summary Report
            if pipeline_stats['analyses_completed'] > 0:
                summary_task = progress.add_task(
                    "Phase 3: Generating insights dashboard...", 
                    total=1
                )
                
                await self._generate_pipeline_summary(pipeline_stats)
                progress.update(summary_task, advance=1)
        
        # Calculate final statistics
        end_time = datetime.now()
        pipeline_stats.update({
            'end_time': end_time,
            'duration': end_time - start_time,
            'success_rate': pipeline_stats['analyses_completed'] / max(pipeline_stats['filings_crawled'], 1)
        })
        
        # Display results
        self._display_pipeline_results(pipeline_stats)
        
        return pipeline_stats
    
    async def _run_crawling_phase(
        self,
        companies: List[str],
        max_filings_per_company: int,
        force_recrawl: bool,
        progress: Progress,
        task_id: TaskID
    ) -> Dict[str, Any]:
        """Run the crawling and extraction phase."""
        logger.info("Starting crawling and extraction phase")
        
        crawl_stats = {
            'companies_processed': 0,
            'filings_processed': 0,
            'sections_extracted': 0,
            'analysis_ready': 0,
            'errors': 0
        }
        
        # Use data bridge for integrated crawling and extraction
        bridge_stats = await self.data_bridge.crawl_and_analyze_companies(
            companies, max_filings_per_company, settings.items_to_extract
        )
        
        crawl_stats.update(bridge_stats)
        progress.update(task_id, completed=len(companies))
        
        logger.info(f"Crawling phase completed: {crawl_stats}")
        return crawl_stats
    
    async def _run_analysis_phase(
        self,
        companies: List[str],
        progress: Progress,
        task_id: TaskID
    ) -> Dict[str, Any]:
        """Run the AI analysis phase."""
        logger.info("Starting AI analysis phase")
        
        analysis_stats = {
            'analyses_completed': 0,
            'insights_generated': 0,
            'errors': 0
        }
        
        # Get filings ready for analysis
        filings_for_analysis = await db_client.get_filings_for_analysis(companies)
        
        progress.update(task_id, total=len(filings_for_analysis))
        
        # Process each filing for analysis
        semaphore = asyncio.Semaphore(settings.max_concurrent_requests)
        
        async def analyze_filing(filing_info):
            async with semaphore:
                try:
                    # Get sections for this filing
                    sections = await db_client.get_filing_sections(filing_info['filing_id'])
                    
                    if not sections:
                        logger.warning(f"No sections found for filing {filing_info['ticker']} {filing_info['fiscal_year']}")
                        return {'success': False}
                    
                    # Run qualitative analysis
                    qualitative_results = await self._analyze_qualitative_aspects(sections)
                    
                    # Generate AI insights
                    ai_insights = await self._generate_ai_insights(filing_info, sections, qualitative_results)
                    
                    # Save analysis results
                    await self._save_analysis_results(filing_info, qualitative_results, ai_insights)
                    
                    progress.update(task_id, advance=1)
                    return {
                        'success': True,
                        'qualitative_score': qualitative_results.get('overall_score', 0),
                        'insights_count': len(ai_insights.get('insights', []))
                    }
                    
                except Exception as e:
                    logger.error(f"Error analyzing {filing_info.get('ticker', 'unknown')}: {e}")
                    progress.update(task_id, advance=1)
                    return {'success': False, 'error': str(e)}
        
        # Run analyses concurrently
        tasks = [analyze_filing(filing) for filing in filings_for_analysis]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for result in results:
            if isinstance(result, Exception):
                analysis_stats['errors'] += 1
            elif isinstance(result, dict):
                if result.get('success'):
                    analysis_stats['analyses_completed'] += 1
                    analysis_stats['insights_generated'] += result.get('insights_count', 0)
                else:
                    analysis_stats['errors'] += 1
        
        logger.info(f"Analysis phase completed: {analysis_stats}")
        return analysis_stats
    
    async def _analyze_qualitative_aspects(self, sections: List[Dict]) -> Dict[str, Any]:
        """Analyze qualitative aspects of filing sections."""
        try:
            # Combine sections for analysis
            combined_text = ""
            section_texts = {}
            
            for section in sections:
                section_name = section['section_name']
                content = section['content']
                section_texts[section_name] = content
                combined_text += f"\\n\\n=== {section_name.upper()} ===\\n{content}"
            
            # Run qualitative analysis
            results = await self.qualitative_analyzer.analyze_comprehensive(
                text=combined_text,
                sections=section_texts
            )
            
            return results
            
        except Exception as e:
            logger.error(f"Error in qualitative analysis: {e}")
            return {'overall_score': 0, 'error': str(e)}
    
    async def _generate_ai_insights(
        self,
        filing_info: Dict,
        sections: List[Dict],
        qualitative_results: Dict
    ) -> Dict[str, Any]:
        """Generate AI-powered investment insights."""
        try:
            # Prepare context for AI analysis
            context = {
                'company': filing_info['ticker'],
                'fiscal_year': filing_info['fiscal_year'],
                'qualitative_score': qualitative_results.get('overall_score', 0),
                'sections': {s['section_name']: s['content'][:5000] for s in sections},  # Limit content length
                'sentiment_analysis': qualitative_results.get('sentiment_analysis', {}),
                'key_metrics': qualitative_results.get('key_metrics', {})
            }
            
            # Generate investment insights
            insights = await self.investment_advisor.generate_investment_insights(context)
            
            return insights
            
        except Exception as e:
            logger.error(f"Error generating AI insights: {e}")
            return {'insights': [], 'error': str(e)}
    
    async def _save_analysis_results(
        self,
        filing_info: Dict,
        qualitative_results: Dict,
        ai_insights: Dict
    ) -> None:
        """Save analysis results to database."""
        try:
            # Save qualitative analysis results
            await db_client.save_qualitative_analysis(
                filing_id=filing_info['filing_id'],
                results=qualitative_results
            )
            
            # Save AI insights
            await db_client.save_ai_insights(
                filing_id=filing_info['filing_id'],
                insights=ai_insights
            )
            
            # Update filing status
            await db_client.update_filing_status(filing_info['filing_id'], 'analyzed')
            
            logger.debug(f"Saved analysis results for {filing_info['ticker']} {filing_info['fiscal_year']}")
            
        except Exception as e:
            logger.error(f"Error saving analysis results: {e}")
            raise
    
    async def _generate_pipeline_summary(self, pipeline_stats: Dict[str, Any]) -> None:
        """Generate and save pipeline summary."""
        try:
            summary = {
                'pipeline_run_date': datetime.now().isoformat(),
                'statistics': pipeline_stats,
                'top_performers': await db_client.get_top_qualitative_scores(limit=10),
                'recent_insights': await db_client.get_recent_ai_insights(limit=20),
                'processing_summary': {
                    'total_companies': pipeline_stats['companies_requested'],
                    'successful_analyses': pipeline_stats['analyses_completed'],
                    'success_rate': pipeline_stats['success_rate'],
                    'processing_time': str(pipeline_stats['duration'])
                }
            }
            
            # Save summary to database
            await db_client.save_pipeline_summary(summary)
            
            logger.info("Pipeline summary generated and saved")
            
        except Exception as e:
            logger.error(f"Error generating pipeline summary: {e}")
    
    def _display_pipeline_results(self, stats: Dict[str, Any]) -> None:
        """Display pipeline results in a formatted way."""
        console.print("\\n" + "="*60, style="bold green")
        console.print("🎉 INTEGRATED PIPELINE COMPLETED", style="bold green")
        console.print("="*60, style="bold green")
        
        console.print(f"📊 **Results Summary:**", style="bold blue")
        console.print(f"   • Companies processed: {stats['companies_processed']}/{stats['companies_requested']}")
        console.print(f"   • Filings crawled: {stats['filings_crawled']}")
        console.print(f"   • Sections extracted: {stats['sections_extracted']}")
        console.print(f"   • AI analyses completed: {stats['analyses_completed']}")
        console.print(f"   • AI insights generated: {stats['ai_insights_generated']}")
        console.print(f"   • Success rate: {stats['success_rate']:.1%}")
        console.print(f"   • Total duration: {stats['duration']}")
        console.print(f"   • Errors encountered: {stats['errors']}")
        
        if stats['analyses_completed'] > 0:
            console.print(f"\\n✅ **Next Steps:**", style="bold cyan")
            console.print("   • Run `python main.py dashboard` to view investment insights")
            console.print("   • Run `python main.py report` to generate detailed reports")
            console.print("   • Run `python main.py chat` to interact with the AI investment advisor")
        
        console.print("\\n" + "="*60, style="bold green")


# Convenience functions
async def run_integrated_pipeline(
    companies: Optional[List[str]] = None,
    max_companies: Optional[int] = None,
    max_filings_per_company: int = 3,
    force_recrawl: bool = False,
    enable_ai_analysis: bool = True
) -> Dict[str, Any]:
    """Run the complete integrated pipeline."""
    async with IntegratedPipeline() as pipeline:
        return await pipeline.run_full_pipeline(
            companies=companies,
            max_companies=max_companies,
            max_filings_per_company=max_filings_per_company,
            force_recrawl=force_recrawl,
            enable_ai_analysis=enable_ai_analysis
        )


async def run_crawling_only(
    companies: List[str],
    max_filings_per_company: int = 3
) -> Dict[str, Any]:
    """Run only the crawling and extraction phase."""
    async with EdgarDataBridge() as bridge:
        return await bridge.crawl_and_analyze_companies(
            companies, max_filings_per_company, settings.items_to_extract
        )


if __name__ == "__main__":
    # Test the integrated pipeline
    test_companies = ["AAPL", "MSFT"]
    results = asyncio.run(run_integrated_pipeline(
        companies=test_companies,
        max_filings_per_company=1,
        enable_ai_analysis=True
    ))
    print(f"Pipeline test results: {results}")