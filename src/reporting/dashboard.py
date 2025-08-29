"""투자 대시보드 및 보고 시스템."""

import asyncio
import json
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, date, timedelta
from dataclasses import dataclass, asdict
import pandas as pd
from pathlib import Path

from rich.console import Console
from rich.table import Table
from rich.progress import Progress
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text
from rich.markdown import Markdown

from loguru import logger

from src.database.connection import db_client
from src.database.schema import InvestmentRecommendation


@dataclass
class DashboardData:
    """대시보드 표시를 위한 집계 데이터."""
    total_companies: int
    recommendations_by_type: Dict[str, int]
    top_performers: List[Dict[str, Any]]
    recent_analyses: List[Dict[str, Any]]
    sector_analysis: Dict[str, Any]
    risk_summary: Dict[str, Any]
    market_sentiment: Dict[str, float]


class InvestmentDashboard:
    """투자 추천을 위한 인터랙티브 대시보드."""
    
    def __init__(self):
        self.console = Console()
        self.data_cache: Optional[DashboardData] = None
        self.last_refresh: Optional[datetime] = None
    
    async def refresh_data(self) -> DashboardData:
        """데이터베이스에서 대시보드 데이터를 새로고침합니다."""
        logger.info("Refreshing dashboard data...")
        
        try:
            # Get investment recommendations
            recommendations = await db_client.get_investment_recommendations(limit=100)
            
            # Get processing statistics
            stats = await db_client.get_processing_stats()
            
            # Process recommendations data
            recommendations_by_type = {
                "Strong Buy": 0,
                "Buy": 0,
                "Hold": 0,
                "Sell": 0,
                "Strong Sell": 0
            }
            
            for rec in recommendations:
                rec_type = rec.get("recommendation", "").replace("_", " ").title()
                if rec_type in recommendations_by_type:
                    recommendations_by_type[rec_type] += 1
            
            # Top performers
            top_performers = sorted(
                recommendations,
                key=lambda x: x.get("qualitative_score", 0),
                reverse=True
            )[:10]
            
            # Recent analyses
            recent_analyses = sorted(
                recommendations,
                key=lambda x: x.get("analysis_date", ""),
                reverse=True
            )[:15]
            
            # Market sentiment (simplified)
            total_recs = len(recommendations)
            positive_recs = recommendations_by_type["Strong Buy"] + recommendations_by_type["Buy"]
            negative_recs = recommendations_by_type["Sell"] + recommendations_by_type["Strong Sell"]
            
            market_sentiment = {
                "bullish": (positive_recs / total_recs) if total_recs > 0 else 0,
                "bearish": (negative_recs / total_recs) if total_recs > 0 else 0,
                "neutral": (recommendations_by_type["Hold"] / total_recs) if total_recs > 0 else 0
            }
            
            self.data_cache = DashboardData(
                total_companies=stats.get("total_companies", 0),
                recommendations_by_type=recommendations_by_type,
                top_performers=top_performers,
                recent_analyses=recent_analyses,
                sector_analysis={},  # Would be populated with sector data
                risk_summary={},     # Would be populated with risk analysis
                market_sentiment=market_sentiment
            )
            
            self.last_refresh = datetime.now()
            logger.info("Dashboard data refreshed successfully")
            
            return self.data_cache
            
        except Exception as e:
            logger.error(f"Error refreshing dashboard data: {e}")
            # Return empty data structure
            return DashboardData(
                total_companies=0,
                recommendations_by_type={},
                top_performers=[],
                recent_analyses=[],
                sector_analysis={},
                risk_summary={},
                market_sentiment={}
            )
    
    def create_summary_panel(self) -> Panel:
        """요약 통계 패널을 생성합니다."""
        if not self.data_cache:
            return Panel("No data available", title="Summary")
        
        data = self.data_cache
        
        summary_text = f"""
[bold]Portfolio Analysis Summary[/bold]

📊 Total Companies Analyzed: {data.total_companies}
📈 Investment Recommendations: {sum(data.recommendations_by_type.values())}
🕒 Last Updated: {self.last_refresh.strftime('%Y-%m-%d %H:%M:%S') if self.last_refresh else 'Never'}

[bold]Market Sentiment Distribution:[/bold]
🟢 Bullish: {data.market_sentiment.get('bullish', 0):.1%}
🔴 Bearish: {data.market_sentiment.get('bearish', 0):.1%}
🟡 Neutral: {data.market_sentiment.get('neutral', 0):.1%}
"""
        return Panel(summary_text.strip(), title="📊 Market Overview", border_style="blue")
    
    def create_recommendations_table(self) -> Table:
        """추천 분포 테이블을 생성합니다."""
        table = Table(title="🎯 Investment Recommendations Distribution", show_header=True)
        table.add_column("Recommendation", style="bold")
        table.add_column("Count", justify="center")
        table.add_column("Percentage", justify="center")
        
        if self.data_cache:
            total = sum(self.data_cache.recommendations_by_type.values())
            
            for rec_type, count in self.data_cache.recommendations_by_type.items():
                percentage = (count / total * 100) if total > 0 else 0
                
                # Style based on recommendation type
                if rec_type in ["Strong Buy", "Buy"]:
                    style = "green"
                elif rec_type == "Hold":
                    style = "yellow"
                else:
                    style = "red"
                
                table.add_row(
                    f"[{style}]{rec_type}[/{style}]",
                    str(count),
                    f"{percentage:.1f}%"
                )
        
        return table
    
    def create_top_performers_table(self) -> Table:
        """상위 성과자 테이블을 생성합니다."""
        table = Table(title="🏆 Top Investment Opportunities", show_header=True)
        table.add_column("Rank", justify="center", width=6)
        table.add_column("Ticker", style="bold cyan", width=8)
        table.add_column("Recommendation", justify="center", width=12)
        table.add_column("Score", justify="center", width=8)
        table.add_column("Confidence", justify="center", width=10)
        table.add_column("Analysis Date", width=12)
        
        if self.data_cache:
            for i, performer in enumerate(self.data_cache.top_performers[:10], 1):
                rec = performer.get("recommendation", "").replace("_", " ").title()
                score = performer.get("qualitative_score", 0)
                confidence = performer.get("confidence", 0)
                
                # Style recommendation
                if rec in ["Strong Buy", "Buy"]:
                    rec_style = "green"
                elif rec == "Hold":
                    rec_style = "yellow"
                else:
                    rec_style = "red"
                
                # Format analysis date
                analysis_date = performer.get("analysis_date", "")
                if analysis_date:
                    try:
                        date_obj = datetime.fromisoformat(analysis_date.replace("Z", "+00:00"))
                        formatted_date = date_obj.strftime("%Y-%m-%d")
                    except:
                        formatted_date = str(analysis_date)[:10]
                else:
                    formatted_date = "N/A"
                
                table.add_row(
                    str(i),
                    performer.get("ticker", "N/A"),
                    f"[{rec_style}]{rec}[/{rec_style}]",
                    f"{score:.1f}",
                    f"{confidence:.1%}" if confidence else "N/A",
                    formatted_date
                )
        
        return table
    
    def create_recent_activity_table(self) -> Table:
        """최근 활동 테이블을 생성합니다."""
        table = Table(title="📋 Recent Analysis Activity", show_header=True)
        table.add_column("Ticker", style="bold", width=8)
        table.add_column("Recommendation", justify="center", width=12)
        table.add_column("Score", justify="center", width=8)
        table.add_column("Date", width=12)
        table.add_column("Key Insight", width=40)
        
        if self.data_cache:
            for activity in self.data_cache.recent_analyses[:10]:
                rec = activity.get("recommendation", "").replace("_", " ").title()
                score = activity.get("qualitative_score", 0)
                
                # Style recommendation
                if rec in ["Strong Buy", "Buy"]:
                    rec_style = "green"
                elif rec == "Hold":
                    rec_style = "yellow"
                else:
                    rec_style = "red"
                
                # Format date
                analysis_date = activity.get("analysis_date", "")
                if analysis_date:
                    try:
                        date_obj = datetime.fromisoformat(analysis_date.replace("Z", "+00:00"))
                        formatted_date = date_obj.strftime("%m-%d")
                    except:
                        formatted_date = str(analysis_date)[:5]
                else:
                    formatted_date = "N/A"
                
                # Generate key insight
                if score > 75:
                    insight = "Strong fundamentals, positive outlook"
                elif score > 60:
                    insight = "Good fundamentals, stable outlook"
                elif score > 40:
                    insight = "Mixed signals, monitor closely"
                else:
                    insight = "Caution advised, high risk factors"
                
                table.add_row(
                    activity.get("ticker", "N/A"),
                    f"[{rec_style}]{rec}[/{rec_style}]",
                    f"{score:.1f}",
                    formatted_date,
                    insight
                )
        
        return table
    
    async def display_dashboard(self):
        """완전한 대시보드를 표시합니다."""
        await self.refresh_data()
        
        self.console.clear()
        self.console.print("\n")
        
        # Header
        header = Text("EDGAR 10-K Qualitative Analysis Dashboard", style="bold magenta")
        self.console.print(header, justify="center")
        self.console.print("=" * 80, style="dim")
        self.console.print()
        
        # Summary panel
        self.console.print(self.create_summary_panel())
        self.console.print()
        
        # Recommendations distribution
        self.console.print(self.create_recommendations_table())
        self.console.print()
        
        # Top performers
        self.console.print(self.create_top_performers_table())
        self.console.print()
        
        # Recent activity
        self.console.print(self.create_recent_activity_table())
        self.console.print()
        
        # Footer
        self.console.print("=" * 80, style="dim")
        self.console.print(f"Dashboard generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 
                          style="dim", justify="center")
    
    async def export_report(self, format: str = "json", filename: Optional[str] = None) -> str:
        """대시보드 데이터를 다양한 형식으로 내보냅니다."""
        if not self.data_cache:
            await self.refresh_data()
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"edgar_investment_report_{timestamp}"
        
        reports_dir = Path("reports")
        reports_dir.mkdir(exist_ok=True)
        
        if format.lower() == "json":
            filepath = reports_dir / f"{filename}.json"
            
            report_data = {
                "generated_at": datetime.now().isoformat(),
                "summary": asdict(self.data_cache),
                "metadata": {
                    "total_recommendations": sum(self.data_cache.recommendations_by_type.values()),
                    "data_freshness": self.last_refresh.isoformat() if self.last_refresh else None
                }
            }
            
            with open(filepath, "w") as f:
                json.dump(report_data, f, indent=2, default=str)
            
        elif format.lower() == "csv":
            filepath = reports_dir / f"{filename}.csv"
            
            # Convert top performers to DataFrame
            df = pd.DataFrame(self.data_cache.top_performers)
            df.to_csv(filepath, index=False)
            
        elif format.lower() == "markdown":
            filepath = reports_dir / f"{filename}.md"
            
            markdown_content = self.generate_markdown_report()
            with open(filepath, "w") as f:
                f.write(markdown_content)
        
        logger.info(f"Report exported to {filepath}")
        return str(filepath)
    
    def generate_markdown_report(self) -> str:
        """마크다운 투자 보고서를 생성합니다."""
        if not self.data_cache:
            return "No data available for report generation."
        
        data = self.data_cache
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        report = f"""# EDGAR 10-K Qualitative Analysis Report

*Generated on {timestamp}*

## Executive Summary

This report analyzes qualitative factors from 10-K filings of {data.total_companies} NASDAQ companies using natural language processing and sentiment analysis.

### Key Metrics
- **Total Companies Analyzed**: {data.total_companies}
- **Investment Recommendations Generated**: {sum(data.recommendations_by_type.values())}
- **Market Sentiment**: {data.market_sentiment.get('bullish', 0):.1%} Bullish, {data.market_sentiment.get('bearish', 0):.1%} Bearish

## Investment Recommendations Distribution

| Recommendation | Count | Percentage |
|---------------|-------|------------|
"""
        
        total_recs = sum(data.recommendations_by_type.values())
        for rec_type, count in data.recommendations_by_type.items():
            percentage = (count / total_recs * 100) if total_recs > 0 else 0
            report += f"| {rec_type} | {count} | {percentage:.1f}% |\n"
        
        report += """
## Top Investment Opportunities

| Rank | Ticker | Recommendation | Score | Analysis Date |
|------|--------|----------------|-------|---------------|
"""
        
        for i, performer in enumerate(data.top_performers[:10], 1):
            ticker = performer.get("ticker", "N/A")
            rec = performer.get("recommendation", "").replace("_", " ").title()
            score = performer.get("qualitative_score", 0)
            
            analysis_date = performer.get("analysis_date", "")
            if analysis_date:
                try:
                    date_obj = datetime.fromisoformat(analysis_date.replace("Z", "+00:00"))
                    formatted_date = date_obj.strftime("%Y-%m-%d")
                except:
                    formatted_date = str(analysis_date)[:10]
            else:
                formatted_date = "N/A"
            
            report += f"| {i} | {ticker} | {rec} | {score:.1f} | {formatted_date} |\n"
        
        report += f"""

## Methodology

This analysis uses advanced natural language processing to evaluate qualitative factors in 10-K filings:

1. **Text Extraction**: Automated extraction of key sections (Business, Risk Factors, MD&A)
2. **Sentiment Analysis**: Multi-model sentiment analysis using TextBlob and BERT
3. **Theme Identification**: TF-IDF and clustering to identify key business themes
4. **Risk Assessment**: Automated categorization and severity scoring of risk factors
5. **Investment Scoring**: Composite scoring based on sentiment, risk, growth, and management factors

## Disclaimers

- This analysis is for informational purposes only and should not be considered as investment advice
- All investment decisions should be based on comprehensive due diligence
- Past performance does not guarantee future results
- Qualitative analysis should be combined with quantitative financial analysis

---
*Report generated by EDGAR 10-K Analyzer v1.0*
"""
        
        return report


class ReportGenerator:
    """다양한 유형의 투자 보고서를 생성합니다."""
    
    def __init__(self):
        self.dashboard = InvestmentDashboard()
    
    async def generate_daily_report(self) -> str:
        """일일 투자 요약 보고서를 생성합니다."""
        logger.info("Generating daily report...")
        
        timestamp = datetime.now().strftime("%Y%m%d")
        filename = f"daily_report_{timestamp}"
        
        return await self.dashboard.export_report("markdown", filename)
    
    async def generate_company_deep_dive(self, ticker: str) -> str:
        """특정 기업에 대한 상세 분석을 생성합니다."""
        logger.info(f"Generating deep dive report for {ticker}")
        
        try:
            # Get company analysis history
            history = await db_client.get_company_analysis_history(ticker)
            
            if not history:
                return f"No analysis data found for {ticker}"
            
            latest = history[0]
            
            # Generate detailed company report
            reports_dir = Path("reports")
            reports_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filepath = reports_dir / f"{ticker}_deep_dive_{timestamp}.md"
            
            report = f"""# {ticker} - Qualitative Analysis Deep Dive

*Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*

## Current Recommendation: {latest.get('recommendation', 'N/A').replace('_', ' ').title()}

**Confidence Level**: {latest.get('confidence', 0):.1%}
**Qualitative Score**: {latest.get('qualitative_score', 0):.1f}/100

## Key Investment Factors

### Strengths
"""
            
            strengths = latest.get('key_strengths', [])
            for strength in strengths[:5]:
                report += f"- {strength}\n"
            
            report += "\n### Concerns\n"
            concerns = latest.get('key_concerns', [])
            for concern in concerns[:5]:
                report += f"- {concern}\n"
            
            report += "\n### Risk Factors\n"
            risks = latest.get('risk_factors', [])
            for risk in risks[:5]:
                report += f"- {risk}\n"
            
            report += "\n### Growth Opportunities\n"
            opportunities = latest.get('growth_opportunities', [])
            for opportunity in opportunities[:5]:
                report += f"- {opportunity}\n"
            
            if len(history) > 1:
                report += f"\n## Historical Analysis\n\n"
                report += f"**Analysis History**: {len(history)} reports\n\n"
                
                for i, analysis in enumerate(history[:5]):
                    fiscal_year = analysis.get('fiscal_year', 'N/A')
                    score = analysis.get('qualitative_score', 0)
                    rec = analysis.get('recommendation', 'N/A').replace('_', ' ').title()
                    report += f"- **FY{fiscal_year}**: {rec} (Score: {score:.1f})\n"
            
            report += """

---
*This analysis is based on qualitative factors extracted from SEC 10-K filings and should be combined with quantitative financial analysis for investment decisions.*
"""
            
            with open(filepath, "w") as f:
                f.write(report)
            
            logger.info(f"Company report generated: {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"Error generating company report: {e}")
            return f"Error generating report for {ticker}: {str(e)}"


async def main():
    """메인 대시보드 애플리케이션."""
    import argparse
    
    parser = argparse.ArgumentParser(description="EDGAR Investment Dashboard")
    parser.add_argument("--mode", choices=["dashboard", "report", "company"], 
                       default="dashboard", help="Operation mode")
    parser.add_argument("--ticker", type=str, help="Company ticker for deep dive")
    parser.add_argument("--format", choices=["json", "csv", "markdown"], 
                       default="markdown", help="Report format")
    
    args = parser.parse_args()
    
    if args.mode == "dashboard":
        dashboard = InvestmentDashboard()
        await dashboard.display_dashboard()
        
        # Keep dashboard running
        console = Console()
        console.print("\n[dim]Press Ctrl+C to exit[/dim]")
        
        try:
            while True:
                await asyncio.sleep(300)  # Refresh every 5 minutes
                await dashboard.display_dashboard()
        except KeyboardInterrupt:
            console.print("\n[yellow]Dashboard stopped by user[/yellow]")
    
    elif args.mode == "report":
        dashboard = InvestmentDashboard()
        filepath = await dashboard.export_report(args.format)
        print(f"Report generated: {filepath}")
    
    elif args.mode == "company":
        if not args.ticker:
            print("Error: --ticker required for company mode")
            return
        
        reporter = ReportGenerator()
        filepath = await reporter.generate_company_deep_dive(args.ticker.upper())
        print(f"Company report generated: {filepath}")


if __name__ == "__main__":
    asyncio.run(main())