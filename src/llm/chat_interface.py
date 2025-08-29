"""투자 쿼리를 위한 인터랙티브 채팅 인터페이스."""

import asyncio
import uuid
from typing import List, Dict, Any, Optional
from datetime import datetime
import re

from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.markdown import Markdown
from rich.table import Table
from rich.prompt import Prompt, Confirm
from rich.live import Live
from rich.spinner import Spinner

from loguru import logger

from src.llm.investment_advisor import (
    LLMInvestmentAdvisor, AdvisorQuery, AdvisorResponse, QueryType
)
from config.settings import settings


class InvestmentChatInterface:
    """투자 조언을 위한 인터랙티브 채팅 인터페이스."""
    
    def __init__(self):
        self.console = Console()
        self.advisor = LLMInvestmentAdvisor()
        self.conversation_history: List[Dict[str, Any]] = []
        self.user_profile = {
            "risk_tolerance": None,
            "investment_amount": None,
            "timeframe": None,
            "interests": []
        }
    
    def display_welcome(self):
        """환영 메시지와 지시사항을 표시합니다."""
        welcome_text = """
# 🤖 EDGAR Investment Advisor

Welcome to your AI-powered investment assistant! I analyze SEC 10-K filings using advanced NLP and provide personalized investment advice.

## What I can help you with:
- 📊 **Company Analysis**: Deep dive into specific companies
- 🏛️ **Sector Comparison**: Compare industries and sectors  
- 📈 **Portfolio Advice**: Personalized portfolio recommendations
- ⚠️ **Risk Assessment**: Identify and analyze investment risks
- 🌍 **Market Outlook**: Current market conditions and trends
- 💡 **Investment Strategy**: General investment guidance

## Example queries:
- "What do you think about Apple's investment potential?"
- "Compare tech vs healthcare sectors"
- "I have $10,000 to invest, what do you recommend?"
- "What are the main risks with Tesla?"
- "How's the current market looking?"

Type 'help' for more commands or 'quit' to exit.
        """
        
        self.console.print(Panel(
            Markdown(welcome_text),
            title="🎯 Investment Advisor",
            border_style="blue"
        ))
    
    def setup_user_profile(self):
        """사용자 투자 프로필을 설정합니다."""
        if not any(self.user_profile.values()):
            if Confirm.ask("\n[bold blue]Would you like to set up your investment profile for personalized advice?[/bold blue]"):
                
                risk_options = ["conservative", "moderate", "aggressive"]
                self.console.print("\n[bold]Risk Tolerance:[/bold]")
                for i, option in enumerate(risk_options, 1):
                    self.console.print(f"  {i}. {option.title()}")
                
                risk_choice = Prompt.ask("Select your risk tolerance (1-3)", choices=["1", "2", "3"], default="2")
                self.user_profile["risk_tolerance"] = risk_options[int(risk_choice) - 1]
                
                # Investment amount (optional)
                amount_input = Prompt.ask("\n[bold]Investment amount (optional, e.g., 10000):[/bold]", default="")
                if amount_input.isdigit():
                    self.user_profile["investment_amount"] = float(amount_input)
                
                # Timeframe
                timeframe_options = ["short-term (< 1 year)", "medium-term (1-5 years)", "long-term (5+ years)"]
                self.console.print("\n[bold]Investment timeframe:[/bold]")
                for i, option in enumerate(timeframe_options, 1):
                    self.console.print(f"  {i}. {option}")
                
                timeframe_choice = Prompt.ask("Select timeframe (1-3)", choices=["1", "2", "3"], default="3")
                self.user_profile["timeframe"] = timeframe_options[int(timeframe_choice) - 1]
                
                self.console.print(f"\n✅ Profile setup complete! Risk: {self.user_profile['risk_tolerance']}, Timeframe: {self.user_profile['timeframe']}")
    
    def extract_tickers(self, query: str) -> List[str]:
        """쿼리에서 주식 티커를 추출합니다."""
        # Common ticker patterns
        ticker_pattern = r'\b[A-Z]{1,5}\b'
        potential_tickers = re.findall(ticker_pattern, query.upper())
        
        # Filter to known tickers (from settings)
        known_tickers = settings.top_nasdaq_tickers
        found_tickers = [t for t in potential_tickers if t in known_tickers]
        
        # Also check for company names
        company_name_map = {
            "APPLE": "AAPL",
            "MICROSOFT": "MSFT", 
            "GOOGLE": "GOOGL",
            "AMAZON": "AMZN",
            "TESLA": "TSLA",
            "META": "META",
            "FACEBOOK": "META",
            "NVIDIA": "NVDA"
        }
        
        for name, ticker in company_name_map.items():
            if name in query.upper():
                found_tickers.append(ticker)
        
        return list(set(found_tickers))  # Remove duplicates
    
    def create_advisor_query(self, user_input: str) -> AdvisorQuery:
        """사용자 입력에서 자문가 쿼리를 생성합니다."""
        query_id = f"chat_{uuid.uuid4().hex[:8]}"
        query_type = self.advisor.classify_query(user_input)
        tickers = self.extract_tickers(user_input)
        
        return AdvisorQuery(
            query_id=query_id,
            user_query=user_input,
            query_type=query_type,
            companies=tickers if tickers else None,
            timeframe=self.user_profile.get("timeframe"),
            risk_tolerance=self.user_profile.get("risk_tolerance"),
            investment_amount=self.user_profile.get("investment_amount"),
            context={"chat_interface": True}
        )
    
    def display_response(self, response: AdvisorResponse):
        """자문가 응답을 형식화된 방식으로 표시합니다."""
        # Main response
        self.console.print("\n")
        self.console.print(Panel(
            Markdown(response.response_text),
            title="💡 Investment Advice",
            border_style="green"
        ))
        
        # Recommendations table
        if response.recommendations:
            table = Table(title="📈 Specific Recommendations", show_header=True)
            table.add_column("Ticker", style="bold cyan")
            table.add_column("Recommendation", justify="center")
            table.add_column("Confidence/Score", justify="center")
            table.add_column("Key Point", style="dim")
            
            for rec in response.recommendations[:5]:  # Limit to top 5
                ticker = rec.get("ticker", "N/A")
                recommendation = rec.get("recommendation", "N/A")
                confidence = rec.get("confidence", rec.get("score", "N/A"))
                key_point = rec.get("rationale", rec.get("key_points", [""])[0] if rec.get("key_points") else "")
                
                # Truncate key point if too long
                if len(key_point) > 50:
                    key_point = key_point[:47] + "..."
                
                # Color code recommendation
                if recommendation in ["Strong Buy", "Buy"]:
                    rec_style = "[green]" + recommendation + "[/green]"
                elif recommendation == "Hold":
                    rec_style = "[yellow]" + recommendation + "[/yellow]"
                else:
                    rec_style = "[red]" + recommendation + "[/red]"
                
                table.add_row(ticker, rec_style, str(confidence), key_point)
            
            self.console.print(table)
        
        # Confidence and metadata
        confidence_color = "green" if response.confidence_level > 0.7 else "yellow" if response.confidence_level > 0.4 else "red"
        self.console.print(f"\n[{confidence_color}]Confidence Level: {response.confidence_level:.1%}[/{confidence_color}] | "
                          f"[dim]Model: {response.model_used} | Generated: {response.timestamp.strftime('%H:%M:%S')}[/dim]")
    
    def display_help(self):
        """도움말 정보를 표시합니다."""
        help_text = """
## 💬 Available Commands:
- **help** - Show this help message
- **profile** - Update your investment profile
- **history** - Show conversation history  
- **clear** - Clear conversation history
- **quit** - Exit the chat

## 🎯 Query Examples:

**Company Analysis:**
- "What's your take on Apple?"
- "Should I invest in Tesla?"
- "Tell me about Microsoft's prospects"

**Portfolio & Strategy:**
- "I have $5000, what should I invest in?"
- "Build me a conservative portfolio"  
- "How should I diversify my investments?"

**Market & Sectors:**
- "How's the tech sector looking?"
- "Compare Apple vs Microsoft"
- "What's your market outlook?"

**Risk Assessment:**
- "What are the risks with investing in Amazon?"
- "Is Tesla too risky for me?"
- "What should conservative investors avoid?"

## 🔧 Tips:
- Be specific about companies, amounts, and timeframes
- Mention your risk tolerance if you haven't set up a profile
- Ask follow-up questions for deeper analysis
- Use company names or ticker symbols (AAPL, TSLA, etc.)
        """
        
        self.console.print(Panel(
            Markdown(help_text),
            title="📚 Help Guide",
            border_style="blue"
        ))
    
    def display_conversation_history(self):
        """대화 기록을 표시합니다."""
        if not self.conversation_history:
            self.console.print("[yellow]No conversation history yet.[/yellow]")
            return
        
        self.console.print(Panel("📚 Conversation History", border_style="blue"))
        
        for i, exchange in enumerate(self.conversation_history[-5:], 1):  # Last 5 exchanges
            self.console.print(f"\n[bold]{i}. User:[/bold] {exchange['query']}")
            self.console.print(f"[bold]   Assistant:[/bold] {exchange['response'][:200]}{'...' if len(exchange['response']) > 200 else ''}")
    
    async def process_user_input(self, user_input: str) -> bool:
        """사용자 입력을 처리하고 계속 여부를 반환합니다."""
        user_input = user_input.strip()
        
        # Handle commands
        if user_input.lower() in ['quit', 'exit', 'bye']:
            self.console.print("[yellow]Thank you for using EDGAR Investment Advisor! Happy investing! 🚀[/yellow]")
            return False
        
        elif user_input.lower() == 'help':
            self.display_help()
            return True
        
        elif user_input.lower() == 'profile':
            self.setup_user_profile()
            return True
        
        elif user_input.lower() == 'history':
            self.display_conversation_history()
            return True
        
        elif user_input.lower() == 'clear':
            self.conversation_history.clear()
            self.console.print("[green]Conversation history cleared.[/green]")
            return True
        
        elif not user_input:
            return True
        
        # Process investment query
        try:
            # Show loading spinner
            with Live(Spinner("dots", text="🤔 Analyzing your query..."), console=self.console):
                query = self.create_advisor_query(user_input)
                response = await self.advisor.process_query(query)
            
            # Display response
            self.display_response(response)
            
            # Save to history
            self.conversation_history.append({
                "query": user_input,
                "response": response.response_text,
                "timestamp": response.timestamp,
                "query_type": query.query_type.value
            })
            
        except Exception as e:
            logger.error(f"Error processing query: {e}")
            self.console.print(f"[red]❌ Sorry, I encountered an error: {str(e)}[/red]")
            self.console.print("[yellow]Please try rephrasing your question or type 'help' for assistance.[/yellow]")
        
        return True
    
    async def run_chat_loop(self):
        """메인 채팅 루프."""
        self.display_welcome()
        self.setup_user_profile()
        
        self.console.print("\n" + "="*60)
        self.console.print("[bold green]💬 Chat started! Ask me anything about investments.[/bold green]")
        self.console.print("[dim]Type 'help' for commands or 'quit' to exit.[/dim]")
        
        while True:
            try:
                self.console.print("\n" + "-"*40)
                user_input = Prompt.ask("[bold blue]You[/bold blue]", console=self.console)
                
                should_continue = await self.process_user_input(user_input)
                if not should_continue:
                    break
                    
            except KeyboardInterrupt:
                self.console.print("\n[yellow]Chat interrupted. Type 'quit' to exit properly.[/yellow]")
            except Exception as e:
                logger.error(f"Unexpected error in chat loop: {e}")
                self.console.print(f"[red]Unexpected error: {str(e)}[/red]")


class InvestmentAPIInterface:
    """프로그래매틱 접근을 위한 API 스타일 인터페이스."""
    
    def __init__(self):
        self.advisor = LLMInvestmentAdvisor()
    
    async def ask_question(self, question: str, **kwargs) -> Dict[str, Any]:
        """질문을 하고 구조화된 응답을 받습니다."""
        query = AdvisorQuery(
            query_id=f"api_{uuid.uuid4().hex[:8]}",
            user_query=question,
            query_type=self.advisor.classify_query(question),
            companies=kwargs.get("companies"),
            risk_tolerance=kwargs.get("risk_tolerance"),
            investment_amount=kwargs.get("investment_amount"),
            timeframe=kwargs.get("timeframe")
        )
        
        response = await self.advisor.process_query(query)
        
        return {
            "answer": response.response_text,
            "recommendations": response.recommendations,
            "confidence": response.confidence_level,
            "query_type": query.query_type.value,
            "timestamp": response.timestamp.isoformat()
        }
    
    async def get_company_analysis(self, ticker: str) -> Dict[str, Any]:
        """특정 기업에 대한 분석을 가져옵니다."""
        return await self.ask_question(
            f"Provide a comprehensive investment analysis for {ticker}",
            companies=[ticker]
        )
    
    async def get_portfolio_advice(self, amount: float, risk_tolerance: str) -> Dict[str, Any]:
        """포트폴리오 조언을 받습니다."""
        return await self.ask_question(
            f"I have ${amount:,.0f} to invest with {risk_tolerance} risk tolerance. What do you recommend?",
            investment_amount=amount,
            risk_tolerance=risk_tolerance
        )


async def main():
    """채팅 인터페이스의 메인 진입점."""
    import argparse
    
    parser = argparse.ArgumentParser(description="EDGAR Investment Advisor Chat")
    parser.add_argument("--mode", choices=["chat", "api"], default="chat",
                       help="Interface mode")
    parser.add_argument("--question", type=str, help="Direct question for API mode")
    
    args = parser.parse_args()
    
    if args.mode == "chat":
        chat = InvestmentChatInterface()
        await chat.run_chat_loop()
    
    elif args.mode == "api" and args.question:
        api = InvestmentAPIInterface()
        result = await api.ask_question(args.question)
        print(json.dumps(result, indent=2, default=str))
    
    else:
        print("API mode requires --question parameter")


if __name__ == "__main__":
    asyncio.run(main())