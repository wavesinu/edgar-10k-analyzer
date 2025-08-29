# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

### Setup and Installation
```bash
# Initial setup
python setup.py

# Install dependencies  
pip install -r requirements.txt

# Download NLTK data
python -c "import nltk; nltk.download('punkt'); nltk.download('stopwords'); nltk.download('wordnet'); nltk.download('averaged_perceptron_tagger')"
```

### Running the Application
```bash
# Initialize company database (required first step)
python main.py init-companies

# Run analysis pipeline (test mode - 5 companies)
python main.py pipeline --mode test --companies 5

# Run full pipeline (all 50 NASDAQ companies)  
python main.py pipeline --mode full --filings 2

# Launch interactive dashboard
python main.py dashboard

# Generate reports
python main.py report --format markdown
python main.py report --company AAPL

# AI chat interface
python main.py chat

# Check system status
python main.py status
```

### Testing
```bash
# Run tests
pytest

# Run specific test types
pytest -m unit
pytest -m integration

# Test database connections
python tests/test_supabase_connection.py
python tests/test_sec_api_connection.py

# Test installation
python test_installation.py
```

### Development Tools
```bash
# Code formatting
black .

# Linting
flake8

# Type checking
mypy src/
```

## High-Level Architecture

This is an integrated EDGAR 10-K analyzer that combines advanced SEC filing crawling with AI-powered qualitative investment analysis.

### Core Components

**Data Pipeline Flow:**
1. **EDGAR API Layer** (`src/api/`): Company mapping and 10-K filing retrieval from SEC.gov
2. **Crawler Engine** (`src/crawler/`): Advanced async extraction with integrated toolkit capabilities
3. **Text Processing** (`src/nlp/`): Multi-model sentiment analysis, theme extraction, risk assessment
4. **AI Analysis** (`src/llm/`): OpenAI GPT-4 powered investment insights and chat interface
5. **Database Layer** (`src/database/`): Supabase cloud database with structured schema
6. **Reporting** (`src/reporting/`): Interactive dashboards and export capabilities

### Key Architecture Patterns

- **Async Processing**: All I/O operations use asyncio for high throughput
- **Pydantic Models**: Strict type validation throughout (`src/database/schema.py`)
- **Orchestrated Pipeline**: Main pipeline in `src/pipeline/integrated_orchestrator.py`
- **Modular NLP**: Multiple sentiment models (TextBlob, BERT, financial domain)
- **Supabase Integration**: Cloud-first database with connection pooling

### Configuration Management

- Settings in `config/settings.py` using Pydantic with environment variable mapping
- Default NASDAQ top 50 companies list configurable
- Pipeline parameters (batch size, concurrency, delays) adjustable
- Multi-model AI configuration (OpenAI models, temperature, tokens)

### Database Schema

Core entities: Companies → Filings → QualitativeSections → Analysis Results
- **Investment Scoring**: Composite scores from sentiment (25%), risk (35%), growth (20%), management (10%), financial health (10%)
- **Recommendation Engine**: 5-tier recommendation system (Strong Buy to Strong Sell)

### Critical Dependencies

- **Supabase**: Primary database (requires SUPABASE_URL, SUPABASE_KEY, SUPABASE_SERVICE_KEY)
- **OpenAI API**: AI analysis engine (requires OPENAI_API_KEY)  
- **SEC.gov Compliance**: Proper User-Agent header required (configured in settings)
- **NLTK Data**: Must download punkt, stopwords, wordnet, averaged_perceptron_tagger

### Environment Setup

The `.env` file must contain:
- Supabase credentials (URL, keys)
- OpenAI API key
- SEC-compliant User-Agent header
- Optional: rate limiting and batch size parameters

### Data Flow Integration

The system combines two extraction approaches:
1. **Live EDGAR Crawling**: Real-time SEC API integration
2. **Legacy Data Migration**: Import from existing extraction toolkit datasets

Both feed into the same analysis pipeline for unified processing.