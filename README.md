# Integrated EDGAR 10-K Analyzer

A comprehensive system that combines advanced EDGAR crawling capabilities with AI-powered qualitative investment analysis. This integrated platform extracts, processes, and analyzes SEC 10-K filings to provide sophisticated investment insights using natural language processing and large language models.

> **🔄 Now with Integrated Extraction Engine**: Enhanced with the power of the edgar-extraction-toolkit for superior data crawling and parsing capabilities.

## 🚀 Features

### 🔍 Advanced Data Extraction
- **Enhanced EDGAR Crawler**: High-performance async crawler with rate limiting and error recovery
- **Intelligent Section Parsing**: Advanced text extraction supporting 15+ 10-K sections
- **Multi-format Support**: HTML, XML, and text filing formats
- **Legacy Data Migration**: Seamlessly migrate existing extraction toolkit data

### 🧠 AI-Powered Analysis
- **Qualitative Analysis Engine**: NLP-based sentiment and risk assessment
- **Investment Advisor AI**: GPT-4 powered investment insights and recommendations
- **Interactive Chat Interface**: Real-time Q&A with AI investment advisor
- **Automated Scoring**: Multi-dimensional qualitative scoring system

### 📊 Comprehensive Reporting
- **Interactive Dashboard**: Real-time investment metrics and company performance
- **Custom Reports**: Company-specific deep-dive analysis reports
- **Export Capabilities**: JSON, CSV, and Markdown format support
- **Scheduled Analysis**: Automated weekly pipeline execution

### ⚡ Performance & Scalability
- **Async Processing**: High-throughput concurrent operations
- **Database Integration**: Supabase cloud database with optimized queries
- **Smart Caching**: Intelligent data caching and incremental updates
- **Error Recovery**: Robust error handling with automatic retries

## 🏗 Architecture

```
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│   EDGAR API         │    │  Text Processing    │    │   NLP Analysis      │
│                     │    │                     │    │                     │
│ • Company Mapping   │ -> │ • HTML Parsing      │ -> │ • Sentiment Analysis│
│ • 10-K Retrieval    │    │ • Section Extract   │    │ • Theme Extraction  │
│ • Filing Downloads  │    │ • Text Cleaning     │    │ • Risk Assessment   │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
                                                                   │
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│   Reporting         │    │   Supabase DB       │    │ Investment Scoring  │
│                     │    │                     │    │                     │
│ • Dashboard         │ <- │ • Companies         │ <- │ • Composite Scoring │
│ • Investment Report │    │ • Filings           │    │ • Recommendations   │
│ • Company Analysis  │    │ • Analysis Results  │    │ • Peer Comparison   │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
```

## 📦 Installation

### Prerequisites

- Python 3.8+
- Supabase account and database
- SEC API compliance (proper User-Agent header)

### Setup

1. **Clone the repository**
```bash
git clone <repository-url>
cd edgar-10k-analyzer
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Download NLTK data**
```bash
python -c "import nltk; nltk.download('punkt'); nltk.download('stopwords'); nltk.download('wordnet'); nltk.download('averaged_perceptron_tagger')"
```

4. **Configure environment**
```bash
cp .env .env
# Edit .env with your Supabase credentials and settings
```

5. **Set up Supabase database**
   - Create a new Supabase project
   - Run the SQL schema from `src/database/schema.py` in your Supabase SQL editor
   - Update your `.env` file with connection details

## 🚀 Usage

### Command Line Interface

The application provides a comprehensive CLI with multiple commands:

```bash
# Initialize company database
python main.py init-companies

# Run analysis pipeline (test mode - 5 companies)
python main.py pipeline --mode test --companies 5

# Run full pipeline (all top 50 NASDAQ companies)
python main.py pipeline --mode full --filings 2

# Launch interactive dashboard
python main.py dashboard

# Generate market report
python main.py report --format markdown

# Generate company-specific analysis
python main.py report --company AAPL

# Check system status
python main.py status

# Run automated scheduler
python main.py scheduler
```

### Pipeline Modes

**Test Mode**: Process a limited number of companies for testing
```bash
python main.py pipeline --mode test --companies 10 --filings 1
```

**Full Mode**: Process all top 50 NASDAQ companies
```bash
python main.py pipeline --mode full --filings 3
```

### Dashboard

Interactive dashboard showing:
- Investment recommendation distribution
- Top performing companies
- Recent analysis activity
- Market sentiment overview

```bash
python main.py dashboard
```

### Reports

Generate various types of reports:

**Market Overview Report**
```bash
python main.py report --format markdown
```

**Company Deep Dive**
```bash
python main.py report --company TSLA --format markdown
```

**Export Data**
```bash
python main.py report --format csv
python main.py report --format json
```

## 🔧 Configuration

### Environment Variables

Key configuration options in `.env`:

```env
# EDGAR API
SEC_API_KEY=optional_sec_api_key
USER_AGENT=YourCompany YourName (yourname@yourcompany.com)

# Supabase
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-supabase-anon-key
SUPABASE_SERVICE_KEY=your-supabase-service-key

# Pipeline Settings
BATCH_SIZE=10
MAX_CONCURRENT_REQUESTS=5
REQUEST_DELAY=0.1

# Logging
LOG_LEVEL=INFO
LOG_FILE=logs/edgar_analyzer.log
```

### Customization

**Company Selection**: Modify `top_nasdaq_tickers` in `config/settings.py`

**Analysis Weights**: Adjust scoring weights in `src/nlp/investment_scorer.py`

**NLP Models**: Configure sentiment models in `src/nlp/qualitative_analyzer.py`

## 📊 Data Model

### Core Entities

- **Companies**: NASDAQ company information and CIK mappings
- **Filings**: 10-K filing metadata and processing status
- **Qualitative Sections**: Extracted text sections with metadata
- **Sentiment Analysis**: Multi-model sentiment scores
- **Key Themes**: Identified business themes and keywords
- **Risk Factors**: Categorized and scored risk assessments
- **Investment Analysis**: Final recommendations and scores

### Investment Scoring

The system generates composite investment scores based on:

- **Sentiment (25%)**: Overall management tone and outlook
- **Risk (35%)**: Risk factor analysis and severity
- **Growth (20%)**: Growth indicators and opportunities
- **Management (10%)**: Management quality and strategy clarity
- **Financial Health (10%)**: Financial stress indicators

## 🤖 AI/NLP Capabilities

### Sentiment Analysis
- **TextBlob**: Basic polarity and subjectivity analysis
- **BERT**: Advanced contextual sentiment classification
- **Financial Domain**: Specialized business terminology handling

### Theme Extraction
- **TF-IDF Vectorization**: Term importance scoring
- **K-Means Clustering**: Automatic theme grouping
- **Business Context**: Domain-specific keyword categorization

### Risk Assessment
- **Pattern Recognition**: Automated risk factor identification
- **Severity Scoring**: Risk impact and likelihood assessment
- **Categorization**: Market, operational, regulatory, financial risks

## 📈 Investment Recommendations

### Recommendation Scale
- **Strong Buy**: Score > 80, high confidence
- **Buy**: Score > 65, good fundamentals
- **Hold**: Score 45-65, mixed signals
- **Sell**: Score 30-45, concerns present
- **Strong Sell**: Score < 30, significant risks

### Analysis Components
- **Qualitative Factors**: Management tone, strategic clarity
- **Risk Profile**: Risk diversity and severity
- **Growth Potential**: Innovation and market expansion
- **Competitive Position**: Advantage and differentiation
- **Financial Health**: Stress indicators and cash flow

## 🗓️ Automation

### Scheduled Runs
The pipeline can run automatically on a schedule:

```bash
# Weekly runs every Sunday at 2 AM
python main.py scheduler
```

### Batch Processing
- Configurable batch sizes for rate limiting
- Concurrent processing with respect for API limits
- Error handling and retry mechanisms
- Progress tracking and statistics

## 📋 Outputs

### Dashboard
- Real-time investment recommendations
- Market sentiment distribution
- Top performing companies
- Recent analysis activity

### Reports
- **Markdown**: Human-readable investment reports
- **JSON**: Structured data for API integration
- **CSV**: Spreadsheet-compatible data export

### Logs
- Comprehensive logging with rotation
- Error tracking and debugging information
- Performance metrics and statistics

## ⚠️ Important Disclaimers

- **Not Investment Advice**: This analysis is for informational purposes only
- **SEC Compliance**: Ensure your User-Agent header complies with SEC guidelines
- **Rate Limiting**: Respect EDGAR API rate limits (10 requests per second)
- **Data Quality**: Results depend on text extraction accuracy
- **Model Limitations**: NLP models may miss nuanced business context

## 🔧 Troubleshooting

### Common Issues

**SEC API Errors**
```bash
# Ensure proper User-Agent header
USER_AGENT="YourCompany YourName (yourname@yourcompany.com)"
```

**Database Connection Issues**
```bash
# Verify Supabase credentials
python -c "from src.database.connection import db_client; print('Connection OK')"
```

**NLTK Data Missing**
```bash
python -c "import nltk; nltk.download('all')"
```

**Memory Issues with Large Filings**
- Reduce batch size in settings
- Limit filings per company
- Increase system memory allocation

### Performance Optimization

- Use SSD storage for faster text processing
- Increase `MAX_CONCURRENT_REQUESTS` cautiously
- Consider using PostgreSQL connection pooling
- Monitor rate limits and adjust delays

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- SEC EDGAR API for providing access to public filings
- Supabase for database infrastructure
- HuggingFace Transformers for NLP models
- Rich library for beautiful CLI output

## 📞 Support

For questions, issues, or feature requests:
1. Check the [Issues](link-to-issues) page
2. Review the troubleshooting section
3. Create a new issue with detailed information

---

**Built with ❤️ for intelligent investment analysis**