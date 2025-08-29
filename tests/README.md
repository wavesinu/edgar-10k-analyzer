# EDGAR 10-K Analyzer - Comprehensive Test Suite

This directory contains a comprehensive test suite for the EDGAR 10-K analyzer project, covering all major components with unit tests, integration tests, and end-to-end testing scenarios.

## Test Structure

```
tests/
├── conftest.py                     # Pytest configuration and shared fixtures
├── test_api_edgar_client.py        # EDGAR API client tests
├── test_database_operations.py     # Database and Supabase tests  
├── test_nlp_processing.py          # NLP and text processing tests
├── test_ai_analysis.py             # AI analysis and OpenAI tests
├── test_e2e_pipeline.py           # End-to-end pipeline tests
├── test_fixtures.py               # Additional test utilities and fixtures
├── test_integration_live.py       # Existing integration tests
├── test_sec_api_connection.py     # Existing SEC API tests
└── test_supabase_connection.py    # Existing Supabase tests
```

## Test Categories

### 🧪 Unit Tests (`@unit_test`)
- **Fast execution** (< 1 second per test)
- **No external dependencies** (mocked APIs, databases)
- **High coverage** of core business logic
- **Components tested:**
  - EDGAR API client parsing and data extraction
  - Database operations with mocked Supabase
  - NLP text processing and analysis
  - AI prompt construction and response parsing
  - Data validation and schema compliance

### 🔗 Integration Tests (`@integration_test`)
- **Medium execution time** (1-10 seconds per test)
- **Real external services** (with proper environment setup)
- **End-to-end workflows** within single components
- **Components tested:**
  - Actual Supabase database operations
  - Real NLTK data processing
  - Configuration management
  - Data flow between components

### 🚀 End-to-End Tests (`@e2e_test`)
- **Slower execution** (10+ seconds per test)
- **Complete pipeline flows** with real data
- **Full system integration** testing
- **Scenarios tested:**
  - Complete company analysis pipeline
  - Data crawling to database storage
  - Multi-company processing workflows
  - Error recovery and resilience

### 🌐 API Tests (`@requires_api`)
- **Live API integration** testing
- **Rate limiting compliance** verification
- **Real data validation** with external services
- **Services tested:**
  - SEC.gov EDGAR API
  - OpenAI GPT-4 API
  - Real HTML parsing and section extraction

### 🗃️ Database Tests (`@requires_db`)
- **Real database operations** with cleanup
- **Data integrity** verification
- **Performance testing** under load
- **CRUD operations** with actual Supabase instance

## Quick Start

### 1. Install Test Dependencies

```bash
# Install core testing packages
pip install pytest pytest-asyncio pytest-cov pytest-mock pytest-html

# Or use the Makefile
make install-test-deps
```

### 2. Set Up Test Environment

```bash
# Create test environment configuration
make setup-test-env

# Edit .env.test with your credentials
cp .env.test .env  # or configure environment variables
```

### 3. Run Tests

```bash
# Quick unit tests (recommended for development)
make test-unit

# All tests with coverage
make test-all

# Integration tests (requires environment setup)
RUN_INTEGRATION_TESTS=1 make test-integration

# Live API tests (requires API keys)
RUN_LIVE_API_TESTS=1 make test-api
```

## Test Execution Modes

### Development Mode (Fast)
```bash
# Run only fast tests during development
make test-fast

# Or use the test runner
python test_runner.py fast -v
```

### CI/CD Mode
```bash
# Comprehensive testing for CI
make ci-test

# Unit tests only for quick feedback
make ci-test-unit
```

### Full Integration Mode
```bash
# Complete test suite with all services
export RUN_INTEGRATION_TESTS=1
export RUN_LIVE_API_TESTS=1
make test-all
```

## Environment Variables

### Required for Integration Tests
```bash
export RUN_INTEGRATION_TESTS=1
export SUPABASE_URL="your-supabase-url"
export SUPABASE_KEY="your-supabase-key"
export SUPABASE_SERVICE_KEY="your-service-key"
```

### Required for API Tests
```bash
export RUN_LIVE_API_TESTS=1
export OPENAI_API_KEY="your-openai-key"
export USER_AGENT="YourApp your-email@example.com"
```

### Optional Configuration
```bash
export DATA_DIR="test_data"
export LOGS_DIR="test_logs"
export LOG_LEVEL="DEBUG"
```

## Test Data and Fixtures

### Shared Fixtures (conftest.py)
- **mock_supabase_client**: Mocked Supabase for unit tests
- **mock_openai_client**: Mocked OpenAI for unit tests  
- **sample_company_data**: Realistic company test data
- **sample_filing_data**: 10-K filing test data
- **sample_qualitative_sections**: Extracted section content
- **mock_sec_api_responses**: SEC API response mocks

### Test Data Generator (test_fixtures.py)
```python
from tests.test_fixtures import TestDataGenerator

generator = TestDataGenerator()
companies = generator.generate_company_data(count=10)
filings = generator.generate_filing_data(company_id="test", count=5)
```

### Performance Monitoring
```python
from tests.test_fixtures import PerformanceMonitor

with PerformanceMonitor() as monitor:
    # Run operation
    result = await some_operation()
    monitor.assert_performance("operation", max_duration=2.0)
```

## Coverage Requirements

- **Minimum Coverage**: 75% overall
- **Unit Test Coverage**: 90%+ for core components
- **Integration Coverage**: Key workflows and error paths
- **Reports Generated**: 
  - Terminal output with missing lines
  - HTML report in `htmlcov/`
  - XML report for CI integration

## Performance Benchmarks

### Unit Tests
- **Individual test**: < 1 second
- **Full unit suite**: < 30 seconds  
- **Memory usage**: < 500MB

### Integration Tests
- **Database operations**: < 5 seconds each
- **API calls**: < 10 seconds each (with rate limiting)
- **Full integration suite**: < 5 minutes

### End-to-End Tests
- **Single company pipeline**: < 2 minutes
- **Multi-company pipeline**: < 10 minutes
- **Full E2E suite**: < 15 minutes

## Test Patterns and Best Practices

### 1. Test Structure (AAA Pattern)
```python
@pytest.mark.asyncio
async def test_analyze_sentiment_positive():
    # Arrange
    analyzer = QualitativeAnalyzer()
    positive_text = "Excellent performance with strong growth."
    
    # Act
    result = analyzer.analyze_sentiment(positive_text)
    
    # Assert
    assert result["overall_sentiment"] > 0.5
    assert result["sentiment_label"] == "positive"
```

### 2. Mock External Dependencies
```python
@patch("src.database.connection.create_client")
async def test_with_mocked_database(mock_supabase):
    mock_supabase.return_value = MagicMock()
    # Test implementation with mocked database
```

### 3. Test Error Scenarios
```python
async def test_handle_api_error():
    client.session.get.side_effect = aiohttp.ClientError("Network error")
    result = await client.get_company_submissions("invalid")
    assert result is None  # Graceful error handling
```

### 4. Performance Testing
```python
async def test_processing_performance():
    start_time = time.time()
    result = await process_large_document(large_text)
    duration = time.time() - start_time
    assert duration < 5.0  # Should complete within 5 seconds
```

## Continuous Integration

### GitHub Actions Integration
```yaml
- name: Run Tests
  run: |
    make ci-test
    
- name: Upload Coverage
  uses: codecov/codecov-action@v3
  with:
    file: ./coverage.xml
```

### Test Result Artifacts
- `test_results.xml`: JUnit format for CI integration
- `htmlcov/`: HTML coverage reports
- `test_report.html`: Comprehensive test report

## Troubleshooting

### Common Issues

1. **Import Errors**
   ```bash
   # Ensure PYTHONPATH includes src directory
   export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
   ```

2. **Environment Variable Issues**
   ```bash
   # Validate test environment
   python test_runner.py validate
   ```

3. **Database Connection Issues**
   ```bash
   # Test Supabase connectivity
   python tests/test_supabase_connection.py
   ```

4. **API Rate Limiting**
   ```bash
   # Run with delays between API calls
   pytest -m requires_api --tb=short -v
   ```

### Debug Mode
```bash
# Run tests with detailed output and no capture
pytest -v -s --tb=long

# Run specific test with debugging
pytest tests/test_api_edgar_client.py::TestEdgarClientUnit::test_get_company_submissions_success -v -s
```

## Contributing to Tests

### Adding New Tests
1. Follow existing patterns and naming conventions
2. Use appropriate test markers (`@unit_test`, `@integration_test`, etc.)
3. Include both positive and negative test cases
4. Add docstrings explaining test purpose
5. Update test fixtures if needed

### Test Review Checklist
- [ ] Tests follow AAA pattern
- [ ] Appropriate mocking for unit tests
- [ ] Error scenarios covered
- [ ] Performance considerations addressed
- [ ] Documentation updated
- [ ] CI integration verified

## Resources

- [Pytest Documentation](https://docs.pytest.org/)
- [pytest-asyncio Guide](https://pytest-asyncio.readthedocs.io/)
- [Coverage.py Documentation](https://coverage.readthedocs.io/)
- [Testing Best Practices](https://docs.python-guide.org/writing/tests/)