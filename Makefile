# Makefile for EDGAR 10-K Analyzer Test Suite
# Provides convenient commands for running different types of tests

.PHONY: test test-unit test-integration test-e2e test-api test-all test-fast test-coverage test-report clean-test validate-test help

# Default target
help:
	@echo "EDGAR 10-K Analyzer Test Suite"
	@echo "==============================="
	@echo ""
	@echo "Available test commands:"
	@echo "  test-unit        - Run unit tests only (fast, no external dependencies)"
	@echo "  test-integration - Run integration tests (requires test environment)"
	@echo "  test-e2e         - Run end-to-end tests (requires full environment)"
	@echo "  test-api         - Run API tests (requires live API access)"
	@echo "  test-all         - Run all tests with coverage reporting"
	@echo "  test-fast        - Run fast tests only (excludes slow/API/DB tests)"
	@echo "  test-coverage    - Run tests with detailed coverage reporting"
	@echo "  test-report      - Generate comprehensive HTML test report"
	@echo "  validate-test    - Validate test environment setup"
	@echo "  clean-test       - Clean test artifacts and cache files"
	@echo ""
	@echo "Environment Variables:"
	@echo "  RUN_INTEGRATION_TESTS=1  - Enable integration tests"
	@echo "  RUN_LIVE_API_TESTS=1     - Enable live API tests"
	@echo "  SUPABASE_URL             - Supabase database URL"
	@echo "  SUPABASE_KEY             - Supabase API key"
	@echo "  OPENAI_API_KEY           - OpenAI API key"
	@echo "  USER_AGENT               - SEC-compliant User-Agent header"
	@echo ""
	@echo "Examples:"
	@echo "  make test-unit                    # Quick unit tests"
	@echo "  make test-all                     # Full test suite"
	@echo "  RUN_INTEGRATION_TESTS=1 make test-integration"
	@echo ""

# Basic test commands using pytest directly
test: test-unit

test-unit:
	@echo "🧪 Running Unit Tests..."
	pytest -m unit -v

test-integration:
	@echo "🔗 Running Integration Tests..."
	@if [ -z "$$RUN_INTEGRATION_TESTS" ]; then \
		echo "⚠️  Set RUN_INTEGRATION_TESTS=1 to enable integration tests"; \
		exit 1; \
	fi
	pytest -m integration -v

test-e2e:
	@echo "🚀 Running End-to-End Tests..."
	@if [ -z "$$RUN_INTEGRATION_TESTS" ]; then \
		echo "⚠️  Set RUN_INTEGRATION_TESTS=1 to enable E2E tests"; \
		exit 1; \
	fi
	pytest -m e2e -v

test-api:
	@echo "🌐 Running API Tests..."
	@if [ -z "$$RUN_LIVE_API_TESTS" ]; then \
		echo "⚠️  Set RUN_LIVE_API_TESTS=1 to enable live API tests"; \
		exit 1; \
	fi
	pytest -m requires_api -v

test-all:
	@echo "🎯 Running All Tests with Coverage..."
	pytest -v --cov=src --cov-report=term-missing --cov-report=html:htmlcov

test-fast:
	@echo "⚡ Running Fast Tests..."
	pytest -m "not slow and not requires_api and not requires_db" -v

test-coverage:
	@echo "📊 Running Tests with Detailed Coverage..."
	pytest --cov=src --cov-report=term-missing --cov-report=html:htmlcov --cov-report=xml --cov-fail-under=75

test-report:
	@echo "📋 Generating Comprehensive Test Report..."
	pytest --html=test_report.html --self-contained-html --cov=src --cov-report=html:htmlcov --junit-xml=test_results.xml

# Advanced test commands using the test runner
test-unit-advanced:
	python test_runner.py unit -v

test-integration-advanced:
	python test_runner.py integration -v

test-e2e-advanced:
	python test_runner.py e2e -v

test-api-advanced:
	python test_runner.py api -v

test-all-advanced:
	python test_runner.py all -v

test-profile:
	python test_runner.py profile -v

# Utility commands
validate-test:
	@echo "✅ Validating Test Environment..."
	python test_runner.py validate

clean-test:
	@echo "🧹 Cleaning Test Artifacts..."
	python test_runner.py clean

# Development commands
install-test-deps:
	@echo "📦 Installing Test Dependencies..."
	pip install pytest pytest-asyncio pytest-cov pytest-mock pytest-html pytest-xdist

setup-test-env:
	@echo "🔧 Setting up Test Environment..."
	@echo "Creating .env.test file with test configurations..."
	@cat > .env.test << EOF
# Test Environment Configuration
SUPABASE_URL=https://test.supabase.co
SUPABASE_KEY=test-key
SUPABASE_SERVICE_KEY=test-service-key
OPENAI_API_KEY=sk-test-12345
USER_AGENT=EDGAR-Analyzer test@example.com
DATA_DIR=test_data
LOGS_DIR=test_logs
RUN_INTEGRATION_TESTS=0
RUN_LIVE_API_TESTS=0
EOF
	@echo "✅ Test environment configuration created in .env.test"
	@echo "Edit .env.test with your actual test credentials to enable integration tests"

# Continuous Integration commands
ci-test:
	@echo "🤖 Running CI Test Suite..."
	pytest -v --cov=src --cov-report=xml --junit-xml=test_results.xml

ci-test-unit:
	@echo "🤖 Running CI Unit Tests..."
	pytest -m unit -v --junit-xml=unit_test_results.xml

ci-test-integration:
	@echo "🤖 Running CI Integration Tests..."
	@if [ "$$CI_ENABLE_INTEGRATION" = "1" ]; then \
		pytest -m integration -v --junit-xml=integration_test_results.xml; \
	else \
		echo "Integration tests disabled in CI (set CI_ENABLE_INTEGRATION=1 to enable)"; \
	fi

# Database-specific test commands
test-database:
	@echo "🗃️  Running Database Tests..."
	pytest tests/test_database_operations.py tests/test_supabase_connection.py -v

test-api-clients:
	@echo "🌐 Running API Client Tests..."
	pytest tests/test_api_edgar_client.py tests/test_sec_api_connection.py -v

test-nlp:
	@echo "🧠 Running NLP Processing Tests..."
	pytest tests/test_nlp_processing.py -v

test-ai:
	@echo "🤖 Running AI Analysis Tests..."
	pytest tests/test_ai_analysis.py -v

# Performance and load testing
test-performance:
	@echo "🏃 Running Performance Tests..."
	pytest -m slow -v --tb=short

test-load:
	@echo "📈 Running Load Tests..."
	pytest -k "performance or load" -v

# Test data management
generate-test-data:
	@echo "📊 Generating Test Data..."
	python -c "from tests.test_fixtures import TestDataGenerator; print('Test data generation complete')"

# Security testing
test-security:
	@echo "🔒 Running Security Tests..."
	pytest -k "security or auth" -v

# Documentation and reporting
test-docs:
	@echo "📚 Generating Test Documentation..."
	pytest --collect-only --quiet | grep "test_" > test_inventory.txt
	@echo "✅ Test inventory generated in test_inventory.txt"

# Watch mode for development
test-watch:
	@echo "👀 Running Tests in Watch Mode..."
	@echo "Install pytest-watch: pip install pytest-watch"
	ptw -- -v

# Parallel test execution
test-parallel:
	@echo "⚡ Running Tests in Parallel..."
	pytest -n auto -v

# Quality assurance
test-quality:
	@echo "🎯 Running Quality Checks..."
	pytest --strict-markers --strict-config -v
	flake8 tests/
	mypy tests/ --ignore-missing-imports