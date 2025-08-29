#!/usr/bin/env python3
"""Comprehensive test runner for EDGAR 10-K analyzer with multiple execution modes."""

import os
import sys
import argparse
import subprocess
from pathlib import Path
from typing import List, Optional


class TestRunner:
    """Advanced test runner with multiple execution modes and reporting."""
    
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.test_dir = self.project_root / "tests"
        
    def run_unit_tests(self, verbose: bool = False, coverage: bool = True) -> int:
        """Run unit tests only."""
        print("🧪 Running Unit Tests...")
        cmd = ["pytest", "-m", "unit"]
        
        if verbose:
            cmd.extend(["-v", "-s"])
        
        if coverage:
            cmd.extend([
                "--cov=src",
                "--cov-report=term-missing",
                "--cov-report=html:htmlcov/unit"
            ])
        
        return subprocess.call(cmd, cwd=self.project_root)
    
    def run_integration_tests(self, verbose: bool = False) -> int:
        """Run integration tests (requires environment setup)."""
        print("🔗 Running Integration Tests...")
        
        # Check for required environment variables
        required_vars = ["RUN_INTEGRATION_TESTS"]
        missing_vars = [var for var in required_vars if not os.environ.get(var)]
        
        if missing_vars:
            print(f"⚠️  Integration tests require environment variables: {', '.join(missing_vars)}")
            print("Set RUN_INTEGRATION_TESTS=1 to enable integration tests")
            return 1
        
        cmd = ["pytest", "-m", "integration"]
        
        if verbose:
            cmd.extend(["-v", "-s"])
        
        return subprocess.call(cmd, cwd=self.project_root)
    
    def run_e2e_tests(self, verbose: bool = False) -> int:
        """Run end-to-end tests (requires full environment)."""
        print("🚀 Running End-to-End Tests...")
        
        # Check for required environment variables
        required_vars = ["RUN_INTEGRATION_TESTS", "SUPABASE_URL", "SUPABASE_KEY"]
        missing_vars = [var for var in required_vars if not os.environ.get(var)]
        
        if missing_vars:
            print(f"⚠️  E2E tests require environment variables: {', '.join(missing_vars)}")
            return 1
        
        cmd = ["pytest", "-m", "e2e"]
        
        if verbose:
            cmd.extend(["-v", "-s"])
        
        return subprocess.call(cmd, cwd=self.project_root)
    
    def run_api_tests(self, verbose: bool = False) -> int:
        """Run tests that require external API access."""
        print("🌐 Running API Tests...")
        
        required_vars = ["RUN_LIVE_API_TESTS", "OPENAI_API_KEY", "USER_AGENT"]
        missing_vars = [var for var in required_vars if not os.environ.get(var)]
        
        if missing_vars:
            print(f"⚠️  API tests require environment variables: {', '.join(missing_vars)}")
            print("Set RUN_LIVE_API_TESTS=1 to enable live API tests")
            return 1
        
        cmd = ["pytest", "-m", "requires_api"]
        
        if verbose:
            cmd.extend(["-v", "-s"])
        
        return subprocess.call(cmd, cwd=self.project_root)
    
    def run_all_tests(self, verbose: bool = False, coverage: bool = True) -> int:
        """Run all tests with comprehensive reporting."""
        print("🎯 Running All Tests...")
        
        cmd = ["pytest"]
        
        if verbose:
            cmd.extend(["-v", "-s"])
        
        if coverage:
            cmd.extend([
                "--cov=src",
                "--cov-report=term-missing",
                "--cov-report=html:htmlcov",
                "--cov-report=xml",
                "--cov-fail-under=75"
            ])
        
        return subprocess.call(cmd, cwd=self.project_root)
    
    def run_fast_tests(self, verbose: bool = False) -> int:
        """Run fast tests only (excludes slow, API, and DB tests)."""
        print("⚡ Running Fast Tests...")
        
        cmd = ["pytest", "-m", "not slow and not requires_api and not requires_db"]
        
        if verbose:
            cmd.extend(["-v", "-s"])
        
        return subprocess.call(cmd, cwd=self.project_root)
    
    def run_specific_test(self, test_path: str, verbose: bool = False) -> int:
        """Run a specific test file or test function."""
        print(f"🎯 Running Specific Test: {test_path}")
        
        cmd = ["pytest", test_path]
        
        if verbose:
            cmd.extend(["-v", "-s"])
        
        return subprocess.call(cmd, cwd=self.project_root)
    
    def run_with_profile(self, verbose: bool = False) -> int:
        """Run tests with performance profiling."""
        print("📊 Running Tests with Performance Profiling...")
        
        try:
            import pytest_profiling  # noqa
        except ImportError:
            print("⚠️  pytest-profiling not installed. Install with: pip install pytest-profiling")
            return 1
        
        cmd = ["pytest", "--profile"]
        
        if verbose:
            cmd.extend(["-v", "-s"])
        
        return subprocess.call(cmd, cwd=self.project_root)
    
    def generate_test_report(self) -> int:
        """Generate comprehensive test report."""
        print("📋 Generating Comprehensive Test Report...")
        
        cmd = [
            "pytest",
            "--html=test_report.html",
            "--self-contained-html",
            "--cov=src",
            "--cov-report=html:htmlcov",
            "--junit-xml=test_results.xml"
        ]
        
        return subprocess.call(cmd, cwd=self.project_root)
    
    def validate_test_environment(self) -> bool:
        """Validate test environment setup."""
        print("✅ Validating Test Environment...")
        
        # Check Python version
        if sys.version_info < (3, 8):
            print("❌ Python 3.8+ required")
            return False
        
        print(f"✅ Python version: {sys.version}")
        
        # Check required packages
        required_packages = [
            "pytest", "pytest-asyncio", "pytest-cov", "pytest-mock"
        ]
        
        for package in required_packages:
            try:
                __import__(package.replace("-", "_"))
                print(f"✅ {package} installed")
            except ImportError:
                print(f"❌ {package} not installed")
                return False
        
        # Check test directory structure
        if not self.test_dir.exists():
            print("❌ Tests directory not found")
            return False
        
        test_files = list(self.test_dir.glob("test_*.py"))
        print(f"✅ Found {len(test_files)} test files")
        
        return True
    
    def clean_test_artifacts(self):
        """Clean test artifacts and cache files."""
        print("🧹 Cleaning Test Artifacts...")
        
        # Remove pytest cache
        pytest_cache = self.project_root / ".pytest_cache"
        if pytest_cache.exists():
            import shutil
            shutil.rmtree(pytest_cache)
            print("✅ Removed .pytest_cache")
        
        # Remove coverage files
        coverage_files = [".coverage", "htmlcov", "test_report.html", "test_results.xml"]
        for file_path in coverage_files:
            full_path = self.project_root / file_path
            if full_path.exists():
                if full_path.is_dir():
                    import shutil
                    shutil.rmtree(full_path)
                else:
                    full_path.unlink()
                print(f"✅ Removed {file_path}")
        
        # Remove __pycache__ directories
        for pycache in self.project_root.rglob("__pycache__"):
            import shutil
            shutil.rmtree(pycache)
        
        print("✅ Test artifacts cleaned")


def main():
    """Main test runner entry point."""
    parser = argparse.ArgumentParser(
        description="Comprehensive test runner for EDGAR 10-K analyzer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test_runner.py unit                    # Run unit tests only
  python test_runner.py integration             # Run integration tests
  python test_runner.py e2e                     # Run end-to-end tests
  python test_runner.py api                     # Run API tests
  python test_runner.py all                     # Run all tests
  python test_runner.py fast                    # Run fast tests only
  python test_runner.py specific tests/test_api_edgar_client.py
  python test_runner.py validate                # Validate test environment
  python test_runner.py clean                   # Clean test artifacts
  python test_runner.py report                  # Generate test report
        """
    )
    
    parser.add_argument(
        "command",
        choices=[
            "unit", "integration", "e2e", "api", "all", "fast", 
            "specific", "profile", "validate", "clean", "report"
        ],
        help="Test command to execute"
    )
    
    parser.add_argument(
        "target",
        nargs="?",
        help="Target test file or function (for specific command)"
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output"
    )
    
    parser.add_argument(
        "--no-coverage",
        action="store_true",
        help="Disable coverage reporting"
    )
    
    args = parser.parse_args()
    
    runner = TestRunner()
    
    # Command routing
    if args.command == "unit":
        return runner.run_unit_tests(args.verbose, not args.no_coverage)
    elif args.command == "integration":
        return runner.run_integration_tests(args.verbose)
    elif args.command == "e2e":
        return runner.run_e2e_tests(args.verbose)
    elif args.command == "api":
        return runner.run_api_tests(args.verbose)
    elif args.command == "all":
        return runner.run_all_tests(args.verbose, not args.no_coverage)
    elif args.command == "fast":
        return runner.run_fast_tests(args.verbose)
    elif args.command == "specific":
        if not args.target:
            print("❌ Target test file/function required for specific command")
            return 1
        return runner.run_specific_test(args.target, args.verbose)
    elif args.command == "profile":
        return runner.run_with_profile(args.verbose)
    elif args.command == "validate":
        return 0 if runner.validate_test_environment() else 1
    elif args.command == "clean":
        runner.clean_test_artifacts()
        return 0
    elif args.command == "report":
        return runner.generate_test_report()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())