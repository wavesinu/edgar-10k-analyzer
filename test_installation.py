"""Test installation and basic functionality."""

import sys
import os
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

def test_imports():
    """Test if all required modules can be imported."""
    print("🧪 Testing module imports...")
    
    test_modules = [
        ("config.settings", "Configuration"),
        ("src.api.company_mapping", "Company mapping"),
        ("src.api.edgar_client", "EDGAR client"),
        ("src.database.connection", "Database connection"),
        ("src.database.schema", "Database schema"),
        ("src.nlp.text_processor", "Text processor"),
        ("src.nlp.qualitative_analyzer", "Qualitative analyzer"),
        ("src.nlp.investment_scorer", "Investment scorer"),
        ("src.llm.openai_client", "OpenAI client"),
        ("src.llm.investment_advisor", "Investment advisor"),
        ("src.llm.chat_interface", "Chat interface"),
        ("src.pipeline.orchestrator", "Pipeline orchestrator"),
        ("src.reporting.dashboard", "Dashboard"),
    ]
    
    successful_imports = 0
    failed_imports = []
    
    for module_name, description in test_modules:
        try:
            __import__(module_name)
            print(f"  ✅ {description}")
            successful_imports += 1
        except ImportError as e:
            print(f"  ❌ {description}: {e}")
            failed_imports.append((module_name, str(e)))
        except Exception as e:
            print(f"  ⚠️  {description}: {e}")
            failed_imports.append((module_name, str(e)))
    
    print(f"\nImport Results: {successful_imports}/{len(test_modules)} successful")
    
    if failed_imports:
        print("\nFailed imports:")
        for module, error in failed_imports:
            print(f"  - {module}: {error}")
    
    return len(failed_imports) == 0

def test_environment():
    """Test environment configuration."""
    print("\n🔧 Testing environment configuration...")
    
    try:
        from config.settings import settings
        
        # Test critical settings
        critical_settings = [
            ("supabase_url", "Supabase URL"),
            ("supabase_key", "Supabase Key"),
            ("openai_api_key", "OpenAI API Key"),
            ("user_agent", "User Agent"),
        ]
        
        config_issues = []
        
        for attr, description in critical_settings:
            try:
                value = getattr(settings, attr)
                if not value or "your-" in str(value) or "YourCompany" in str(value):
                    config_issues.append(f"{description} not configured")
                    print(f"  ⚠️  {description}: Not configured")
                else:
                    print(f"  ✅ {description}: Configured")
            except AttributeError:
                config_issues.append(f"{description} missing")
                print(f"  ❌ {description}: Missing")
        
        if config_issues:
            print(f"\nConfiguration issues:")
            for issue in config_issues:
                print(f"  - {issue}")
            return False
        
        return True
        
    except Exception as e:
        print(f"  ❌ Error loading settings: {e}")
        return False

def test_database_connection():
    """Test database connection (without actually connecting)."""
    print("\n🗄️  Testing database setup...")
    
    try:
        from src.database.connection import db_client
        from src.database.schema import CREATE_TABLES_SQL
        
        print("  ✅ Database modules loaded")
        print("  ✅ SQL schema available")
        print("  ℹ️  Database connection will be tested when first used")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Database setup error: {e}")
        return False

def test_ai_modules():
    """Test AI/LLM modules."""
    print("\n🤖 Testing AI modules...")
    
    try:
        from src.llm.openai_client import OpenAIFinancialAnalyst
        from src.llm.investment_advisor import LLMInvestmentAdvisor
        
        print("  ✅ OpenAI client module loaded")
        print("  ✅ Investment advisor module loaded")
        print("  ℹ️  API functionality will be tested when first used")
        
        return True
        
    except Exception as e:
        print(f"  ❌ AI modules error: {e}")
        return False

def main():
    """Main test function."""
    print("🧪 EDGAR 10-K Analyzer Installation Test")
    print("=" * 50)
    
    tests = [
        ("Module Imports", test_imports),
        ("Environment Config", test_environment),
        ("Database Setup", test_database_connection),
        ("AI Modules", test_ai_modules),
    ]
    
    passed_tests = 0
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed_tests += 1
        except Exception as e:
            print(f"  ❌ {test_name} failed with error: {e}")
    
    print("\n" + "=" * 50)
    print(f"Test Results: {passed_tests}/{len(tests)} tests passed")
    
    if passed_tests == len(tests):
        print("🎉 All tests passed! Installation looks good.")
        print("\nReady to use:")
        print("  python main.py chat          # Start AI chat")
        print("  python main.py status        # Check system status")
        print("  python main.py init-companies # Initialize company DB")
        
    elif passed_tests >= len(tests) - 1:
        print("⚠️  Installation mostly complete. Minor issues detected.")
        print("You can probably proceed, but check the warnings above.")
        
    else:
        print("❌ Installation has significant issues. Please fix before proceeding.")
    
    return passed_tests >= len(tests) - 1

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)