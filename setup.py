"""Setup script for EDGAR 10-K Analyzer."""

import subprocess
import sys
import os
from pathlib import Path

def install_requirements():
    """Install required Python packages."""
    print("📦 Installing Python dependencies...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✅ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error installing dependencies: {e}")
        return False

def download_nltk_data():
    """Download required NLTK data."""
    print("📚 Downloading NLTK data...")
    try:
        import nltk
        nltk_downloads = ['punkt', 'stopwords', 'wordnet', 'averaged_perceptron_tagger']
        
        for item in nltk_downloads:
            try:
                nltk.data.find(f'tokenizers/{item}')
            except LookupError:
                print(f"  Downloading {item}...")
                nltk.download(item, quiet=True)
        
        print("✅ NLTK data downloaded successfully")
        return True
    except Exception as e:
        print(f"❌ Error downloading NLTK data: {e}")
        return False

def check_env_file():
    """Check if .env file exists and has required variables."""
    env_file = Path(".env")
    
    if not env_file.exists():
        print("❌ .env file not found. Please copy .env.example to .env and configure your API keys.")
        return False
    
    print("✅ .env file found")
    
    # Check for critical environment variables
    required_vars = [
        "SUPABASE_URL", "SUPABASE_KEY", "SUPABASE_SERVICE_KEY",
        "OPENAI_API_KEY", "USER_AGENT"
    ]
    
    missing_vars = []
    with open(env_file, 'r') as f:
        env_content = f.read()
        
        for var in required_vars:
            if f"{var}=" not in env_content or f"{var}=your-" in env_content or f"{var}=YourCompany" in env_content:
                missing_vars.append(var)
    
    if missing_vars:
        print(f"⚠️  Please configure these environment variables in .env:")
        for var in missing_vars:
            print(f"   - {var}")
        return False
    
    print("✅ Environment variables configured")
    return True

def create_directories():
    """Create necessary directories."""
    print("📁 Creating directories...")
    directories = ["data", "logs", "reports"]
    
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
    
    print("✅ Directories created")
    return True

def main():
    """Main setup function."""
    print("🚀 EDGAR 10-K Analyzer Setup")
    print("=" * 50)
    
    # Change to project directory
    project_dir = Path(__file__).parent
    os.chdir(project_dir)
    
    success_steps = 0
    total_steps = 4
    
    # Step 1: Create directories
    if create_directories():
        success_steps += 1
    
    # Step 2: Install requirements
    if install_requirements():
        success_steps += 1
    
    # Step 3: Download NLTK data
    if download_nltk_data():
        success_steps += 1
    
    # Step 4: Check environment configuration
    if check_env_file():
        success_steps += 1
    
    print("\n" + "=" * 50)
    print(f"Setup completed: {success_steps}/{total_steps} steps successful")
    
    if success_steps == total_steps:
        print("🎉 Setup completed successfully!")
        print("\nNext steps:")
        print("1. Configure your Supabase database (run the SQL schema)")
        print("2. Test the installation: python main.py status")
        print("3. Initialize companies: python main.py init-companies")
        print("4. Start the AI chat: python main.py chat")
        
    else:
        print("⚠️  Setup incomplete. Please address the issues above.")
    
    return success_steps == total_steps

if __name__ == "__main__":
    main()