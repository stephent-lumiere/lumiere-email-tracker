#!/bin/bash

# Lumiere Email Tracker - Developer Setup Script
# Run this script to set up your local development environment

set -e

echo "=============================================="
echo "  Lumiere Email Tracker - Developer Setup"
echo "=============================================="
echo ""

# Check for required tools
echo "Checking requirements..."

if ! command -v git &> /dev/null; then
    echo "ERROR: git is not installed. Please install git first."
    exit 1
fi

if ! command -v python3 &> /dev/null; then
    echo "ERROR: python3 is not installed. Please install Python 3.11+ first."
    exit 1
fi

if ! command -v pip3 &> /dev/null && ! command -v pip &> /dev/null; then
    echo "ERROR: pip is not installed. Please install pip first."
    exit 1
fi

echo "All requirements met!"
echo ""

# Check if we're in the right directory
if [ ! -f "app.py" ]; then
    echo "You're not in the lumiere-email-tracker directory."
    echo ""
    read -p "Clone the repo now? (y/n): " clone_repo
    if [ "$clone_repo" = "y" ]; then
        git clone https://github.com/stephent-lumiere/lumiere-email-tracker.git
        cd lumiere-email-tracker
        echo "Cloned and moved into lumiere-email-tracker/"
    else
        echo "Please run this script from the lumiere-email-tracker directory."
        exit 1
    fi
fi

echo ""
echo "=============================================="
echo "  Step 1: Create .env file"
echo "=============================================="
echo ""

if [ -f ".env" ]; then
    read -p ".env already exists. Overwrite? (y/n): " overwrite_env
    if [ "$overwrite_env" != "y" ]; then
        echo "Keeping existing .env"
    else
        rm .env
    fi
fi

if [ ! -f ".env" ]; then
    echo "Ask your team lead for these values (check Slack or 1Password)"
    echo ""

    read -p "SUPABASE_URL: " supabase_url
    read -p "SUPABASE_KEY: " supabase_key
    read -p "GITHUB_TOKEN: " github_token

    cat > .env << EOF
# Supabase credentials
SUPABASE_URL=$supabase_url
SUPABASE_KEY=$supabase_key

# Google credentials file path
GOOGLE_CREDENTIALS_FILE=credentials.json

# GitHub token for triggering workflows
GITHUB_TOKEN=$github_token
EOF

    echo ""
    echo ".env file created!"
fi

echo ""
echo "=============================================="
echo "  Step 2: Create credentials.json"
echo "=============================================="
echo ""

if [ -f "credentials.json" ]; then
    read -p "credentials.json already exists. Overwrite? (y/n): " overwrite_creds
    if [ "$overwrite_creds" != "y" ]; then
        echo "Keeping existing credentials.json"
    else
        rm credentials.json
    fi
fi

if [ ! -f "credentials.json" ]; then
    echo "Ask your team lead for the Google Service Account JSON file."
    echo "They can send it via Slack DM or 1Password."
    echo ""
    echo "Option 1: Paste the JSON content below (then press Ctrl+D when done)"
    echo "Option 2: Press Ctrl+C and manually copy credentials.json to this folder"
    echo ""
    read -p "Paste JSON now? (y/n): " paste_json

    if [ "$paste_json" = "y" ]; then
        echo "Paste the JSON content, then press Ctrl+D on a new line:"
        cat > credentials.json
        echo ""
        echo "credentials.json created!"
    else
        echo ""
        echo "Please manually copy credentials.json to: $(pwd)/"
    fi
fi

echo ""
echo "=============================================="
echo "  Step 3: Install Python dependencies"
echo "=============================================="
echo ""

pip3 install -r requirements.txt || pip install -r requirements.txt

echo ""
echo "=============================================="
echo "  Step 4: Verify setup"
echo "=============================================="
echo ""

# Check if files exist
if [ -f ".env" ] && [ -f "credentials.json" ]; then
    echo "All files present!"
else
    echo "WARNING: Some files are missing:"
    [ ! -f ".env" ] && echo "  - .env (required)"
    [ ! -f "credentials.json" ] && echo "  - credentials.json (required for local Gmail testing)"
fi

echo ""
echo "=============================================="
echo "  Setup Complete!"
echo "=============================================="
echo ""
echo "To start developing with Claude Code:"
echo ""
echo "  STEP 1: Install Homebrew (if you don't have it)"
echo "     Open Terminal and run:"
echo "     /bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
echo ""
echo "  STEP 2: Install Node.js"
echo "     brew install node"
echo ""
echo "  STEP 3: Install Claude Code"
echo "     npm install -g @anthropic-ai/claude-code"
echo ""
echo "  STEP 4: Start Claude Code in this directory"
echo "     claude"
echo ""
echo "  STEP 5: Describe what you want to change in plain English!"
echo ""
echo "  NOTE: First time running 'claude' will ask you to log in"
echo "        with your Anthropic account (Claude Pro/Max) or API key."
echo ""
echo "To run the dashboard locally:"
echo "     streamlit run app.py"
echo ""
echo "To run the tracker manually:"
echo "     python3 tracker_supabase.py --user email@domain.com --backfill"
echo ""
echo "=============================================="
