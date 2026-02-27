#!/bin/bash
# ══════════════════════════════════════════════════════════
# MIIM — One-Click GitHub Push & Deploy Script
# ══════════════════════════════════════════════════════════
#
# This script will:
#   1. Initialize a Git repo in this folder
#   2. Create a GitHub repository called "miim-platform"
#   3. Push all code to GitHub
#   4. Open the Streamlit Cloud deploy page in your browser
#
# Prerequisites:
#   - Git installed (git-scm.com)
#   - GitHub CLI installed (cli.github.com)
#
# Usage:
#   chmod +x setup_and_deploy.sh
#   ./setup_and_deploy.sh
# ══════════════════════════════════════════════════════════

set -e  # Stop on any error

echo ""
echo "🏭 MIIM — Morocco Industry Intelligence Monitor"
echo "   One-Click Setup & Deploy"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# ── Step 0: Check prerequisites ──────────────────────────
echo "🔍 Checking prerequisites..."

if ! command -v git &> /dev/null; then
    echo "❌ Git is not installed."
    echo "   Install it from: https://git-scm.com"
    echo ""
    echo "   Mac:     brew install git"
    echo "   Windows: Download from git-scm.com"
    echo "   Linux:   sudo apt install git"
    exit 1
fi
echo "   ✅ Git found: $(git --version)"

if ! command -v gh &> /dev/null; then
    echo ""
    echo "❌ GitHub CLI (gh) is not installed."
    echo "   Install it from: https://cli.github.com"
    echo ""
    echo "   Mac:     brew install gh"
    echo "   Windows: winget install --id GitHub.cli"
    echo "   Linux:   sudo apt install gh"
    exit 1
fi
echo "   ✅ GitHub CLI found: $(gh --version | head -1)"

# ── Step 1: Check GitHub authentication ──────────────────
echo ""
echo "🔑 Checking GitHub authentication..."
if ! gh auth status &> /dev/null; then
    echo "   You need to log in to GitHub first."
    echo "   Running: gh auth login"
    echo ""
    gh auth login
fi
echo "   ✅ Authenticated as: $(gh api user --jq .login)"

# ── Step 2: Initialize Git repo ──────────────────────────
echo ""
echo "📁 Initializing Git repository..."
if [ -d ".git" ]; then
    echo "   Git repo already exists — skipping init."
else
    git init
    echo "   ✅ Git initialized."
fi

# ── Step 3: Stage and commit ─────────────────────────────
echo ""
echo "📦 Staging and committing files..."
git add .
git commit -m "Initial MIIM setup: Streamlit dashboard, LLM extraction pipeline, Supabase schema

- Streamlit dashboard with 5 tabs (directory, charts, network, map, events)
- GPT-4o extraction pipeline for French/Arabic industrial news
- 12 passing unit tests
- Connected to Supabase (EU West Paris) with 10 seeded companies
- Navy/teal theme, Plotly charts, Folium map" 2>/dev/null || echo "   (No new changes to commit)"
echo "   ✅ Files committed."

# ── Step 4: Create GitHub repository ─────────────────────
echo ""
echo "🌐 Creating GitHub repository..."
REPO_EXISTS=$(gh repo view miim-platform --json name 2>/dev/null || echo "NOT_FOUND")

if [ "$REPO_EXISTS" = "NOT_FOUND" ]; then
    gh repo create miim-platform --public --source=. --remote=origin --push
    echo "   ✅ Repository created and code pushed!"
else
    echo "   Repository already exists — pushing to it."
    git branch -M main
    git remote add origin "https://github.com/$(gh api user --jq .login)/miim-platform.git" 2>/dev/null || true
    git push -u origin main
    echo "   ✅ Code pushed!"
fi

# ── Step 5: Show result ──────────────────────────────────
GITHUB_USER=$(gh api user --jq .login)
REPO_URL="https://github.com/$GITHUB_USER/miim-platform"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ SUCCESS! Your code is now on GitHub:"
echo ""
echo "   $REPO_URL"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "🚀 NEXT: Deploy to Streamlit Cloud"
echo ""
echo "   Opening Streamlit Cloud in your browser..."
echo ""

# Open Streamlit Cloud in browser
if command -v open &> /dev/null; then
    open "https://share.streamlit.io"
elif command -v xdg-open &> /dev/null; then
    xdg-open "https://share.streamlit.io"
elif command -v start &> /dev/null; then
    start "https://share.streamlit.io"
else
    echo "   Please open this URL manually:"
    echo "   https://share.streamlit.io"
fi

echo ""
echo "   On Streamlit Cloud:"
echo "   1. Sign in with GitHub"
echo "   2. Click 'New app'"
echo "   3. Select: $GITHUB_USER/miim-platform"
echo "   4. Branch: main"
echo "   5. Main file: app.py"
echo "   6. Click Deploy"
echo ""
echo "   Then add Secrets (Settings → Secrets):"
echo '   SUPABASE_URL = "https://rkqfjesnavbngtihffge.supabase.co"'
echo '   SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJrcWZqZXNuYXZibmd0aWhmZmdlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzIxNTY0NzIsImV4cCI6MjA4NzczMjQ3Mn0.Djfr1UI1XzrUQmvo2rNi3rvMQIC0GXrMHpZiPnG6zfE"'
echo ""
echo "🏭 MIIM is ready. Bonne construction, Jad!"
