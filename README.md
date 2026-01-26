# Lumiere Email Response Tracker

## Overview

The Lumiere Email Response Tracker is an internal tool that monitors and analyzes email response times across the organization. It replaces the previous TimeToReply tool with a custom solution that provides more flexibility and control over tracking metrics.

The system automatically fetches email data from Gmail using Google's API, calculates response times for external emails, and presents the data through an interactive dashboard. It supports multiple domains (lumiere.education, ladderinternships.com, veritasai.com, horizoninspires.com, youngfounderslab.org, wallstreetguide.net) and treats emails between these domains as "internal" (not tracked for response time).

---

## Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Backend/Tracker** | Python 3.11 | Fetches Gmail data, processes threads, calculates metrics |
| **Database** | Supabase (PostgreSQL) | Stores tracked users, response pairs, and daily stats |
| **Dashboard** | Streamlit | Interactive web UI for viewing metrics and managing users |
| **Authentication** | Google Service Account | Domain-Wide Delegation to access Gmail across domains |
| **Automation** | GitHub Actions | Runs daily sync at 1:00 AM EST |
| **Hosting** | Streamlit Community Cloud | Hosts the public dashboard |

---

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  GitHub Actions │────▶│ tracker_supabase│────▶│    Supabase     │
│  (Daily @ 1AM)  │     │      .py        │     │   (PostgreSQL)  │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                                                         ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Team Members   │────▶│  Streamlit App  │────▶│    Dashboard    │
│   (Browser)     │     │    (app.py)     │     │   (Hosted)      │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

---

## Key Files

| File | Purpose |
|------|---------|
| `app.py` | Streamlit dashboard - the main UI |
| `tracker_supabase.py` | Core tracking logic - fetches Gmail, processes threads, saves to Supabase |
| `requirements.txt` | Python dependencies |
| `.github/workflows/daily-sync.yml` | GitHub Actions workflow for automated daily sync |
| `credentials.json` | Google Service Account credentials (not in repo - stored as secret) |
| `.env` | Local environment variables (not in repo) |

---

## Database Schema (Supabase)

**tracked_users**
- `email` (text, primary key)
- `display_name` (text)
- `domain` (text)
- `team_function` (text) - "operations", "growth", or "other"
- `is_active` (boolean)
- `created_at` (timestamp)

**response_pairs**
- `id` (uuid)
- `user_email` (text)
- `external_sender` (text)
- `subject` (text)
- `received_at` (timestamp)
- `replied_at` (timestamp)
- `response_hours` (numeric)
- `thread_id` (text)
- Unique constraint on `(thread_id, replied_at)`

**daily_stats**
- `user_email` (text)
- `date` (date)
- `emails_received` (integer)
- `emails_sent` (integer)
- `response_pairs_count` (integer)
- `avg_response_hours` (numeric)
- `median_response_hours` (numeric)
- `min_response_hours` (numeric)
- `max_response_hours` (numeric)
- Unique constraint on `(user_email, date)`

---

## Setup Requirements

### 1. Google Service Account with Domain-Wide Delegation
Each domain needs to authorize the service account in their Google Admin Console:
- Admin Console → Security → API Controls → Domain-wide Delegation
- Client ID: `115180249992623685625`
- Scope: `https://www.googleapis.com/auth/gmail.readonly`

### 2. Supabase Project
- Tables created as described above
- API URL and Key stored in secrets

### 3. GitHub Secrets (for Actions)
- `SUPABASE_URL`
- `SUPABASE_KEY`
- `GOOGLE_CREDENTIALS_JSON` (full JSON content of credentials.json)

### 4. Streamlit Cloud Secrets
- `SUPABASE_URL`
- `SUPABASE_KEY`
- `GITHUB_TOKEN` (for triggering workflows from dashboard)

---

## How to Update Using Claude Code

### Installing Claude Code on Mac (Step-by-Step)

**Step 1: Open Terminal**
- Press `Cmd + Space` to open Spotlight
- Type `Terminal` and press Enter

**Step 2: Install Homebrew (if you don't have it)**
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```
Follow the prompts. When done, close and reopen Terminal.

**Step 3: Install Node.js**
```bash
brew install node
```
Verify it worked: `node --version` (should show v18 or higher)

**Step 4: Install Claude Code**
```bash
npm install -g @anthropic-ai/claude-code
```

**Step 5: Authenticate**
```bash
claude
```
The first time you run it, it will open a browser window to log in with your Anthropic account (Claude Pro or Max subscription), or ask for an API key.

**Step 6: Navigate to the project and start coding**
```bash
cd ~/lumiere-email-tracker
claude
```
Now just type what you want to change in plain English!

---

### Adding a New Feature to the Dashboard

```
Open Claude Code in the project directory:
cd ~/lumiere-email-tracker

Then describe what you want:
"Add a chart showing response time trends over the past 30 days"
"Add a new filter for date range on the dashboard"
"Show the top 5 slowest responses for each user"
```

### Modifying the Tracker Logic

```
"Change the definition of 'external email' to exclude @example.com"
"Add a new noise filter to exclude emails from noreply@someservice.com"
"Increase the default thread fetch from 500 to 1000"
```

### Database Changes

```
"Add a new column 'department' to the tracked_users table"
"Create a query to find users with response times over 24 hours"
```

### After Making Changes

Claude Code will edit the files. Then commit and push:
```bash
git add .
git commit -m "Description of changes"
git push origin main
```

The Streamlit Cloud app will auto-redeploy. If you changed `tracker_supabase.py`, the next GitHub Actions run (or manual trigger) will use the new code.

---

## Common Tasks

### Add a New User
Use the "Manage Team" tab in the dashboard, or:
```bash
python3 tracker_supabase.py --user newemail@domain.com --backfill
```

### Re-sync a User's Data
From the dashboard "Manage Team" tab, or:
```bash
python3 tracker_supabase.py --user email@domain.com --backfill
```

### Add a New Domain
1. Set up Domain-Wide Delegation in Google Admin Console for that domain
2. Add a user from that domain through the dashboard
3. The domain will automatically be recognized as "internal"

### Run Tracker Manually
```bash
# All users, recent data
python3 tracker_supabase.py

# Specific user with 90-day backfill
python3 tracker_supabase.py --user email@domain.com --backfill
```

---

## Monitoring & Troubleshooting

- **GitHub Actions logs**: Check workflow runs at `github.com/stephent-lumiere/lumiere-email-tracker/actions`
- **Streamlit logs**: Click "Manage app" in the deployed app to view logs
- **Supabase**: View data directly in the Supabase dashboard table editor

---

## Cost

- **Streamlit Community Cloud**: Free
- **Supabase**: Free tier (sufficient for this use case)
- **GitHub Actions**: Free for public repos
- **Google API**: Free (within quota limits)

**Total: $0/month**
