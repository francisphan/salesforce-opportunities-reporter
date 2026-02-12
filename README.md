# Salesforce Opportunities Weekly Report

Automated weekly email report of open Salesforce Opportunities with meaningful human activity. Each opportunity owner receives a personalized report showing only their opportunities. Runs on GitHub Actions every Monday morning — no servers, no cost.

## What the report includes

- Open Opportunities created in the last 6 months with 2+ human Tasks (excludes TVG)
- Automated system interactions are filtered out based on Salesforce user license type
- **High Priority section** at the top for opportunities with no activity in 2+ months
- Last touched date and touch count per opportunity, sorted by most touches first
- Clickable links to each Opportunity in Salesforce Lightning
- Each owner only sees their own opportunities

## How it works

1. Connects to Salesforce via OAuth 2.0
2. Queries open Opportunities (created in last 6 months, excluding TVG)
3. Queries Tasks linked to those Opportunities
4. Filters out touches by automated users (integration licenses, process automation)
5. Counts human touches per opportunity and keeps those with 2+
6. Groups opportunities by Owner and sends personalized emails via Gmail API
7. Opportunities with no activity in 2+ months are flagged as high priority

## Setup

### Prerequisites

- Python 3.10+
- A Salesforce Connected App with OAuth enabled
- A Google Cloud project with Gmail API enabled and OAuth 2.0 credentials (Desktop app type)

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment

Copy the example and fill in your credentials:

```bash
cp .env.example .env
```

| Variable | Description |
|---|---|
| `SF_CLIENT_ID` | Salesforce Connected App consumer key |
| `SF_CLIENT_SECRET` | Salesforce Connected App consumer secret |
| `SF_REDIRECT_URI` | OAuth callback URL (default: `http://localhost:8400/callback`) |
| `SF_DOMAIN` | Leave empty for production, `test` for sandbox |
| `GMAIL_CLIENT_ID` | Google OAuth 2.0 client ID |
| `GMAIL_CLIENT_SECRET` | Google OAuth 2.0 client secret |
| `GMAIL_SENDER` | Sender email address |
| `SUBSCRIBERS` | Comma-separated list of recipient emails (must match Salesforce Owner.Email) |
| `REPORT_CC` | Optional comma-separated CC list for all outgoing reports |

### 3. Run locally (first time)

```bash
python3 main.py
```

This will:
- Open a browser for Salesforce OAuth — log in and authorize
- Open a browser for Gmail OAuth — log in and grant send permission
- Query Salesforce and send personalized reports to each subscriber

Tokens are cached in `.token_cache.json` (Salesforce) and `.gmail_token.json` (Gmail) for subsequent runs.

### 4. Deploy to GitHub Actions

Add repository secrets (replace values with your own):

```bash
gh secret set SF_CLIENT_ID --body "your_value"
gh secret set SF_CLIENT_SECRET --body "your_value"
gh secret set SF_REFRESH_TOKEN --body "your_value"       # from .token_cache.json
gh secret set SF_INSTANCE_URL --body "your_value"         # from .token_cache.json
gh secret set SF_DOMAIN --body ""
gh secret set GMAIL_SENDER --body "your_email@example.com"
gh secret set GMAIL_CLIENT_ID --body "your_value"
gh secret set GMAIL_CLIENT_SECRET --body "your_value"
gh secret set GMAIL_REFRESH_TOKEN --body "your_value"     # from .gmail_token.json
gh secret set SUBSCRIBERS --body "person1@example.com,person2@example.com"
gh secret set REPORT_CC --body "manager@example.com"      # optional
```

The workflow runs every Monday at 9:00 AM EST. To test manually:

```bash
gh workflow run weekly_report.yml
```

## Managing subscribers

Subscribers are stored in the `SUBSCRIBERS` GitHub Secret as a comma-separated list. Each email must match an Opportunity Owner's email in Salesforce. To add or remove subscribers, update the secret:

```bash
gh secret set SUBSCRIBERS --body "person1@example.com,person2@example.com,person3@example.com"
```

## Project structure

```
main.py                  # Entry point — per-owner email routing
src/
  sf_client.py           # Salesforce OAuth client with retry logic
  opportunities.py       # SOQL queries and human touch counting
  report_template.py     # HTML email template (stale/active sections)
  email_sender.py        # Gmail API OAuth sender with CC support
.github/workflows/
  weekly_report.yml      # Scheduled GitHub Actions workflow
```
