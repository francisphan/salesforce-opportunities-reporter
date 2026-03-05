#!/usr/bin/env python3
"""Generate a live 4-page branded PV Campaign Dashboard PDF from Pardot + Salesforce.

Usage:
    python3 send_pv_dashboard_report.py              # Generate PDF only
    python3 send_pv_dashboard_report.py --email      # Generate PDF and email to subscribers

Produces: pv_dashboard_report_YYYYMMDD_HHMM.pdf
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import requests
from reportlab.platypus import SimpleDocTemplate, Spacer, PageBreak
from reportlab.lib.units import inch

from src import sf_client
from src import vines_pdf_styles as S

# ── Configuration ────────────────────────────────────────────────────────────

BU_ID = "0Uv8b0000008OLTCA2"
PARDOT_BASE = "https://pi.pardot.com/api/v5"

PV_PARENT_CAMPAIGN_ID = "701PW00000wgsU2YAI"  # TVOM_PV_Campaign_2025

# Keywords that identify PV campaign emails in Pardot
PV_EMAIL_KEYWORDS = [
    "Intro_", "Persuasion_", "Incentive_", "Mike-Intro", "DB-Thanks",
    "Journey", "Invest", "Tailored", "Why-Now", "PV-Overview",
    "Mike-Incentive", "PVO", "Private Vineyard",
]

W = S.CONTENT_WIDTH


# ── Data fetchers ────────────────────────────────────────────────────────────

def _pardot_headers(access_token):
    return {
        "Authorization": f"Bearer {access_token}",
        "Pardot-Business-Unit-Id": BU_ID,
        "Content-Type": "application/json",
    }


def fetch_pardot_email_stats(headers):
    """Fetch all list-email stats, filtering for PV-related emails."""
    url = f"{PARDOT_BASE}/objects/list-emails"
    params = {
        "fields": "id,name,campaignId,sentAt,createdAt",
        "orderBy": "createdAt DESC",
        "limit": 200,
    }

    resp = requests.get(url, headers=headers, params=params)
    if not resp.ok:
        print(f"  Warning: Could not fetch list-emails: {resp.status_code}")
        return []

    emails = resp.json().get("values", [])

    # Filter for PV-related
    pv_emails = []
    for e in emails:
        name = e.get("name") or ""
        if any(kw.lower() in name.lower() for kw in PV_EMAIL_KEYWORDS):
            pv_emails.append(e)

    # Fetch stats for each
    results = []
    for e in pv_emails:
        eid = e["id"]
        sr = requests.get(f"{PARDOT_BASE}/objects/list-emails/{eid}/stats", headers=headers)
        if sr.ok:
            stats = sr.json()
            stats["emailName"] = e.get("name", "")
            stats["sentAt"] = e.get("sentAt", "")
            results.append(stats)

    return results


def fetch_pardot_forms(headers):
    """Fetch form metadata."""
    resp = requests.get(f"{PARDOT_BASE}/objects/forms", headers=headers,
                        params={"fields": "id,name"})
    if resp.ok:
        return resp.json().get("values", [])
    return []


def fetch_sf_campaign_members(sf_holder):
    """Fetch campaign member stats for PV segments."""
    return sf_client.query(sf_holder, f"""
        SELECT Campaign.Parent.Name segment, Campaign.Name campaign_name,
               Status, COUNT(Id) cnt
        FROM CampaignMember
        WHERE Campaign.Parent.ParentId = '{PV_PARENT_CAMPAIGN_ID}'
           OR Campaign.ParentId = '{PV_PARENT_CAMPAIGN_ID}'
        GROUP BY Campaign.Parent.Name, Campaign.Name, Status
        ORDER BY Campaign.Parent.Name, COUNT(Id) DESC
    """)


def fetch_sf_pipeline(sf_holder):
    """Fetch opportunity pipeline."""
    return sf_client.query(sf_holder, """
        SELECT StageName, COUNT(Id) cnt, SUM(Amount) total_amount
        FROM Opportunity
        WHERE IsClosed = false OR IsWon = true
        GROUP BY StageName
        ORDER BY COUNT(Id) DESC
    """)


def fetch_sf_leads(sf_holder):
    """Fetch lead stats."""
    by_status = sf_client.query(sf_holder, """
        SELECT Status, COUNT(Id) cnt
        FROM Lead
        GROUP BY Status
        ORDER BY COUNT(Id) DESC
    """)

    by_language = sf_client.query(sf_holder, """
        SELECT Primary_Language__c lang, COUNT(Id) cnt
        FROM Lead
        WHERE Primary_Language__c != null
        GROUP BY Primary_Language__c
        ORDER BY COUNT(Id) DESC
        LIMIT 10
    """)

    scored = sf_client.query(sf_holder, """
        SELECT COUNT(Id) cnt, AVG(pi__score__c) avg_score
        FROM Lead
        WHERE pi__score__c > 0
    """)
    scored_rec = scored[0] if scored else {}

    return {
        "by_status": by_status,
        "by_language": by_language,
        "scored_count": scored_rec.get("cnt", 0),
        "avg_score": scored_rec.get("avg_score", 0) or 0,
    }


# ── Aggregation helpers ──────────────────────────────────────────────────────

def aggregate_email_stats(email_stats):
    """Aggregate email stats into summary and per-template rows."""
    if not email_stats:
        return {}, []

    total_sent = sum(e.get("sent", 0) for e in email_stats)
    total_delivered = sum(e.get("delivered", 0) for e in email_stats)
    total_opens = sum(e.get("opens", 0) for e in email_stats)
    total_clicks = sum(e.get("totalClicks", 0) for e in email_stats)
    total_unique_clicks = sum(e.get("uniqueClicks", 0) for e in email_stats)
    total_optouts = sum(e.get("optOuts", 0) for e in email_stats)
    total_bounced = sum(e.get("hardBounced", 0) + e.get("softBounced", 0) for e in email_stats)

    summary = {
        "total_sent": total_sent,
        "total_delivered": total_delivered,
        "total_opens": total_opens,
        "total_clicks": total_clicks,
        "total_unique_clicks": total_unique_clicks,
        "total_optouts": total_optouts,
        "total_bounced": total_bounced,
        "open_rate": total_opens / total_delivered if total_delivered else 0,
        "ctr": total_clicks / total_delivered if total_delivered else 0,
        "ctor": total_clicks / total_opens if total_opens else 0,
        "delivery_rate": total_delivered / total_sent if total_sent else 0,
        "bounce_rate": total_bounced / total_sent if total_sent else 0,
        "optout_rate": total_optouts / total_delivered if total_delivered else 0,
    }

    # Group by email name (collapse duplicates)
    by_name = {}
    for e in email_stats:
        name = e.get("emailName", "Unknown")
        if name not in by_name:
            by_name[name] = {"sent": 0, "delivered": 0, "opens": 0,
                             "clicks": 0, "unique_clicks": 0}
        by_name[name]["sent"] += e.get("sent", 0)
        by_name[name]["delivered"] += e.get("delivered", 0)
        by_name[name]["opens"] += e.get("opens", 0)
        by_name[name]["clicks"] += e.get("totalClicks", 0)
        by_name[name]["unique_clicks"] += e.get("uniqueClicks", 0)

    per_template = []
    for name, d in sorted(by_name.items(), key=lambda x: -x[1]["sent"]):
        if d["sent"] == 0:
            continue
        o_rate = d["opens"] / d["delivered"] if d["delivered"] else 0
        c_rate = d["clicks"] / d["delivered"] if d["delivered"] else 0
        ct_rate = d["clicks"] / d["opens"] if d["opens"] else 0
        per_template.append({
            "name": name, "sent": d["sent"], "delivered": d["delivered"],
            "opens": d["opens"], "clicks": d["clicks"],
            "open_rate": o_rate, "ctr": c_rate, "ctor": ct_rate,
        })

    return summary, per_template


def aggregate_by_language(per_template):
    """Group email stats by language suffix (ENG/SPA/POR)."""
    lang_map = {"ENG": "English", "SPA": "Spanish", "POR": "Portuguese", "ES": "Spanish",
                "PT": "Portuguese", "EN": "English"}
    by_lang = {}
    for t in per_template:
        name = t["name"]
        lang = "Other"
        for suffix, full in lang_map.items():
            if f"_{suffix}" in name or name.endswith(suffix):
                lang = full
                break
        if lang not in by_lang:
            by_lang[lang] = {"sent": 0, "delivered": 0, "opens": 0, "clicks": 0}
        by_lang[lang]["sent"] += t["sent"]
        by_lang[lang]["delivered"] += t["delivered"]
        by_lang[lang]["opens"] += t["opens"]
        by_lang[lang]["clicks"] += t["clicks"]
    return by_lang


# ── PDF page builders ────────────────────────────────────────────────────────

def _pct(n, d):
    return f"{n / d * 100:.1f}%" if d else "0.0%"


def _usd(n):
    return f"${n:,.0f}" if n else "$0"


def _num(n):
    return f"{n:,}" if isinstance(n, int) else str(n)


def page_cover(generated_at):
    return S.cover_page_elements(
        title="Private Vineyards<br/>Campaign Dashboard",
        subtitle="Live Data Report  \u2022  Pardot + Salesforce",
        date_text=f"Generated {generated_at}",
        doc_type="PERFORMANCE DASHBOARD",
    )


def page_email_performance(summary, per_template, by_lang):
    els = []
    els.append(S.section_heading("Page 1: Email Performance"))

    # Summary KPIs
    els.append(S.section_heading("Overall Metrics", level=2))
    kpi_data = [
        ["Total Sent", _num(summary.get("total_sent", 0))],
        ["Total Delivered", _num(summary.get("total_delivered", 0))],
        ["Delivery Rate", _pct(summary.get("total_delivered", 0), summary.get("total_sent", 1))],
        ["Total Opens", _num(summary.get("total_opens", 0))],
        ["Open Rate", f"{summary.get('open_rate', 0):.1%}"],
        ["Total Clicks", _num(summary.get("total_clicks", 0))],
        ["Click-Through Rate (CTR)", f"{summary.get('ctr', 0):.1%}"],
        ["Click-to-Open Rate (CTOR)", f"{summary.get('ctor', 0):.1%}"],
        ["Opt-Outs", _num(summary.get("total_optouts", 0))],
        ["Bounced", _num(summary.get("total_bounced", 0))],
    ]
    els.append(S.branded_table(["Metric", "Value"], kpi_data,
                               [W - 1.5 * inch, 1.5 * inch]))

    # By Language
    if by_lang:
        els.append(S.section_heading("Performance by Language", level=2))
        lang_rows = []
        for lang, d in sorted(by_lang.items(), key=lambda x: -x[1]["sent"]):
            if d["sent"] == 0:
                continue
            lang_rows.append([
                lang, _num(d["sent"]), _num(d["opens"]),
                _pct(d["opens"], d["delivered"]),
                _num(d["clicks"]),
                _pct(d["clicks"], d["delivered"]),
                _pct(d["clicks"], d["opens"]),
            ])
        els.append(S.branded_table(
            ["Language", "Sent", "Opens", "Open Rate", "Clicks", "CTR", "CTOR"],
            lang_rows,
            [1.0 * inch, 0.7 * inch, 0.7 * inch, 0.85 * inch, 0.7 * inch, 0.7 * inch,
             W - 4.65 * inch], font_size=8))

    # Per Template (top 15)
    if per_template:
        els.append(S.section_heading("Performance by Email Template", level=2))
        tmpl_rows = []
        for t in per_template[:15]:
            name = t["name"]
            if len(name) > 45:
                name = name[:42] + "..."
            tmpl_rows.append([
                name, _num(t["sent"]), _num(t["opens"]),
                f"{t['open_rate']:.0%}",
                _num(t["clicks"]),
                f"{t['ctr']:.1%}",
                f"{t['ctor']:.1%}",
            ])
        els.append(S.branded_table(
            ["Email Template", "Sent", "Opens", "OR", "Clicks", "CTR", "CTOR"],
            tmpl_rows,
            [2.6 * inch, 0.55 * inch, 0.55 * inch, 0.55 * inch, 0.6 * inch, 0.6 * inch,
             W - 5.45 * inch], font_size=7.5))

    els.append(PageBreak())
    return els


def page_engagement_clicks(per_template, campaign_members):
    els = []
    els.append(S.section_heading("Page 2: Engagement & Campaign Members"))

    # Campaign member stats by segment
    els.append(S.section_heading("Campaign Members by Audience Segment", level=2))
    seg_counts = {}
    for r in campaign_members:
        parent = r.get("Parent") or {}
        seg = parent.get("Name") or r.get("Campaign", {}).get("Name", "Unknown")
        if seg not in seg_counts:
            seg_counts[seg] = 0
        seg_counts[seg] += r.get("cnt", 0)

    if seg_counts:
        total_members = sum(seg_counts.values())
        seg_rows = []
        for seg, cnt in sorted(seg_counts.items(), key=lambda x: -x[1]):
            seg_rows.append([seg, _num(cnt), _pct(cnt, total_members)])
        els.append(S.branded_table(
            ["Audience Segment", "Members", "% of Total"],
            seg_rows,
            [2.5 * inch, 1.0 * inch, W - 3.5 * inch]))
        els.append(S.body_text(f"<b>Total campaign members: {_num(total_members)}</b>"))
    else:
        els.append(S.body_text("No campaign member data available yet."))

    # Email engagement summary
    els.append(S.section_heading("Email Engagement Summary", level=2))
    total_clicks = sum(t.get("clicks", 0) for t in per_template)
    total_opens = sum(t.get("opens", 0) for t in per_template)
    total_sent = sum(t.get("sent", 0) for t in per_template)

    eng_data = [
        ["Total Emails Sent (PV campaign)", _num(total_sent)],
        ["Total Opens", _num(total_opens)],
        ["Total Clicks", _num(total_clicks)],
        ["Avg Open Rate", _pct(total_opens, total_sent) if total_sent else "N/A"],
        ["Avg CTR", _pct(total_clicks, total_sent) if total_sent else "N/A"],
        ["Emails with >50% Open Rate",
         _num(sum(1 for t in per_template if t.get("open_rate", 0) > 0.5))],
        ["Emails with >10% CTR",
         _num(sum(1 for t in per_template if t.get("ctr", 0) > 0.1))],
    ]
    els.append(S.branded_table(["Metric", "Value"], eng_data,
                               [W - 1.5 * inch, 1.5 * inch]))

    # Top performing emails by clicks
    if per_template:
        els.append(S.section_heading("Top 10 Emails by Clicks", level=2))
        top_click = sorted(per_template, key=lambda x: -x.get("clicks", 0))[:10]
        tc_rows = []
        for t in top_click:
            if t["clicks"] == 0:
                continue
            name = t["name"]
            if len(name) > 50:
                name = name[:47] + "..."
            tc_rows.append([name, _num(t["clicks"]), f"{t['ctr']:.1%}", f"{t['ctor']:.1%}"])
        if tc_rows:
            els.append(S.branded_table(
                ["Email", "Clicks", "CTR", "CTOR"],
                tc_rows,
                [3.5 * inch, 0.8 * inch, 0.8 * inch, W - 5.1 * inch], font_size=8))

    els.append(PageBreak())
    return els


def page_forms_replies(forms, lead_data):
    els = []
    els.append(S.section_heading("Page 3: Forms & Leads by Language"))

    # Forms inventory
    els.append(S.section_heading("Active Forms", level=2))
    pv_forms = [f for f in forms if any(kw in (f.get("name") or "")
                for kw in ["PV", "Landing", "TVOM"])]
    if pv_forms:
        form_rows = [[f.get("name", ""), str(f.get("id", ""))] for f in pv_forms]
        els.append(S.branded_table(["Form Name", "Pardot ID"], form_rows,
                                   [W - 1.2 * inch, 1.2 * inch]))
    else:
        form_rows = [[f.get("name", ""), str(f.get("id", ""))] for f in forms[:16]]
        els.append(S.branded_table(["Form Name", "Pardot ID"], form_rows,
                                   [W - 1.2 * inch, 1.2 * inch], font_size=8))

    # Leads by language
    els.append(S.section_heading("Leads by Primary Language", level=2))
    lang_rows = []
    total_leads = sum(r.get("cnt", 0) for r in lead_data["by_language"])
    for r in lead_data["by_language"]:
        lang = r.get("lang") or r.get("Primary_Language__c") or "Unknown"
        cnt = r.get("cnt", 0)
        lang_rows.append([lang, _num(cnt), _pct(cnt, total_leads)])
    if lang_rows:
        els.append(S.branded_table(["Language", "Lead Count", "% of Total"],
                                   lang_rows,
                                   [1.5 * inch, 1.0 * inch, W - 2.5 * inch]))

    # Leads by status
    els.append(S.section_heading("Leads by Status", level=2))
    status_rows = []
    total_by_status = sum(r.get("cnt", 0) for r in lead_data["by_status"])
    for r in lead_data["by_status"]:
        status = r.get("Status", "Unknown")
        cnt = r.get("cnt", 0)
        status_rows.append([status, _num(cnt), _pct(cnt, total_by_status)])
    if status_rows:
        els.append(S.branded_table(["Status", "Count", "% of Total"],
                                   status_rows,
                                   [1.5 * inch, 1.0 * inch, W - 2.5 * inch]))

    # Pardot scoring summary
    els.append(S.section_heading("Pardot Scoring Summary", level=2))
    score_data = [
        ["Prospects with Pardot Score > 0", _num(lead_data.get("scored_count", 0))],
        ["Average Pardot Score", f"{lead_data.get('avg_score', 0):.1f}"],
        ["MQL Threshold", "Score \u2265 50"],
    ]
    els.append(S.branded_table(["Metric", "Value"], score_data,
                               [W - 1.5 * inch, 1.5 * inch]))

    els.append(PageBreak())
    return els


def page_pipeline(pipeline_data):
    els = []
    els.append(S.section_heading("Page 4: Pipeline & Conversion"))

    # Pipeline by stage
    els.append(S.section_heading("Opportunity Pipeline by Stage", level=2))

    active_stages = ["Deep Discovery", "Formal Proposal", "Awaiting Demo", "Signed"]
    won_stages = ["Closed Won", "Won"]
    lost_stages = ["Closed Lost", "Closed Cancel"]

    active_opps = []
    won_opps = []
    lost_opps = []

    for r in pipeline_data:
        stage = r.get("StageName", "Unknown")
        cnt = r.get("cnt", 0)
        amt = r.get("total_amount") or 0
        if stage in active_stages:
            active_opps.append((stage, cnt, amt))
        elif stage in won_stages:
            won_opps.append((stage, cnt, amt))
        elif stage in lost_stages:
            lost_opps.append((stage, cnt, amt))

    # Active pipeline
    els.append(S.section_heading("Active Pipeline", level=3))
    if active_opps:
        total_active = sum(c for _, c, _ in active_opps)
        total_active_val = sum(a for _, _, a in active_opps)
        rows = [[s, _num(c), _usd(a), _pct(c, total_active)]
                for s, c, a in sorted(active_opps, key=lambda x: -x[1])]
        rows.append(["<b>Total Active</b>", f"<b>{_num(total_active)}</b>",
                      f"<b>{_usd(total_active_val)}</b>", "<b>100%</b>"])
        els.append(S.branded_table(["Stage", "Count", "Value", "% of Active"], rows,
                                   [1.5 * inch, 0.8 * inch, 1.5 * inch, W - 3.8 * inch]))
    else:
        els.append(S.body_text("No active pipeline opportunities found."))

    # Won
    els.append(S.section_heading("Closed Won", level=3))
    if won_opps:
        total_won = sum(c for _, c, _ in won_opps)
        total_won_val = sum(a for _, _, a in won_opps)
        rows = [[s, _num(c), _usd(a)] for s, c, a in won_opps]
        rows.append(["<b>Total Won</b>", f"<b>{_num(total_won)}</b>",
                      f"<b>{_usd(total_won_val)}</b>"])
        els.append(S.branded_table(["Stage", "Count", "Revenue"], rows,
                                   [1.5 * inch, 1.0 * inch, W - 2.5 * inch]))

    # Lost
    els.append(S.section_heading("Closed Lost / Cancelled", level=3))
    if lost_opps:
        total_lost = sum(c for _, c, _ in lost_opps)
        total_lost_val = sum(a for _, _, a in lost_opps)
        rows = [[s, _num(c), _usd(a)] for s, c, a in lost_opps]
        rows.append(["<b>Total Lost</b>", f"<b>{_num(total_lost)}</b>",
                      f"<b>{_usd(total_lost_val)}</b>"])
        els.append(S.branded_table(["Stage", "Count", "Value"], rows,
                                   [1.5 * inch, 1.0 * inch, W - 2.5 * inch]))

    # Win rate
    total_won_cnt = sum(c for _, c, _ in won_opps)
    total_lost_cnt = sum(c for _, c, _ in lost_opps)
    total_active_cnt = sum(c for _, c, _ in active_opps)

    els.append(S.section_heading("Conversion Summary", level=2))
    conv_data = [
        ["Total Opportunities", _num(sum(r.get("cnt", 0) for r in pipeline_data))],
        ["Active Pipeline", _num(total_active_cnt)],
        ["Active Pipeline Value", _usd(sum(a for _, _, a in active_opps))],
        ["Won", _num(total_won_cnt)],
        ["Won Revenue", _usd(sum(a for _, _, a in won_opps))],
        ["Win Rate (Won / Won+Lost)", _pct(total_won_cnt, total_won_cnt + total_lost_cnt)],
        ["Lost / Cancelled", _num(total_lost_cnt)],
    ]
    els.append(S.branded_table(["Metric", "Value"], conv_data,
                               [W - 1.5 * inch, 1.5 * inch]))

    return els


# ── Email helpers ────────────────────────────────────────────────────────────

def send_pdf_email(pdf_path: str, generated_at: str):
    """Email the PDF as an attachment to subscribers."""
    import base64
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.mime.application import MIMEApplication

    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build

    from dotenv import load_dotenv
    load_dotenv()

    # Get Gmail credentials (same pattern as src/email_sender.py)
    refresh_token = os.environ.get("GMAIL_REFRESH_TOKEN")
    if refresh_token:
        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            client_id=os.environ["GMAIL_CLIENT_ID"],
            client_secret=os.environ["GMAIL_CLIENT_SECRET"],
            token_uri="https://oauth2.googleapis.com/token",
            scopes=["https://www.googleapis.com/auth/gmail.send"],
        )
        creds.refresh(Request())
    else:
        gmail_token = Path(__file__).parent / ".gmail_token.json"
        if gmail_token.exists():
            creds = Credentials.from_authorized_user_file(
                str(gmail_token), ["https://www.googleapis.com/auth/gmail.send"])
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
        else:
            print("  No Gmail credentials available — skipping email.")
            return

    service = build("gmail", "v1", credentials=creds)
    sender = os.environ.get("GMAIL_SENDER", "me")
    subscribers = os.environ.get("SUBSCRIBERS", "").split(",")
    subscribers = [s.strip() for s in subscribers if s.strip()]
    cc = os.environ.get("REPORT_CC", "").split(",")
    cc = [c.strip() for c in cc if c.strip()]

    if not subscribers:
        print("  No SUBSCRIBERS configured — skipping email.")
        return

    msg = MIMEMultipart("mixed")
    msg["Subject"] = f"PV Campaign Dashboard — {generated_at}"
    msg["From"] = sender
    msg["To"] = ", ".join(subscribers)
    if cc:
        msg["Cc"] = ", ".join(cc)

    body_html = f"""\
<p>Hi team,</p>
<p>Attached is the latest <b>Private Vineyards Campaign Dashboard</b> generated on {generated_at}.</p>
<p>This report pulls live data from Pardot (email performance, forms) and Salesforce
(campaign members, leads, pipeline) for the PV 2025 campaign.</p>
<p style="color: #888; font-size: 12px;">This is an automated report.</p>
"""
    msg.attach(MIMEText(body_html, "html"))

    # Attach PDF
    pdf_data = Path(pdf_path).read_bytes()
    attachment = MIMEApplication(pdf_data, _subtype="pdf")
    attachment.add_header("Content-Disposition", "attachment", filename=Path(pdf_path).name)
    msg.attach(attachment)

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    service.users().messages().send(userId="me", body={"raw": raw}).execute()
    all_recipients = subscribers + cc
    print(f"  Dashboard emailed to {len(all_recipients)} recipient(s).")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    send_email = "--email" in sys.argv

    now = datetime.now()
    generated_at = now.strftime("%B %d, %Y at %H:%M")
    output_file = f"pv_dashboard_report_{now.strftime('%Y%m%d_%H%M')}.pdf"

    print("=" * 60)
    print("PV CAMPAIGN DASHBOARD REPORT GENERATOR")
    print("=" * 60)

    # Connect to Salesforce
    print("\nConnecting to Salesforce...")
    try:
        sf = sf_client.connect()
        sf_holder = [sf]
        print(f"  Connected to {sf.sf_instance}")
    except Exception as e:
        print(f"  ERROR: {e}")
        sys.exit(1)

    # Pardot token — use the SF access token (requires pardot_api OAuth scope)
    access_token = sf.session_id
    headers = _pardot_headers(access_token)

    # Fetch data
    print("\nFetching Pardot email stats...")
    email_stats = fetch_pardot_email_stats(headers)
    print(f"  {len(email_stats)} PV-related emails found")

    print("Fetching Pardot forms...")
    forms = fetch_pardot_forms(headers)
    print(f"  {len(forms)} forms found")

    print("Fetching Salesforce campaign members...")
    campaign_members = fetch_sf_campaign_members(sf_holder)
    print(f"  {len(campaign_members)} member records")

    print("Fetching Salesforce pipeline...")
    pipeline = fetch_sf_pipeline(sf_holder)
    print(f"  {len(pipeline)} pipeline stages")

    print("Fetching Salesforce lead data...")
    lead_data = fetch_sf_leads(sf_holder)
    print(f"  {lead_data['scored_count']} scored leads")

    # Aggregate
    print("\nAggregating data...")
    summary, per_template = aggregate_email_stats(email_stats)
    by_lang = aggregate_by_language(per_template)

    # Build PDF
    print(f"\nGenerating PDF: {output_file}")
    doc = SimpleDocTemplate(
        output_file,
        pagesize=S.letter,
        leftMargin=S.MARGIN_LEFT,
        rightMargin=S.MARGIN_RIGHT,
        topMargin=S.MARGIN_TOP,
        bottomMargin=S.MARGIN_BOTTOM,
        title="PV Campaign Dashboard Report",
        author="The Vines of Mendoza",
    )

    elements = []
    elements += page_cover(generated_at)
    elements += page_email_performance(summary, per_template, by_lang)
    elements += page_engagement_clicks(per_template, campaign_members)
    elements += page_forms_replies(forms, lead_data)
    elements += page_pipeline(pipeline)

    def first_page(canvas, doc):
        S.cover_header_footer(canvas, doc)

    def later_pages(canvas, doc):
        S.header_footer(canvas, doc, title="PV Campaign Dashboard")

    doc.build(elements, onFirstPage=first_page, onLaterPages=later_pages)

    print(f"\nDone! Created {output_file} ({doc.page} pages)")

    # Optionally email
    if send_email:
        print("\nEmailing PDF...")
        send_pdf_email(output_file, generated_at)

    print("=" * 60)


if __name__ == "__main__":
    main()
