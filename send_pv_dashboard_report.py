#!/usr/bin/env python3
"""Generate a branded 5-page PV Campaign 2026 Report PDF from Pardot + Salesforce.

Usage:
    python3 send_pv_dashboard_report.py                    # Generate PDF only
    python3 send_pv_dashboard_report.py --email            # Generate PDF and email
    python3 send_pv_dashboard_report.py --skip-audience    # Skip audience resolution (fast)

Produces: pv_dashboard_report_YYYYMMDD_HHMM.pdf
"""

import base64
import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import requests
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Spacer, PageBreak

from src import sf_client
from src import vines_pdf_styles as S

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────

BU_ID = "0Uv8b0000008OLTCA2"
PARDOT_BASE = "https://pi.pardot.com/api/v5"

PV_CAMPAIGN_ID = "701PW00001T1KhrYAF"       # Salesforce campaign
PARDOT_CAMPAIGN_ID = "263691"                # Pardot campaign
CAMPAIGN_START_DATE = "2026-03-01T00:00:00+00:00"

TEST_EMAIL_PATTERNS = ["fs.phan", "ms.arricar"]

W = S.CONTENT_WIDTH

# ── Hardcoded Template IDs ───────────────────────────────────────────────────

TEMPLATE_STATS = {
    "62574": {"email_num": "01a", "lang": "EN", "name": "PV-2026_EN_Email-01a-ThankYou"},
    "62577": {"email_num": "01b", "lang": "EN", "name": "PV-2026_EN_Email-01b-Intro"},
    "62580": {"email_num": "02",  "lang": "EN", "name": "PV-2026_EN_Email-02-FollowUp"},
    "62583": {"email_num": "03",  "lang": "EN", "name": "PV-2026_EN_Email-03-WineJourney"},
    "62586": {"email_num": "04",  "lang": "EN", "name": "PV-2026_EN_Email-04-LabelDesign"},
    "62589": {"email_num": "05",  "lang": "EN", "name": "PV-2026_EN_Email-05-Lifestyle"},
    "62592": {"email_num": "06",  "lang": "EN", "name": "PV-2026_EN_Email-06-Urgency"},
    "62595": {"email_num": "07",  "lang": "EN", "name": "PV-2026_EN_Email-07-FinalPush"},
    "62598": {"email_num": "01a", "lang": "ES", "name": "PV-2026_ES_Email-01a-ThankYou"},
    "62601": {"email_num": "01b", "lang": "ES", "name": "PV-2026_ES_Email-01b-Intro"},
    "62604": {"email_num": "07",  "lang": "ES", "name": "PV-2026_ES_Email-07-FinalPush"},
    "62607": {"email_num": "01a", "lang": "PT", "name": "PV-2026_PT_Email-01a-ThankYou"},
    "62610": {"email_num": "01b", "lang": "PT", "name": "PV-2026_PT_Email-01b-Intro"},
    "62613": {"email_num": "07",  "lang": "PT", "name": "PV-2026_PT_Email-07-FinalPush"},
    "62616": {"email_num": "02",  "lang": "ES", "name": "PV-2026_ES_Email-02-FollowUp"},
    "62619": {"email_num": "03",  "lang": "ES", "name": "PV-2026_ES_Email-03-WineJourney"},
    "62622": {"email_num": "04",  "lang": "ES", "name": "PV-2026_ES_Email-04-LabelDesign"},
    "62625": {"email_num": "05",  "lang": "ES", "name": "PV-2026_ES_Email-05-Lifestyle"},
    "62628": {"email_num": "06",  "lang": "ES", "name": "PV-2026_ES_Email-06-Urgency"},
    "62631": {"email_num": "02",  "lang": "PT", "name": "PV-2026_PT_Email-02-FollowUp"},
    "62634": {"email_num": "03",  "lang": "PT", "name": "PV-2026_PT_Email-03-WineJourney"},
    "62637": {"email_num": "04",  "lang": "PT", "name": "PV-2026_PT_Email-04-LabelDesign"},
    "62640": {"email_num": "05",  "lang": "PT", "name": "PV-2026_PT_Email-05-Lifestyle"},
    "62643": {"email_num": "06",  "lang": "PT", "name": "PV-2026_PT_Email-06-Urgency"},
    "63408": {"email_num": "BD",  "lang": "EN", "name": "PV-2026_EN_Email-BrochureDownload"},
    "63411": {"email_num": "BD",  "lang": "ES", "name": "PV-2026_ES_Email-BrochureDownload"},
    "63414": {"email_num": "BD",  "lang": "PT", "name": "PV-2026_PT_Email-BrochureDownload"},
}
TEMPLATE_IDS = set(TEMPLATE_STATS.keys())

EMAIL_DISPLAY = {
    "01a": "Email 1a - Thank You",
    "01b": "Email 1b - Intro",
    "02": "Email 2 - Follow Up",
    "03": "Email 3 - Wine Journey",
    "04": "Email 4 - Label Design",
    "05": "Email 5 - Lifestyle",
    "06": "Email 6 - Urgency",
    "07": "Email 7 - Final Push",
    "BD": "Brochure Download",
}

AUDIENCE_SEGMENTS = [
    "Unknown Database", "7Fuegos", "PV Prospects", "Not PV",
    "Past Guests", "TVG only", "TVRS Guests Static", "General",
    "Digital Prospects", "TVG Residences",
]

FORM_DISPLAY = {
    "Download-Brochure": "Download Brochure",
    "Donwload-Brochure": "Download Brochure",
    "Get-In-Touch": "Get In Touch",
    "Schedule-A-Meeting": "Schedule a Meeting",
    "Schedule_A_Meeting": "Schedule a Meeting",
    "Pop-Up": "Website Pop-Up",
    "Pop_Up": "Website Pop-Up",
    "Wine-Journey": "Join Wine Journey",
    "Thank-You": "Thank You",
}

PV_FORM_PREFIXES = ["PV_2026_", "PV_Landing_", "PV_Web_", "Landing_"]

QUERY_PAGE_LIMIT = 999
QUERY_THROTTLE = 0.2  # seconds between pages


# ── Pardot API Helpers ───────────────────────────────────────────────────────

def _pardot_headers(access_token):
    return {
        "Authorization": f"Bearer {access_token}",
        "Pardot-Business-Unit-Id": BU_ID,
        "Content-Type": "application/json",
    }


def _pardot_paginate_id(url, headers, params, throttle=QUERY_THROTTLE):
    """Paginate Pardot API using idGreaterThan."""
    all_rows = []
    last_id = 0
    while True:
        page_params = dict(params)
        if last_id:
            page_params["idGreaterThan"] = str(last_id)
        resp = requests.get(url, headers=headers, params=page_params)
        if resp.status_code != 200:
            log.warning("  API error %s: %s", resp.status_code, resp.text[:200])
            break
        vals = resp.json().get("values", [])
        if not vals:
            break
        all_rows.extend(vals)
        last_id = max(int(v["id"]) for v in vals)
        time.sleep(throttle)
    return all_rows


def _pardot_get_all(url, headers, params):
    """Paginate using nextPageToken (for lists, forms, etc.)."""
    results = []
    current_params = dict(params)
    base_params = {k: v for k, v in current_params.items() if k != "nextPageToken"}
    for _ in range(50):
        resp = requests.get(url, headers=headers, params=current_params)
        if resp.status_code != 200:
            break
        data = resp.json()
        vals = data.get("values", [])
        results.extend(vals)
        nxt = data.get("nextPageToken")
        if not nxt or not vals:
            break
        current_params = dict(base_params)
        current_params["nextPageToken"] = nxt
        time.sleep(0.2)
    return results


# ── Data Fetchers ────────────────────────────────────────────────────────────

def fetch_email_engagement(headers):
    """Pull email engagement activities via Pardot visitor-activities API.

    Queries types 6 (sent), 11 (open), 12 (click), 35 (unsub), 36 (bounce)
    one at a time, then filters client-side to campaignId == PARDOT_CAMPAIGN_ID.
    """
    log.info("Fetching email engagement from Pardot...")
    fields = "id,prospectId,campaignId,type,typeName,emailTemplateId,createdAt"
    activity_types = [6, 11, 12, 35, 36]
    type_names = {6: "sent", 11: "open", 12: "click", 35: "unsub", 36: "bounce"}

    all_activities = []
    for atype in activity_types:
        rows = _pardot_paginate_id(
            f"{PARDOT_BASE}/objects/visitor-activities",
            headers,
            {
                "fields": fields,
                "type": str(atype),
                "createdAtAfterOrEqualTo": CAMPAIGN_START_DATE,
                "limit": str(QUERY_PAGE_LIMIT),
            },
        )
        # Client-side campaign filter
        pv_rows = [r for r in rows if str(r.get("campaignId", "")) == PARDOT_CAMPAIGN_ID]
        log.info("  %s: %d total, %d PV-2026", type_names.get(atype, str(atype)),
                 len(rows), len(pv_rows))
        for r in pv_rows:
            r["type"] = int(r.get("type", 0))
        all_activities.extend(pv_rows)

    # Warn about unrecognised template IDs
    seen_templates = set()
    for a in all_activities:
        tid = str(a.get("emailTemplateId", ""))
        if tid and tid not in TEMPLATE_IDS:
            seen_templates.add(tid)
    if seen_templates:
        log.warning("  WARNING: Unrecognised template IDs in PV activities: %s",
                    ", ".join(sorted(seen_templates)))

    log.info("  Total PV activities: %d", len(all_activities))
    return all_activities


def fetch_audience_segments(headers):
    """Build prospect_id -> segment mapping from PV-2026 list memberships."""
    log.info("Fetching audience segments from Pardot lists...")

    all_lists = _pardot_get_all(
        f"{PARDOT_BASE}/objects/lists",
        headers,
        {"fields": "id,name,isDynamic"},
    )
    pv_lists = [lst for lst in all_lists if lst.get("name", "").startswith("PV-2026-")]
    log.info("  Found %d PV-2026 lists", len(pv_lists))

    prospect_audience = {}  # prospect_id (str or int) -> segment name

    for lst in pv_lists:
        list_name = lst["name"]
        list_id = str(lst["id"])

        segment = _segment_from_list_name(list_name)
        if not segment:
            continue

        members = _pardot_paginate_id(
            f"{PARDOT_BASE}/objects/list-memberships",
            headers,
            {"fields": "id,prospectId,listId", "listId": list_id, "limit": "999"},
        )

        for m in members:
            pid = m.get("prospectId")
            if pid and pid not in prospect_audience:
                prospect_audience[pid] = segment

        log.info("  %s: %d members -> %s", list_name, len(members), segment)
        time.sleep(0.3)

    log.info("  Total: %d prospects mapped to audiences", len(prospect_audience))

    # Identify test prospect IDs by scanning prospect emails
    test_prospect_ids = _find_test_prospect_ids(headers, prospect_audience)

    return prospect_audience, test_prospect_ids


def _find_test_prospect_ids(headers, prospect_audience):
    """Scan prospects to find test accounts matching TEST_EMAIL_PATTERNS.

    Queries all prospects via idGreaterThan pagination (same approach as
    dashboard_data.py) and flags any whose email matches a test pattern.
    """
    log.info("  Scanning prospects for test accounts...")
    test_ids = set()
    last_id = 0
    scanned = 0
    while True:
        params = {"fields": "id,email", "limit": "999"}
        if last_id:
            params["idGreaterThan"] = str(last_id)
        resp = requests.get(
            f"{PARDOT_BASE}/objects/prospects", headers=headers, params=params,
        )
        if resp.status_code != 200:
            break
        vals = resp.json().get("values", [])
        if not vals:
            break
        for p in vals:
            email = (p.get("email") or "").strip().lower()
            if any(pat in email for pat in TEST_EMAIL_PATTERNS):
                pid = p.get("id")
                test_ids.add(pid)
                test_ids.add(str(pid))
        scanned += len(vals)
        last_id = max(int(v["id"]) for v in vals)
        time.sleep(QUERY_THROTTLE)

    if test_ids:
        log.info("  Excluded %d test prospect IDs (%d prospects scanned)",
                 len(test_ids) // 2, scanned)
    return test_ids


def _segment_from_list_name(name):
    """Extract audience segment from a PV-2026 list name."""
    short = name.replace("PV-2026-", "")
    for lang in ["EN-", "ES-", "PT-"]:
        short = short.replace(lang, "", 1)
    if short.startswith("Suppression"):
        return None
    if short in ("Digital-Prospects", "Digital-Prospects-Helper"):
        return "Digital Prospects"
    return short.replace("-", " ")


def fetch_form_submissions(headers):
    """Fetch PV-related form submissions from Pardot."""
    log.info("Fetching form submissions from Pardot...")

    # Get all forms
    all_forms = _pardot_get_all(
        f"{PARDOT_BASE}/objects/forms",
        headers,
        {"fields": "id,name"},
    )

    # Filter to PV forms
    pv_forms = {}  # form_id (str) -> {name, display}
    for f in all_forms:
        name = f.get("name", "")
        if "[TEST]" in name:
            continue
        if not any(name.startswith(prefix) for prefix in PV_FORM_PREFIXES):
            continue
        display = _classify_form(name)
        if display:
            pv_forms[str(f["id"])] = {"name": name, "display": display}

    log.info("  Found %d PV-related forms", len(pv_forms))

    if not pv_forms:
        return [], {}

    # Pull type=3 (form submission) activities
    rows = _pardot_paginate_id(
        f"{PARDOT_BASE}/objects/visitor-activities",
        headers,
        {
            "fields": "id,prospectId,type,typeName,formId,createdAt",
            "type": "3",
            "createdAtAfterOrEqualTo": CAMPAIGN_START_DATE,
            "limit": str(QUERY_PAGE_LIMIT),
        },
    )

    filtered = [r for r in rows if str(r.get("formId", "")) in pv_forms]
    log.info("  %d total form activities, %d PV forms", len(rows), len(filtered))
    return filtered, pv_forms


def _classify_form(name):
    """Map form name to display name using FORM_DISPLAY patterns."""
    for pattern, label in FORM_DISPLAY.items():
        if pattern in name:
            return label
    return None


def fetch_sf_opportunities(sf_holder):
    """Fetch PV Campaign 2026 opportunities from Salesforce."""
    log.info("Fetching Salesforce opportunities...")
    records = sf_client.query(sf_holder, f"""
        SELECT Id, Name, StageName, Amount, CreatedDate, CloseDate,
               Account.Name, LeadSource
        FROM Opportunity
        WHERE CampaignId = '{PV_CAMPAIGN_ID}'
        ORDER BY CreatedDate DESC
    """)
    log.info("  Found %d opportunities", len(records))
    return records


# ── Aggregation ──────────────────────────────────────────────────────────────

def aggregate_email_stats(activities):
    """Aggregate email activities into overall, by-email, and by-language stats.

    Returns (overall, by_email, by_lang) dicts.
    """
    if not activities:
        return _empty_overall(), {}, {}

    # Total counts by type
    totals = defaultdict(int)
    # Unique prospect sets by type
    uniques = defaultdict(set)
    # By email_num: {email_num: {type: count}}
    by_email_totals = defaultdict(lambda: defaultdict(int))
    by_email_uniques = defaultdict(lambda: defaultdict(set))
    # By language
    by_lang_totals = defaultdict(lambda: defaultdict(int))
    by_lang_uniques = defaultdict(lambda: defaultdict(set))

    for a in activities:
        t = a["type"]
        pid = a.get("prospectId")
        tid = str(a.get("emailTemplateId", ""))
        tmpl = TEMPLATE_STATS.get(tid)

        totals[t] += 1
        if pid:
            uniques[t].add(pid)

        if tmpl:
            enum = tmpl["email_num"]
            lang = tmpl["lang"]
            by_email_totals[enum][t] += 1
            by_lang_totals[lang][t] += 1
            if pid:
                by_email_uniques[enum][t].add(pid)
                by_lang_uniques[lang][t].add(pid)

    overall = {
        "sent": totals[6], "opens": totals[11], "clicks": totals[12],
        "unsubs": totals[35], "bounces": totals[36],
        "unique_sent": len(uniques[6]), "unique_opens": len(uniques[11]),
        "unique_clicks": len(uniques[12]),
    }
    overall["open_rate"] = overall["opens"] / overall["sent"] if overall["sent"] else 0
    overall["ctr"] = overall["clicks"] / overall["sent"] if overall["sent"] else 0
    overall["ctor"] = overall["clicks"] / overall["opens"] if overall["opens"] else 0
    overall["unique_open_rate"] = overall["unique_opens"] / overall["unique_sent"] if overall["unique_sent"] else 0
    overall["unique_ctr"] = overall["unique_clicks"] / overall["unique_sent"] if overall["unique_sent"] else 0
    overall["unsub_rate"] = overall["unsubs"] / overall["sent"] if overall["sent"] else 0
    overall["bounce_rate"] = overall["bounces"] / overall["sent"] if overall["sent"] else 0

    by_email = {}
    for enum in EMAIL_DISPLAY:
        t = by_email_totals[enum]
        u = by_email_uniques[enum]
        sent, opens, clicks = t[6], t[11], t[12]
        by_email[enum] = {
            "sent": sent, "opens": opens, "clicks": clicks,
            "open_rate": opens / sent if sent else 0,
            "ctr": clicks / sent if sent else 0,
            "ctor": clicks / opens if opens else 0,
        }

    by_lang = {}
    for lang in ["EN", "ES", "PT"]:
        t = by_lang_totals[lang]
        sent, opens, clicks = t[6], t[11], t[12]
        by_lang[lang] = {
            "sent": sent, "opens": opens, "clicks": clicks,
            "open_rate": opens / sent if sent else 0,
            "ctr": clicks / sent if sent else 0,
            "ctor": clicks / opens if opens else 0,
        }

    return overall, by_email, by_lang


def _empty_overall():
    return {
        "sent": 0, "opens": 0, "clicks": 0, "unsubs": 0, "bounces": 0,
        "unique_sent": 0, "unique_opens": 0, "unique_clicks": 0,
        "open_rate": 0, "ctr": 0, "ctor": 0,
        "unique_open_rate": 0, "unique_ctr": 0,
        "unsub_rate": 0, "bounce_rate": 0,
    }


def aggregate_audience_stats(activities, prospect_audience):
    """Aggregate email activities by audience segment.

    Returns dict: segment -> {sent, opens, clicks, open_rate, ctr, ctor}.
    """
    seg_totals = defaultdict(lambda: defaultdict(int))

    for a in activities:
        t = a["type"]
        pid = a.get("prospectId")
        segment = prospect_audience.get(pid, prospect_audience.get(str(pid), "Unassigned"))
        seg_totals[segment][t] += 1

    result = {}
    for seg in AUDIENCE_SEGMENTS + ["Unassigned"]:
        t = seg_totals[seg]
        sent, opens, clicks = t[6], t[11], t[12]
        result[seg] = {
            "sent": sent, "opens": opens, "clicks": clicks,
            "open_rate": opens / sent if sent else 0,
            "ctr": clicks / sent if sent else 0,
            "ctor": clicks / opens if opens else 0,
        }

    return result


def aggregate_forms(form_activities, pv_forms):
    """Aggregate form submissions by display name.

    Returns list of {form, interactions, unique_prospects}.
    """
    by_form = defaultdict(lambda: {"count": 0, "prospects": set()})
    for a in form_activities:
        fid = str(a.get("formId", ""))
        info = pv_forms.get(fid)
        if not info:
            continue
        display = info["display"]
        by_form[display]["count"] += 1
        pid = a.get("prospectId")
        if pid:
            by_form[display]["prospects"].add(pid)

    rows = []
    for form_name in sorted(by_form):
        d = by_form[form_name]
        rows.append({
            "form": form_name,
            "interactions": d["count"],
            "unique_prospects": len(d["prospects"]),
        })
    return rows


def aggregate_pipeline(opportunities):
    """Aggregate opportunities by stage.

    Returns (stage_rows, top_recent) where stage_rows = [{stage, count, amount}].
    """
    by_stage = defaultdict(lambda: {"count": 0, "amount": 0})
    for opp in opportunities:
        stage = opp.get("StageName", "Unknown")
        amt = opp.get("Amount") or 0
        by_stage[stage]["count"] += 1
        by_stage[stage]["amount"] += amt

    stage_rows = []
    for stage, d in sorted(by_stage.items(), key=lambda x: -x[1]["count"]):
        stage_rows.append({"stage": stage, "count": d["count"], "amount": d["amount"]})

    top_recent = []
    for opp in opportunities[:5]:
        account = opp.get("Account") or {}
        top_recent.append({
            "name": opp.get("Name", ""),
            "account": account.get("Name", ""),
            "stage": opp.get("StageName", ""),
            "amount": opp.get("Amount") or 0,
            "created": (opp.get("CreatedDate") or "")[:10],
        })

    return stage_rows, top_recent


def aggregate_daily(activities):
    """Aggregate email activities by day for the last 14 days.

    Returns list of rows with daily and cumulative counts.
    """
    daily = defaultdict(lambda: defaultdict(int))
    for a in activities:
        date = (a.get("createdAt") or "")[:10]
        if not date:
            continue
        daily[date][a["type"]] += 1

    sorted_dates = sorted(daily.keys())
    cum_sent = cum_opens = cum_clicks = 0
    rows = []
    for date in sorted_dates:
        d = daily[date]
        sent, opens, clicks = d[6], d[11], d[12]
        unsubs, bounces = d[35], d[36]
        cum_sent += sent
        cum_opens += opens
        cum_clicks += clicks
        rows.append({
            "date": date,
            "sent": sent, "opens": opens, "clicks": clicks,
            "unsubs": unsubs, "bounces": bounces,
            "cum_sent": cum_sent, "cum_opens": cum_opens, "cum_clicks": cum_clicks,
            "open_rate": cum_opens / cum_sent * 100 if cum_sent else 0,
            "ctr": cum_clicks / cum_sent * 100 if cum_sent else 0,
        })

    # Return only the last 14 days
    return rows[-14:]


# ── PDF Formatting Helpers ───────────────────────────────────────────────────

def _pct(n, d):
    return f"{n / d * 100:.1f}%" if d else "0.0%"


def _pct_val(v):
    return f"{v * 100:.1f}%"


def _usd(n):
    return f"${n:,.0f}" if n else "$0"


def _num(n):
    return f"{n:,}" if isinstance(n, int) else str(n)


# ── PDF Page Builders ────────────────────────────────────────────────────────

def page_cover(generated_at):
    return S.cover_page_elements(
        title="Private Vineyards<br/>Campaign Report 2026",
        subtitle="Pardot + Salesforce",
        date_text=f"Generated {generated_at}",
        doc_type="PERFORMANCE DASHBOARD",
    )


def page_kpis_and_email(overall, by_email, by_lang, sql_count):
    """Page 1: Campaign KPIs + Email Performance."""
    els = []
    els.append(S.section_heading("Campaign KPIs & Email Performance"))

    # Overall Metrics
    els.append(S.section_heading("Overall Metrics", level=2))
    kpi_data = [
        ["Total Sent", _num(overall["sent"])],
        ["Total Opens", _num(overall["opens"])],
        ["Total Clicks", _num(overall["clicks"])],
        ["Open Rate", _pct_val(overall["open_rate"])],
        ["CTR", _pct_val(overall["ctr"])],
        ["CTOR", _pct_val(overall["ctor"])],
        ["Unique Sent", _num(overall["unique_sent"])],
        ["Unique Opens", _num(overall["unique_opens"])],
        ["Unique Clicks", _num(overall["unique_clicks"])],
        ["Unique Open Rate", _pct_val(overall["unique_open_rate"])],
        ["Unique CTR", _pct_val(overall["unique_ctr"])],
        ["Unsubs", _num(overall["unsubs"])],
        ["Bounces", _num(overall["bounces"])],
        ["Unsub Rate", _pct_val(overall["unsub_rate"])],
        ["Bounce Rate", _pct_val(overall["bounce_rate"])],
        ["SQLs", _num(sql_count)],
    ]
    els.append(S.branded_table(["Metric", "Value"], kpi_data,
                               [W - 1.5 * inch, 1.5 * inch]))

    # Performance by Email
    els.append(S.section_heading("Performance by Email", level=2))
    email_rows = []
    for enum, display in EMAIL_DISPLAY.items():
        d = by_email.get(enum, {})
        email_rows.append([
            display,
            _num(d.get("sent", 0)),
            _num(d.get("opens", 0)),
            _num(d.get("clicks", 0)),
            _pct_val(d.get("open_rate", 0)),
            _pct_val(d.get("ctr", 0)),
            _pct_val(d.get("ctor", 0)),
        ])
    els.append(S.branded_table(
        ["Email", "Sent", "Opens", "Clicks", "Open Rate", "CTR", "CTOR"],
        email_rows,
        [1.8 * inch, 0.6 * inch, 0.6 * inch, 0.6 * inch, 0.8 * inch, 0.6 * inch,
         W - 5.0 * inch], font_size=8))

    # Performance by Language
    els.append(S.section_heading("Performance by Language", level=2))
    lang_rows = []
    for lang in ["EN", "ES", "PT"]:
        d = by_lang.get(lang, {})
        lang_rows.append([
            lang,
            _num(d.get("sent", 0)),
            _num(d.get("opens", 0)),
            _pct_val(d.get("open_rate", 0)),
            _num(d.get("clicks", 0)),
            _pct_val(d.get("ctr", 0)),
            _pct_val(d.get("ctor", 0)),
        ])
    els.append(S.branded_table(
        ["Language", "Sent", "Opens", "Open Rate", "Clicks", "CTR", "CTOR"],
        lang_rows,
        [1.0 * inch, 0.7 * inch, 0.7 * inch, 0.85 * inch, 0.7 * inch, 0.7 * inch,
         W - 4.65 * inch], font_size=8))

    els.append(PageBreak())
    return els


def page_audience(audience_stats):
    """Page 2: Audience Performance."""
    els = []
    els.append(S.section_heading("Audience Performance"))

    rows = []
    for seg in AUDIENCE_SEGMENTS + ["Unassigned"]:
        d = audience_stats.get(seg, {})
        rows.append([
            seg,
            _num(d.get("sent", 0)),
            _num(d.get("opens", 0)),
            _num(d.get("clicks", 0)),
            _pct_val(d.get("open_rate", 0)),
            _pct_val(d.get("ctr", 0)),
            _pct_val(d.get("ctor", 0)),
        ])

    els.append(S.branded_table(
        ["Audience", "Sent", "Opens", "Clicks", "Open Rate", "CTR", "CTOR"],
        rows,
        [1.8 * inch, 0.6 * inch, 0.6 * inch, 0.6 * inch, 0.8 * inch, 0.6 * inch,
         W - 5.0 * inch], font_size=8))

    els.append(PageBreak())
    return els


def page_forms_pipeline(form_rows, stage_rows, top_recent):
    """Page 3: Forms + Pipeline."""
    els = []
    els.append(S.section_heading("Form Conversions & Pipeline"))

    # Form Conversions
    els.append(S.section_heading("Form Conversions", level=2))
    if form_rows:
        f_data = [[r["form"], _num(r["interactions"]), _num(r["unique_prospects"])]
                  for r in form_rows]
        els.append(S.branded_table(
            ["Form", "Interactions", "Unique Prospects"],
            f_data,
            [2.5 * inch, 1.2 * inch, W - 3.7 * inch]))
    else:
        els.append(S.body_text("No form submissions recorded yet."))

    # Opportunity Pipeline
    els.append(S.section_heading("Opportunity Pipeline", level=2))
    if stage_rows:
        total_count = sum(r["count"] for r in stage_rows)
        total_amount = sum(r["amount"] for r in stage_rows)
        p_data = [[r["stage"], _num(r["count"]), _usd(r["amount"])] for r in stage_rows]
        p_data.append(["<b>Total</b>", f"<b>{_num(total_count)}</b>",
                       f"<b>{_usd(total_amount)}</b>"])
        els.append(S.branded_table(
            ["Stage", "Count", "Amount"],
            p_data,
            [2.0 * inch, 1.0 * inch, W - 3.0 * inch]))
    else:
        els.append(S.body_text("No opportunities found for this campaign."))

    # Top 5 Recent
    if top_recent:
        els.append(S.section_heading("5 Most Recent Opportunities", level=2))
        r_data = [[r["name"][:40], r["account"][:25], r["stage"],
                   _usd(r["amount"]), r["created"]] for r in top_recent]
        els.append(S.branded_table(
            ["Name", "Account", "Stage", "Amount", "Created"],
            r_data,
            [1.8 * inch, 1.3 * inch, 1.2 * inch, 0.9 * inch, W - 5.2 * inch],
            font_size=7.5))

    els.append(PageBreak())
    return els


def page_daily_trends(daily_rows):
    """Page 4: Daily Trends (last 14 days)."""
    els = []
    els.append(S.section_heading("Daily Trends (Last 14 Days)"))

    if daily_rows:
        d_data = []
        for r in daily_rows:
            d_data.append([
                r["date"],
                _num(r["sent"]), _num(r["opens"]), _num(r["clicks"]),
                _num(r["unsubs"]), _num(r["bounces"]),
                _num(r["cum_sent"]), _num(r["cum_opens"]), _num(r["cum_clicks"]),
                f"{r['open_rate']:.1f}%", f"{r['ctr']:.1f}%",
            ])
        els.append(S.branded_table(
            ["Date", "Sent", "Opens", "Clicks", "Unsubs", "Bounces",
             "Cum. Sent", "Cum. Opens", "Cum. Clicks", "Open Rate %", "CTR %"],
            d_data,
            [0.8 * inch, 0.5 * inch, 0.5 * inch, 0.5 * inch, 0.5 * inch, 0.5 * inch,
             0.65 * inch, 0.65 * inch, 0.65 * inch, 0.65 * inch,
             W - 5.9 * inch],
            font_size=7))
    else:
        els.append(S.body_text("No daily activity data available yet."))

    return els


# ── Email Delivery ───────────────────────────────────────────────────────────

def send_pdf_email(pdf_path: str, generated_at: str):
    """Email the PDF as an attachment to subscribers via Gmail API."""
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build

    from dotenv import load_dotenv
    load_dotenv()

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
            log.info("  No Gmail credentials available - skipping email.")
            return

    service = build("gmail", "v1", credentials=creds)
    sender = os.environ.get("GMAIL_SENDER", "me")
    subscribers = [s.strip() for s in os.environ.get("SUBSCRIBERS", "").split(",") if s.strip()]
    cc = [c.strip() for c in os.environ.get("REPORT_CC", "").split(",") if c.strip()]

    if not subscribers:
        log.info("  No SUBSCRIBERS configured - skipping email.")
        return

    msg = MIMEMultipart("mixed")
    msg["Subject"] = f"PV Campaign 2026 Report - {generated_at}"
    msg["From"] = sender
    msg["To"] = ", ".join(subscribers)
    if cc:
        msg["Cc"] = ", ".join(cc)

    body_html = f"""\
<p>Hi team,</p>
<p>Attached is the latest <b>Private Vineyards Campaign Report</b> generated on {generated_at}.</p>
<p>This report pulls live data from Pardot (email performance, forms, audience segments)
and Salesforce (opportunities, pipeline) for the PV 2026 campaign.</p>
<p style="color: #888; font-size: 12px;">This is an automated report.</p>
"""
    msg.attach(MIMEText(body_html, "html"))

    pdf_data = Path(pdf_path).read_bytes()
    attachment = MIMEApplication(pdf_data, _subtype="pdf")
    attachment.add_header("Content-Disposition", "attachment", filename=Path(pdf_path).name)
    msg.attach(attachment)

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    service.users().messages().send(userId="me", body={"raw": raw}).execute()
    all_recipients = subscribers + cc
    log.info("  Report emailed to %d recipient(s).", len(all_recipients))


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    send_email = "--email" in sys.argv
    skip_audience = "--skip-audience" in sys.argv

    now = datetime.now()
    generated_at = now.strftime("%B %d, %Y at %H:%M")
    output_file = f"pv_dashboard_report_{now.strftime('%Y%m%d_%H%M')}.pdf"

    log.info("=" * 60)
    log.info("PV CAMPAIGN 2026 DASHBOARD REPORT GENERATOR")
    log.info("=" * 60)

    # Connect to Salesforce
    log.info("\nConnecting to Salesforce...")
    try:
        sf = sf_client.connect()
        sf_holder = [sf]
        log.info("  Connected to %s", sf.sf_instance)
    except Exception as e:
        log.error("  ERROR: %s", e)
        sys.exit(1)

    # Pardot uses SF session_id as Bearer token
    access_token = sf.session_id
    headers = _pardot_headers(access_token)

    # Fetch data
    activities = fetch_email_engagement(headers)

    prospect_audience = {}
    test_prospect_ids = set()
    if not skip_audience:
        prospect_audience, test_prospect_ids = fetch_audience_segments(headers)
    else:
        log.info("Skipping audience segment resolution (--skip-audience)")

    # Exclude test accounts from activities
    if test_prospect_ids:
        before = len(activities)
        activities = [a for a in activities if a.get("prospectId") not in test_prospect_ids]
        log.info("  Filtered %d test account activities (%d -> %d)",
                 before - len(activities), before, len(activities))

    form_activities, pv_forms = fetch_form_submissions(headers)

    # Exclude test accounts from form activities
    if test_prospect_ids:
        form_before = len(form_activities)
        form_activities = [a for a in form_activities if a.get("prospectId") not in test_prospect_ids]
        if form_before != len(form_activities):
            log.info("  Filtered %d test form activities", form_before - len(form_activities))

    opportunities = fetch_sf_opportunities(sf_holder)

    # Aggregate
    log.info("\nAggregating data...")
    overall, by_email, by_lang = aggregate_email_stats(activities)
    audience_stats = aggregate_audience_stats(activities, prospect_audience) if not skip_audience else {}
    form_agg = aggregate_forms(form_activities, pv_forms)
    stage_rows, top_recent = aggregate_pipeline(opportunities)
    daily_rows = aggregate_daily(activities)

    # Build PDF
    log.info("\nGenerating PDF: %s", output_file)
    doc = SimpleDocTemplate(
        output_file,
        pagesize=S.letter,
        leftMargin=S.MARGIN_LEFT,
        rightMargin=S.MARGIN_RIGHT,
        topMargin=S.MARGIN_TOP,
        bottomMargin=S.MARGIN_BOTTOM,
        title="PV Campaign 2026 Report",
        author="The Vines of Mendoza",
    )

    elements = []
    elements += page_cover(generated_at)
    elements += page_kpis_and_email(overall, by_email, by_lang, len(opportunities))
    if not skip_audience:
        elements += page_audience(audience_stats)
    elements += page_forms_pipeline(form_agg, stage_rows, top_recent)
    elements += page_daily_trends(daily_rows)

    def first_page(canvas, doc):
        S.cover_header_footer(canvas, doc)

    def later_pages(canvas, doc):
        S.header_footer(canvas, doc, title="PV Campaign 2026 Report")

    doc.build(elements, onFirstPage=first_page, onLaterPages=later_pages)
    log.info("\nDone! Created %s (%d pages)", output_file, doc.page)

    # Optionally email
    if send_email:
        log.info("\nEmailing PDF...")
        send_pdf_email(output_file, generated_at)

    log.info("=" * 60)


if __name__ == "__main__":
    main()
