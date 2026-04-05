#!/usr/bin/env python3
"""MKT Campaign Follow-Up Overview — management summary report."""

import argparse
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timezone, timedelta

from dotenv import load_dotenv

load_dotenv()

from src import sf_client, email_sender

# License types that indicate automated/non-human users
NON_HUMAN_LICENSES = {
    "Salesforce Integration",
    "Salesforce API Only System Integrations",
    "Identity",
    "Automated Process",
}

NON_HUMAN_USERNAMES = {
    "Automated Process",
}

BATCH_SIZE = 200

OPPS_SOQL = """
    SELECT Id, Name, StageName, Amount, OwnerId, Owner.Name, Owner.Email,
           AccountId, Account.Name, LeadSource, LastActivityDate, CreatedDate
    FROM Opportunity
    WHERE IsClosed = false AND LeadSource = 'Internal - MKT Campaign'
      AND (NOT Name LIKE '%Test%')
    ORDER BY Owner.Name, StageName
"""

TASKS_SOQL_TEMPLATE = """
    SELECT Id, WhatId, CreatedById, CreatedDate
    FROM Task
    WHERE WhatId IN ({ids})
"""

USERS_SOQL_TEMPLATE = """
    SELECT Id, Name, Profile.UserLicense.Name
    FROM User
    WHERE Id IN ({ids})
"""


def _ids_csv(ids: list[str]) -> str:
    return ",".join(f"'{id_}'" for id_ in ids)


def _batch_ids(ids: list[str]) -> list[list[str]]:
    return [ids[i:i + BATCH_SIZE] for i in range(0, len(ids), BATCH_SIZE)]


def _query_batched(sf_holder: list, template: str, ids: list[str]) -> list[dict]:
    results = []
    for batch in _batch_ids(ids):
        soql = template.format(ids=_ids_csv(batch))
        results.extend(sf_client.query(sf_holder, soql))
    return results


def _get_human_user_ids(sf_holder: list, user_ids: list[str]) -> set[str]:
    users = _query_batched(sf_holder, USERS_SOQL_TEMPLATE, user_ids)
    human_ids = set()
    for user in users:
        name = user.get("Name", "")
        if name in NON_HUMAN_USERNAMES:
            continue
        profile = user.get("Profile")
        if profile:
            user_license = profile.get("UserLicense")
            if user_license:
                license_name = user_license.get("Name", "")
                if license_name in NON_HUMAN_LICENSES:
                    continue
        human_ids.add(user["Id"])
    return human_ids


def _get_nested(record: dict, *keys, default=""):
    val = record
    for key in keys:
        if not isinstance(val, dict):
            return default
        val = val.get(key)
        if val is None:
            return default
    return val


def _is_stale(opp: dict, days: int = 7) -> bool:
    last_activity = opp.get("LastActivityDate")
    if not last_activity:
        return True
    try:
        last_dt = datetime.strptime(last_activity[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return last_dt < datetime.now(timezone.utc) - timedelta(days=days)
    except (ValueError, TypeError):
        return True


def fetch_mkt_campaign_data(sf_holder: list) -> tuple[list[dict], dict[str, int]]:
    """Fetch MKT Campaign opps and return (opps, touch_counts)."""
    opps = sf_client.query(sf_holder, OPPS_SOQL)
    if not opps:
        return [], {}

    opp_ids = [o["Id"] for o in opps]

    tasks = _query_batched(sf_holder, TASKS_SOQL_TEMPLATE, opp_ids)

    all_user_ids = {t["CreatedById"] for t in tasks}
    human_ids = _get_human_user_ids(sf_holder, list(all_user_ids)) if all_user_ids else set()

    touch_counts = defaultdict(int)
    for t in tasks:
        if t["CreatedById"] in human_ids:
            touch_counts[t["WhatId"]] += 1

    for opp in opps:
        opp["_touch_count"] = touch_counts.get(opp["Id"], 0)

    return opps, dict(touch_counts)


def _format_amount(amount) -> str:
    if amount is None:
        return "—"
    return f"${amount:,.0f}"


def _format_date(dt_str) -> str:
    if not dt_str:
        return "—"
    return dt_str[:10]


def _days_since(dt_str) -> str:
    if not dt_str:
        return "Never"
    try:
        last_dt = datetime.strptime(dt_str[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        days = (datetime.now(timezone.utc) - last_dt).days
        return f"{days}d"
    except (ValueError, TypeError):
        return "—"


def _render_stale_detail(opps: list[dict], instance_url: str) -> str:
    """Render a detail table of all stale opportunities."""
    stale_opps = [o for o in opps if _is_stale(o)]
    if not stale_opps:
        return ""

    # Sort by owner name, then days since activity (worst first)
    def _sort_key(o):
        owner = _get_nested(o, "Owner", "Name") or "ZZZ"
        last = o.get("LastActivityDate") or "0000-00-00"
        return (owner.lower(), last)

    stale_opps.sort(key=_sort_key)

    rows = []
    for i, opp in enumerate(stale_opps):
        bg = "#fff8f0" if i % 2 == 0 else "#ffffff"
        opp_id = opp.get("Id", "")
        name = opp.get("Name", "—")
        if instance_url and opp_id:
            opp_link = f'<a href="{instance_url}/lightning/r/Opportunity/{opp_id}/view" style="color:#722F37;text-decoration:none;font-weight:500;">{name}</a>'
        else:
            opp_link = name
        owner = _get_nested(opp, "Owner", "Name") or "—"
        account = _get_nested(opp, "Account", "Name") or "—"
        stage = opp.get("StageName", "—")
        amount = _format_amount(opp.get("Amount"))
        last_activity = _format_date(opp.get("LastActivityDate"))
        days = _days_since(opp.get("LastActivityDate"))
        touches = opp.get("_touch_count", 0)

        rows.append(f"""\
      <tr style="background:{bg};">
        <td style="padding:8px 12px;border-bottom:1px solid #eee;">{opp_link}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;">{owner}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;">{account}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;">{stage}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;text-align:right;">{amount}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;">{last_activity}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;text-align:center;color:#c0392b;font-weight:600;">{days}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;text-align:center;">{touches}</td>
      </tr>""")

    rows_html = "\n".join(rows)

    return f"""\
    <h3 style="color:#c0392b;margin:28px 0 12px;font-size:15px;">Opportunities Needing Attention ({len(stale_opps)})</h3>
    <p style="color:#888;font-size:13px;font-style:italic;margin-bottom:12px;">
      These opportunities have had no activity in the last 7 days.
    </p>
    <table style="width:100%;border-collapse:collapse;font-size:14px;margin-bottom:24px;">
      <thead>
        <tr style="background:#c0392b;color:#fff;">
          <th style="padding:10px 12px;text-align:left;">Opportunity</th>
          <th style="padding:10px 12px;text-align:left;">Owner</th>
          <th style="padding:10px 12px;text-align:left;">Account</th>
          <th style="padding:10px 12px;text-align:left;">Stage</th>
          <th style="padding:10px 12px;text-align:right;">Amount</th>
          <th style="padding:10px 12px;text-align:left;">Last Activity</th>
          <th style="padding:10px 12px;text-align:center;">Days Since</th>
          <th style="padding:10px 12px;text-align:center;">Touches</th>
        </tr>
      </thead>
      <tbody>
{rows_html}
      </tbody>
    </table>"""


def render_overview_report(opps: list[dict], instance_url: str = "") -> tuple[str, str]:
    """Render management overview HTML email. Returns (subject, html)."""
    today = date.today().strftime("%B %d, %Y")

    total_opps = len(opps)
    owners = {_get_nested(o, "Owner", "Name") for o in opps}
    total_reps = len(owners - {""})
    needs_attention = sum(1 for o in opps if _is_stale(o))
    total_touches = sum(o.get("_touch_count", 0) for o in opps)

    subject = f"MKT Campaign Overview — {today} ({total_opps} opportunities)"

    # Build pivot table: Owner x Stage
    pivot = defaultdict(lambda: {"opps": 0, "touches": 0, "stale": 0})
    owner_totals = defaultdict(lambda: {"opps": 0, "touches": 0, "stale": 0})

    for opp in opps:
        owner = _get_nested(opp, "Owner", "Name") or "Unknown"
        stage = opp.get("StageName", "Unknown")
        touches = opp.get("_touch_count", 0)
        stale = 1 if _is_stale(opp) else 0

        pivot[(owner, stage)]["opps"] += 1
        pivot[(owner, stage)]["touches"] += touches
        pivot[(owner, stage)]["stale"] += stale
        owner_totals[owner]["opps"] += 1
        owner_totals[owner]["touches"] += touches
        owner_totals[owner]["stale"] += stale

    # Sort by owner name, then stage
    sorted_keys = sorted(pivot.keys(), key=lambda k: (k[0].lower(), k[1].lower()))

    # Build pivot rows with subtotals
    table_rows = []
    current_owner = None
    for owner, stage in sorted_keys:
        if owner != current_owner:
            # Insert subtotal for previous owner
            if current_owner is not None:
                t = owner_totals[current_owner]
                stale_style = "color:#c0392b;" if t['stale'] > 0 else ""
                table_rows.append(f"""\
      <tr style="background:#f0ece4;font-weight:600;">
        <td style="padding:8px 12px;border-bottom:2px solid #d4c5a9;" colspan="2">{current_owner} — Subtotal</td>
        <td style="padding:8px 12px;border-bottom:2px solid #d4c5a9;text-align:center;">{t['opps']}</td>
        <td style="padding:8px 12px;border-bottom:2px solid #d4c5a9;text-align:center;{stale_style}">{t['stale']}</td>
        <td style="padding:8px 12px;border-bottom:2px solid #d4c5a9;text-align:center;">{t['touches']}</td>
      </tr>""")
            current_owner = owner

        d = pivot[(owner, stage)]
        bg = "#ffffff" if len(table_rows) % 2 == 0 else "#fafafa"
        stale_style = "color:#c0392b;font-weight:600;" if d['stale'] > 0 else ""
        table_rows.append(f"""\
      <tr style="background:{bg};">
        <td style="padding:8px 12px;border-bottom:1px solid #eee;">{owner}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;">{stage}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;text-align:center;">{d['opps']}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;text-align:center;{stale_style}">{d['stale']}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;text-align:center;">{d['touches']}</td>
      </tr>""")

    # Final owner subtotal
    if current_owner is not None:
        t = owner_totals[current_owner]
        stale_style = "color:#c0392b;" if t['stale'] > 0 else ""
        table_rows.append(f"""\
      <tr style="background:#f0ece4;font-weight:600;">
        <td style="padding:8px 12px;border-bottom:2px solid #d4c5a9;" colspan="2">{current_owner} — Subtotal</td>
        <td style="padding:8px 12px;border-bottom:2px solid #d4c5a9;text-align:center;">{t['opps']}</td>
        <td style="padding:8px 12px;border-bottom:2px solid #d4c5a9;text-align:center;{stale_style}">{t['stale']}</td>
        <td style="padding:8px 12px;border-bottom:2px solid #d4c5a9;text-align:center;">{t['touches']}</td>
      </tr>""")

    # Grand total
    table_rows.append(f"""\
      <tr style="background:#4A0E0E;color:#fff;font-weight:700;">
        <td style="padding:10px 12px;" colspan="2">Grand Total</td>
        <td style="padding:10px 12px;text-align:center;">{total_opps}</td>
        <td style="padding:10px 12px;text-align:center;">{needs_attention}</td>
        <td style="padding:10px 12px;text-align:center;">{total_touches}</td>
      </tr>""")

    rows_html = "\n".join(table_rows)

    html = f"""\
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f5f5f5;">
<div style="font-family:'Segoe UI',Arial,sans-serif;max-width:900px;margin:0 auto;background:#fff;">
  <div style="background:linear-gradient(135deg,#4A0E0E,#722F37);color:#fff;padding:30px 35px;">
    <h1 style="margin:0 0 5px;font-size:22px;font-weight:600;">MKT Campaign Follow-Up Overview</h1>
    <p style="margin:0;opacity:0.85;font-size:13px;">Generated {today}</p>
  </div>
  <div style="padding:25px 35px 35px;">

    <div style="display:flex;gap:16px;margin:0 0 25px;flex-wrap:wrap;">
      <div style="flex:1;min-width:120px;background:#f8f9fa;border-radius:8px;padding:18px 20px;text-align:center;">
        <div style="font-size:28px;font-weight:700;color:#2c3e50;">{total_opps}</div>
        <div style="font-size:11px;color:#7f8c8d;text-transform:uppercase;letter-spacing:0.5px;margin-top:4px;">Active Opps</div>
      </div>
      <div style="flex:1;min-width:120px;background:#f8f9fa;border-radius:8px;padding:18px 20px;text-align:center;">
        <div style="font-size:28px;font-weight:700;color:#2c3e50;">{total_reps}</div>
        <div style="font-size:11px;color:#7f8c8d;text-transform:uppercase;letter-spacing:0.5px;margin-top:4px;">Sales Reps</div>
      </div>
      <div style="flex:1;min-width:120px;background:#fdedec;border:1px solid #e74c3c;border-radius:8px;padding:18px 20px;text-align:center;">
        <div style="font-size:28px;font-weight:700;color:#c0392b;">{needs_attention}</div>
        <div style="font-size:11px;color:#7f8c8d;text-transform:uppercase;letter-spacing:0.5px;margin-top:4px;">Needs Attention</div>
      </div>
      <div style="flex:1;min-width:120px;background:#f8f9fa;border-radius:8px;padding:18px 20px;text-align:center;">
        <div style="font-size:28px;font-weight:700;color:#2c3e50;">{total_touches}</div>
        <div style="font-size:11px;color:#7f8c8d;text-transform:uppercase;letter-spacing:0.5px;margin-top:4px;">Total Touches</div>
      </div>
    </div>

    <p style="color:#888;font-size:13px;font-style:italic;margin-bottom:16px;">
      "Needs Attention" counts opportunities with no activity in the last 7 days.
      Touch counts reflect human interactions (tasks) only — automated system activity is excluded.
    </p>

    <h3 style="color:#333;margin:20px 0 12px;font-size:15px;">Opportunities by Owner &amp; Stage</h3>
    <table style="width:100%;border-collapse:collapse;font-size:14px;">
      <thead>
        <tr style="background:#722F37;color:#fff;">
          <th style="padding:10px 12px;text-align:left;">Opp Owner</th>
          <th style="padding:10px 12px;text-align:left;">Stage</th>
          <th style="padding:10px 12px;text-align:center;">Total Opps</th>
          <th style="padding:10px 12px;text-align:center;">Needs Attention</th>
          <th style="padding:10px 12px;text-align:center;">Total Touches</th>
        </tr>
      </thead>
      <tbody>
{rows_html}
      </tbody>
    </table>

{_render_stale_detail(opps, instance_url)}
  </div>
  <div style="padding:20px 35px;background:#f8f9fa;font-size:11px;color:#95a5a6;border-top:1px solid #eee;">
    This is an automated report. Contact your administrator if you wish to unsubscribe.
  </div>
</div>
</body>
</html>"""

    return subject, html


def main():
    parser = argparse.ArgumentParser(description="MKT Campaign management overview report")
    parser.add_argument("--email", help="Override all recipients (for testing)")
    args = parser.parse_args()

    # Load config
    managers_raw = os.environ.get("MKT_CAMPAIGN_MANAGERS", "")
    managers = [e.strip() for e in managers_raw.split(",") if e.strip()]

    if not args.email and not managers:
        print("No managers configured (set MKT_CAMPAIGN_MANAGERS). Skipping.")
        sys.exit(0)

    # Connect to Salesforce
    print("Connecting to Salesforce...")
    sf = sf_client.connect()
    sf_holder = [sf]
    print(f"Connected to {sf.sf_instance}")

    # Fetch data
    print("Querying MKT Campaign opportunities...")
    opps, touch_counts = fetch_mkt_campaign_data(sf_holder)
    print(f"Found {len(opps)} active MKT Campaign opportunities")

    if not opps:
        print("No opportunities found. Nothing to send.")
        return

    # Render overview
    instance_url = f"https://{sf.sf_instance}"
    subject, html = render_overview_report(opps, instance_url)

    if args.email:
        recipients = [args.email]
        subject = f"[TEST] {subject}"
        cc = []
    else:
        recipients = managers
        cc = []

    print(f"Sending overview to {', '.join(recipients)}...")
    email_sender.send_report(subject, html, recipients, cc=cc if cc else None)
    print("Done. Overview report sent.")


if __name__ == "__main__":
    main()
