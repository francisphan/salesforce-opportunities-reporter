#!/usr/bin/env python3
"""MKT Campaign Follow-Up Report — per-Sales-Rep individual report."""

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

SERVICE_TYPE = "TVOM Vineyards"
LEAD_SOURCE = "Internal - MKT Campaign"
CAMPAIGN_START = "2026-03-31T00:00:00Z"

OPPS_SOQL = f"""
    SELECT Id, Name, StageName, Amount, OwnerId, Owner.Name, Owner.Email,
           AccountId, Account.Name, LeadSource, LastActivityDate, CreatedDate
    FROM Opportunity
    WHERE IsClosed = false AND Service__c = '{SERVICE_TYPE}'
      AND LeadSource = '{LEAD_SOURCE}'
      AND CreatedDate >= {CAMPAIGN_START}
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


def fetch_mkt_campaign_data(sf_holder: list) -> tuple[list[dict], dict[str, int]]:
    """Fetch MKT Campaign opps and return (opps, touch_counts)."""
    opps = sf_client.query(sf_holder, OPPS_SOQL)
    if not opps:
        return [], {}

    opp_ids = [o["Id"] for o in opps]

    # Get Tasks linked to these opps
    tasks = _query_batched(sf_holder, TASKS_SOQL_TEMPLATE, opp_ids)

    # Filter to human-created tasks
    all_user_ids = {t["CreatedById"] for t in tasks}
    human_ids = _get_human_user_ids(sf_holder, list(all_user_ids)) if all_user_ids else set()

    # Count human touches per opportunity
    touch_counts = defaultdict(int)
    for t in tasks:
        if t["CreatedById"] in human_ids:
            touch_counts[t["WhatId"]] += 1

    # Enrich opps with touch counts
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
    """True if last activity is > days ago or has no activity."""
    last_activity = opp.get("LastActivityDate")
    if not last_activity:
        return True
    try:
        last_dt = datetime.strptime(last_activity[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return last_dt < datetime.now(timezone.utc) - timedelta(days=days)
    except (ValueError, TypeError):
        return True


def _render_opp_table(opps: list[dict], instance_url: str, header_bg: str) -> str:
    if not opps:
        return ""

    rows = []
    for i, opp in enumerate(opps):
        bg = "#f9f9f9" if i % 2 == 0 else "#ffffff"
        opp_url = f"{instance_url}/lightning/r/Opportunity/{opp['Id']}/view"
        name = opp.get("Name", "—")
        account = _get_nested(opp, "Account", "Name") or "—"
        stage = opp.get("StageName", "—")
        amount = _format_amount(opp.get("Amount"))
        created = _format_date(opp.get("CreatedDate"))
        last_activity = _format_date(opp.get("LastActivityDate"))
        touches = opp.get("_touch_count", 0)

        rows.append(f"""\
      <tr style="background:{bg};">
        <td style="padding:8px 12px;border-bottom:1px solid #eee;">
          <a href="{opp_url}" style="color:#722F37;text-decoration:none;font-weight:500;">{name}</a>
        </td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;">{account}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;">{stage}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;text-align:right;">{amount}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;">{created}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;">{last_activity}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;text-align:center;">{touches}</td>
      </tr>""")

    rows_html = "\n".join(rows)

    return f"""\
    <table style="width:100%;border-collapse:collapse;font-size:14px;margin-bottom:24px;">
      <thead>
        <tr style="background:{header_bg};color:#fff;">
          <th style="padding:10px 12px;text-align:left;">Opportunity</th>
          <th style="padding:10px 12px;text-align:left;">Account</th>
          <th style="padding:10px 12px;text-align:left;">Stage</th>
          <th style="padding:10px 12px;text-align:right;">Amount</th>
          <th style="padding:10px 12px;text-align:left;">Created</th>
          <th style="padding:10px 12px;text-align:left;">Last Activity</th>
          <th style="padding:10px 12px;text-align:center;">Touches</th>
        </tr>
      </thead>
      <tbody>
{rows_html}
      </tbody>
    </table>"""


def render_individual_report(opps: list[dict], instance_url: str, owner_name: str) -> tuple[str, str]:
    """Render HTML email for one sales rep. Returns (subject, html)."""
    today = date.today().strftime("%B %d, %Y")
    stale = [o for o in opps if _is_stale(o)]
    subject = f"MKT Campaign Follow-Up — {today} ({len(opps)} opportunities)"

    all_table = _render_opp_table(opps, instance_url, header_bg="#722F37")

    stale_section = ""
    if stale:
        stale_section = f"""\
    <div style="margin-top:8px;padding:16px 20px;background:#fff8f0;border-left:4px solid #e67e22;border-radius:0 4px 4px 0;margin-bottom:16px;">
      <p style="margin:0 0 12px;color:#555;font-size:14px;">
        Below are the <strong style="color:#c0392b;">{len(stale)}</strong> opportunit{"y" if len(stale) == 1 else "ies"} with no activity in the last 7 days:
      </p>
    </div>
{_render_opp_table(stale, instance_url, header_bg="#c0392b")}"""

    html = f"""\
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f5f5f5;">
<div style="font-family:'Segoe UI',Arial,sans-serif;max-width:900px;margin:0 auto;background:#fff;">
  <div style="background:linear-gradient(135deg,#4A0E0E,#722F37);color:#fff;padding:30px 35px;">
    <h1 style="margin:0 0 5px;font-size:22px;font-weight:600;">MKT Campaign Follow-Up Report</h1>
    <p style="margin:0;opacity:0.85;font-size:13px;">Generated {today}</p>
  </div>
  <div style="padding:25px 35px 35px;">
    <p style="color:#333;font-size:15px;">Hi {owner_name},</p>
    <p style="color:#555;font-size:14px;margin-bottom:20px;">
      Here is the list of your <strong>{len(opps)}</strong> active opportunit{"y" if len(opps) == 1 else "ies"} coming from the marketing campaign.
    </p>

    <h3 style="color:#333;margin:20px 0 12px;font-size:15px;">All Active Opportunities</h3>
{all_table}
{stale_section}
  </div>
  <div style="padding:20px 35px;background:#f8f9fa;font-size:11px;color:#95a5a6;border-top:1px solid #eee;">
    This is an automated report. Contact your administrator if you wish to unsubscribe.
  </div>
</div>
</body>
</html>"""

    return subject, html


def main():
    parser = argparse.ArgumentParser(description="MKT Campaign individual follow-up report")
    parser.add_argument("--email", help="Override all recipients (for testing)")
    args = parser.parse_args()

    # Load config
    subscribers_raw = os.environ.get("MKT_CAMPAIGN_SUBSCRIBERS", "")
    subscribers = {e.strip().lower() for e in subscribers_raw.split(",") if e.strip()}
    cc_raw = os.environ.get("MKT_CAMPAIGN_CC", "")
    cc = [e.strip() for e in cc_raw.split(",") if e.strip()]

    if not args.email and not subscribers:
        print("No subscribers configured (set MKT_CAMPAIGN_SUBSCRIBERS). Skipping.")
        sys.exit(0)

    # Connect to Salesforce
    print("Connecting to Salesforce...")
    sf = sf_client.connect()
    sf_holder = [sf]
    instance_url = f"https://{sf.sf_instance}"
    print(f"Connected to {sf.sf_instance}")

    # Fetch data
    print("Querying MKT Campaign opportunities...")
    opps, touch_counts = fetch_mkt_campaign_data(sf_holder)
    print(f"Found {len(opps)} active MKT Campaign opportunities")

    if not opps:
        print("No opportunities found. Nothing to send.")
        return

    # Group by owner email
    opps_by_owner = defaultdict(list)
    for opp in opps:
        owner_email = _get_nested(opp, "Owner", "Email")
        if owner_email:
            opps_by_owner[owner_email.lower()].append(opp)

    # Determine which owners to send to
    if args.email:
        # Testing mode: send all reports to the override email
        target_owners = set(opps_by_owner.keys())
    else:
        # Production: only send to subscribed reps who have opps
        target_owners = subscribers & set(opps_by_owner.keys())

    sent = 0
    for owner_email in sorted(target_owners):
        owner_opps = opps_by_owner[owner_email]
        owner_name = _get_nested(owner_opps[0], "Owner", "Name") or "there"

        subject, html = render_individual_report(owner_opps, instance_url, owner_name)

        if args.email:
            recipients = [args.email]
            send_cc = []
            subject = f"[TEST for {owner_name}] {subject}"
        else:
            recipients = [owner_email]
            send_cc = cc

        print(f"Sending {len(owner_opps)} opps report for {owner_name} to {', '.join(recipients)}...")
        email_sender.send_report(subject, html, recipients, cc=send_cc if send_cc else None)
        sent += 1

    print(f"Done. Sent {sent} individual report(s).")


if __name__ == "__main__":
    main()
