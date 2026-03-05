"""Lead cleanup report: flag Leads whose email already exists as an Account or Contact."""

from collections import Counter, defaultdict
from datetime import date, datetime

from src import sf_client, email_sender

LEAD_SOQL = """
    SELECT Id, Name, Email, Company, Phone, Status, LeadSource,
           CreatedDate, LastModifiedDate, OwnerId, Owner.Name,
           IsConverted, ConvertedAccountId, ConvertedContactId
    FROM Lead
    WHERE Email != null
"""

ACCOUNT_SOQL = """
    SELECT Id, Name, PersonEmail, Phone, CreatedDate, OwnerId, Owner.Name
    FROM Account
    WHERE PersonEmail != null
"""

CONTACT_SOQL = """
    SELECT Id, Name, Email, Phone, AccountId, Account.Name,
           CreatedDate, OwnerId, Owner.Name
    FROM Contact
    WHERE Email != null
"""


def _lower(val):
    return (val or "").strip().lower()


def _days_ago(date_str):
    """Return number of days since a Salesforce datetime string."""
    if not date_str:
        return None
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return (datetime.now(dt.tzinfo) - dt).days
    except Exception:
        return None


def _owner_name(record):
    owner = record.get("Owner") or {}
    return owner.get("Name", "—")


def _account_name(record):
    acc = record.get("Account") or {}
    return acc.get("Name", "—")


def _build_email_index(records, email_field):
    """Build a dict of {lowercased_email: [records]}."""
    index = defaultdict(list)
    for r in records:
        email = _lower(r.get(email_field))
        if email:
            index[email].append(r)
    return index


def build_report_html(matches, lead_only_dupes, instance_url):
    """Build the HTML email.

    matches: list of dicts with lead info + matching account/contact info
    lead_only_dupes: list of (email, [leads]) for leads that are dupes of each other but have no Account/Contact match
    """
    today = date.today().strftime("%B %d, %Y")

    # Categorise matches
    open_matches = [m for m in matches if not m["lead_is_converted"] and m["lead_status"] not in ("Closed", "Disqualified")]
    converted_matches = [m for m in matches if m["lead_is_converted"]]
    closed_matches = [m for m in matches if not m["lead_is_converted"] and m["lead_status"] in ("Closed", "Disqualified")]

    # Breakdown by what they match
    match_account = [m for m in open_matches if m["matching_accounts"]]
    match_contact = [m for m in open_matches if m["matching_contacts"]]
    match_both = [m for m in open_matches if m["matching_accounts"] and m["matching_contacts"]]

    # Status distribution of open matches
    status_dist = Counter(m["lead_status"] for m in open_matches)

    html = f"""\
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  body {{ font-family: 'Segoe UI', Arial, sans-serif; color: #2c3e50; margin: 0; padding: 0; background: #f5f6fa; }}
  .container {{ max-width: 1020px; margin: 0 auto; background: #fff; }}
  .header {{ background: linear-gradient(135deg, #27ae60, #2ecc71); color: white; padding: 30px 35px; }}
  .header h1 {{ margin: 0 0 5px; font-size: 22px; font-weight: 600; }}
  .header p {{ margin: 0; opacity: 0.85; font-size: 13px; }}
  .content {{ padding: 25px 35px 35px; }}
  .summary-cards {{ display: flex; gap: 16px; margin: 0 0 25px; flex-wrap: wrap; }}
  .card {{ flex: 1; min-width: 130px; background: #f8f9fa; border-radius: 8px; padding: 18px 20px; text-align: center; }}
  .card .number {{ font-size: 28px; font-weight: 700; color: #2c3e50; }}
  .card .label {{ font-size: 11px; color: #7f8c8d; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 4px; }}
  .card.danger {{ background: #fdedec; border: 1px solid #e74c3c; }}
  .card.warning {{ background: #fef9e7; border: 1px solid #f39c12; }}
  .card.info {{ background: #eaf4fb; border: 1px solid #3498db; }}
  .card.success {{ background: #d5f5e3; border: 1px solid #27ae60; }}
  h2 {{ color: #2c3e50; font-size: 16px; margin: 28px 0 12px; padding-bottom: 6px; border-bottom: 2px solid #27ae60; }}
  h3 {{ color: #34495e; font-size: 14px; margin: 22px 0 8px; }}
  table {{ border-collapse: collapse; width: 100%; margin: 10px 0 20px; font-size: 12px; }}
  th {{ background: #f8f9fa; color: #2c3e50; font-weight: 600; text-align: left; padding: 8px 10px; border-bottom: 2px solid #dee2e6; white-space: nowrap; }}
  td {{ padding: 6px 10px; border-bottom: 1px solid #eee; vertical-align: top; }}
  tr:hover td {{ background: #f8f9fa; }}
  .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  a {{ color: #2a6496; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .callout {{ background: #eaf4fb; border-left: 4px solid #3498db; padding: 14px 18px; margin: 15px 0; border-radius: 0 4px 4px 0; font-size: 13px; line-height: 1.5; }}
  .callout.action {{ border-left-color: #e74c3c; background: #fdedec; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 3px; font-size: 11px; font-weight: 600; margin: 1px 2px; }}
  .badge-account {{ background: #fdebd0; color: #e67e22; }}
  .badge-contact {{ background: #d6eaf8; color: #2980b9; }}
  .badge-open {{ background: #d5f5e3; color: #27ae60; }}
  .badge-closed {{ background: #f5f5f5; color: #95a5a6; }}
  .badge-old {{ background: #fadbd8; color: #c0392b; }}
  .footer {{ padding: 20px 35px; background: #f8f9fa; font-size: 11px; color: #95a5a6; border-top: 1px solid #eee; }}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h1>Lead Cleanup Report</h1>
    <p>Leads whose email already exists as an Account or Contact &bull; Generated {today}</p>
  </div>
  <div class="content">

    <div class="summary-cards">
      <div class="card danger">
        <div class="number">{len(open_matches):,}</div>
        <div class="label">Open Leads w/ Match</div>
      </div>
      <div class="card warning">
        <div class="number">{len(match_both):,}</div>
        <div class="label">Match Both Acct+Contact</div>
      </div>
      <div class="card info">
        <div class="number">{len(converted_matches):,}</div>
        <div class="label">Already Converted</div>
      </div>
      <div class="card">
        <div class="number">{len(lead_only_dupes):,}</div>
        <div class="label">Lead-Only Dupes</div>
      </div>
    </div>

    <div class="callout action">
      <strong>Action needed:</strong> {len(open_matches):,} open Leads already have a matching Account or Contact
      (by email). These are likely unconverted leads that should be converted to link to the existing record,
      or deleted if stale. {len(converted_matches):,} additional Leads are already marked as converted.
    </div>
"""

    # ── Status breakdown ──
    html += "    <h2>Open Matches by Lead Status</h2>\n"
    html += "    <table>\n"
    html += '      <tr><th>Lead Status</th><th class="num">Count</th></tr>\n'
    for status, count in status_dist.most_common():
        html += f'      <tr><td>{status}</td><td class="num">{count:,}</td></tr>\n'
    html += "    </table>\n"

    # ── Age breakdown ──
    age_buckets = {"< 30 days": 0, "30–90 days": 0, "90–365 days": 0, "1–2 years": 0, "2+ years": 0}
    for m in open_matches:
        days = m["lead_age_days"]
        if days is None:
            continue
        if days < 30:
            age_buckets["< 30 days"] += 1
        elif days < 90:
            age_buckets["30–90 days"] += 1
        elif days < 365:
            age_buckets["90–365 days"] += 1
        elif days < 730:
            age_buckets["1–2 years"] += 1
        else:
            age_buckets["2+ years"] += 1

    html += "    <h2>Open Matches by Lead Age</h2>\n"
    html += "    <table>\n"
    html += '      <tr><th>Age</th><th class="num">Count</th></tr>\n'
    for bucket, count in age_buckets.items():
        html += f'      <tr><td>{bucket}</td><td class="num">{count:,}</td></tr>\n'
    html += "    </table>\n"

    # ── Owner breakdown ──
    owner_dist = Counter(m["lead_owner"] for m in open_matches)
    html += "    <h2>Open Matches by Lead Owner</h2>\n"
    html += "    <table>\n"
    html += '      <tr><th>Owner</th><th class="num">Count</th></tr>\n'
    for owner, count in owner_dist.most_common():
        html += f'      <tr><td>{owner}</td><td class="num">{count:,}</td></tr>\n'
    html += "    </table>\n"

    # ── Detail table: open matches ──
    html += f'    <h2>Open Leads with Existing Account/Contact ({len(open_matches):,})</h2>\n'
    html += '    <div class="callout">Sorted by lead age (oldest first). These are the strongest candidates for conversion or deletion.</div>\n'
    html += "    <table>\n"
    html += (
        "      <tr>"
        "<th>Lead</th>"
        "<th>Email</th>"
        "<th>Status</th>"
        "<th>Owner</th>"
        "<th>Created</th>"
        "<th>Age</th>"
        "<th>Matches</th>"
        "<th>Matching Account/Contact</th>"
        "</tr>\n"
    )

    # Sort by age descending (oldest first)
    open_sorted = sorted(open_matches, key=lambda m: -(m["lead_age_days"] or 0))

    for m in open_sorted[:500]:
        lead_link = f'<a href="{instance_url}/lightning/r/Lead/{m["lead_id"]}/view">{m["lead_name"]}</a>'
        days = m["lead_age_days"]
        age_str = f"{days:,}d" if days is not None else "—"
        age_badge = ' <span class="badge badge-old">old</span>' if days and days > 365 else ""

        # Build match badges and names
        match_badges = ""
        match_names = []
        if m["matching_accounts"]:
            match_badges += '<span class="badge badge-account">Account</span> '
            for a in m["matching_accounts"]:
                acc_link = f'<a href="{instance_url}/lightning/r/Account/{a["Id"]}/view">{a.get("Name", "—")}</a>'
                match_names.append(f"{acc_link} (Owner: {_owner_name(a)})")
        if m["matching_contacts"]:
            match_badges += '<span class="badge badge-contact">Contact</span> '
            for c in m["matching_contacts"]:
                ct_link = f'<a href="{instance_url}/lightning/r/Contact/{c["Id"]}/view">{c.get("Name", "—")}</a>'
                acct = _account_name(c)
                match_names.append(f"{ct_link} (Acct: {acct})")

        html += (
            f"      <tr>"
            f"<td>{lead_link}</td>"
            f"<td>{m['email']}</td>"
            f"<td>{m['lead_status']}</td>"
            f"<td>{m['lead_owner']}</td>"
            f"<td>{m['lead_created'][:10] if m['lead_created'] else '—'}</td>"
            f"<td>{age_str}{age_badge}</td>"
            f"<td>{match_badges}</td>"
            f"<td>{'<br>'.join(match_names)}</td>"
            f"</tr>\n"
        )

    if len(open_sorted) > 500:
        html += f'      <tr><td colspan="8" style="color:#7f8c8d; font-style:italic;">... and {len(open_sorted) - 500} more not shown</td></tr>\n'

    html += "    </table>\n"

    # ── Lead-only duplicates (no Account/Contact match) ──
    if lead_only_dupes:
        total_lead_dupes = sum(len(leads) for _, leads in lead_only_dupes)
        html += f'    <h2>Lead-Only Duplicates ({len(lead_only_dupes):,} groups, {total_lead_dupes:,} leads)</h2>\n'
        html += '    <div class="callout">Leads that share an email with other Leads but have no matching Account or Contact. These should be merged into a single Lead.</div>\n'
        html += "    <table>\n"
        html += (
            "      <tr>"
            "<th>Email</th>"
            '<th class="num">Count</th>'
            "<th>Lead Names</th>"
            "<th>Statuses</th>"
            "<th>Oldest</th>"
            "</tr>\n"
        )

        lead_dupes_sorted = sorted(lead_only_dupes, key=lambda x: -len(x[1]))
        for email, leads in lead_dupes_sorted[:200]:
            names = []
            for l in leads:
                link = f'<a href="{instance_url}/lightning/r/Lead/{l["Id"]}/view">{l.get("Name", "—")}</a>'
                names.append(link)
            statuses = ", ".join(sorted(set(l.get("Status", "—") for l in leads)))
            dates = sorted(l.get("CreatedDate", "") for l in leads if l.get("CreatedDate"))
            oldest = dates[0][:10] if dates else "—"

            html += (
                f"      <tr>"
                f"<td>{email}</td>"
                f'<td class="num">{len(leads)}</td>'
                f"<td>{', '.join(names)}</td>"
                f"<td>{statuses}</td>"
                f"<td>{oldest}</td>"
                f"</tr>\n"
            )

        if len(lead_dupes_sorted) > 200:
            html += f'      <tr><td colspan="5" style="color:#7f8c8d; font-style:italic;">... and {len(lead_dupes_sorted) - 200} more not shown</td></tr>\n'
        html += "    </table>\n"

    html += """  </div>
  <div class="footer">
    This is an automated read-only report. No records were modified. Data sourced from Salesforce Lead, Account, and Contact objects.
  </div>
</div>
</body>
</html>"""

    return html


def main():
    print("Connecting to Salesforce...")
    sf = sf_client.connect()
    sf_holder = [sf]
    instance_url = f"https://{sf.sf_instance}"
    print(f"Connected to {sf.sf_instance}")

    # Query all three objects
    print("Querying Leads...")
    leads = sf_client.query(sf_holder, LEAD_SOQL)
    print(f"  {len(leads):,} leads with email")

    print("Querying Accounts...")
    accounts = sf_client.query(sf_holder, ACCOUNT_SOQL)
    print(f"  {len(accounts):,} accounts with email")

    print("Querying Contacts...")
    contacts = sf_client.query(sf_holder, CONTACT_SOQL)
    print(f"  {len(contacts):,} contacts with email")

    # Build email indexes for accounts and contacts
    account_index = _build_email_index(accounts, "PersonEmail")
    contact_index = _build_email_index(contacts, "Email")

    # Find leads that match an existing account or contact
    matches = []
    unmatched_by_email = defaultdict(list)  # for lead-only dupes

    for lead in leads:
        email = _lower(lead.get("Email"))
        if not email:
            continue

        matching_accounts = account_index.get(email, [])
        matching_contacts = contact_index.get(email, [])

        if matching_accounts or matching_contacts:
            matches.append({
                "lead_id": lead["Id"],
                "lead_name": lead.get("Name", "—"),
                "email": email,
                "lead_status": lead.get("Status", "—"),
                "lead_source": lead.get("LeadSource", "—"),
                "lead_owner": _owner_name(lead),
                "lead_company": lead.get("Company", "—"),
                "lead_created": lead.get("CreatedDate", ""),
                "lead_age_days": _days_ago(lead.get("CreatedDate")),
                "lead_is_converted": lead.get("IsConverted", False),
                "matching_accounts": matching_accounts,
                "matching_contacts": matching_contacts,
            })
        else:
            unmatched_by_email[email].append(lead)

    print(f"Leads matching an Account or Contact: {len(matches):,}")

    # Find lead-only duplicates (same email, no Account/Contact match)
    lead_only_dupes = [(e, ls) for e, ls in unmatched_by_email.items() if len(ls) >= 2]
    print(f"Lead-only duplicate groups (no Account/Contact match): {len(lead_only_dupes):,}")

    # Build and send report
    print("Building report...")
    html = build_report_html(matches, lead_only_dupes, instance_url)

    today = date.today().strftime("%B %d, %Y")
    subject = f"Lead Cleanup Report — {today} ({len(matches):,} leads with existing Account/Contact)"
    recipients = ["francis.phan@vinesofmendoza.com"]

    print(f"Sending to {', '.join(recipients)}...")
    email_sender.send_report(subject, html, recipients)
    print("Done!")


if __name__ == "__main__":
    main()
