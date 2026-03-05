"""Detect duplicate records across Salesforce objects and send an HTML report."""

from collections import Counter, defaultdict
from datetime import date

from src import sf_client, email_sender

# ── SOQL queries per object ──

ACCOUNT_SOQL = """
    SELECT Id, Name, PersonEmail, Phone, CreatedDate, OwnerId, Owner.Name
    FROM Account
    WHERE PersonEmail != null
"""

CONTACT_SOQL = """
    SELECT Id, Name, Email, Phone, AccountId, CreatedDate
    FROM Contact
    WHERE Email != null
"""

LEAD_SOQL = """
    SELECT Id, Name, Email, Company, Phone, Status, CreatedDate
    FROM Lead
    WHERE Email != null
"""

OPPORTUNITY_SOQL = """
    SELECT Id, Name, Email__c, StageName, Amount, AccountId, Account.Name, CreatedDate
    FROM Opportunity
    WHERE Email__c != null
"""

GUEST_SOQL = """
    SELECT Id, Guest_First_Name__c, Guest_Last_Name__c, Email__c,
           Check_In_Date__c, Account__c, Related_Opportunity__c
    FROM TVRS_Guest__c
    WHERE Email__c != null
"""

# Object definitions: (label, soql, email_field, display_fields)
OBJECTS = [
    ("Account", ACCOUNT_SOQL, "PersonEmail", ["Id", "Name", "PersonEmail", "Phone", "CreatedDate", "Owner.Name"]),
    ("Contact", CONTACT_SOQL, "Email", ["Id", "Name", "Email", "Phone", "AccountId", "CreatedDate"]),
    ("Lead", LEAD_SOQL, "Email", ["Id", "Name", "Email", "Company", "Phone", "Status", "CreatedDate"]),
    ("Opportunity", OPPORTUNITY_SOQL, "Email__c", ["Id", "Name", "Email__c", "StageName", "Amount", "CreatedDate"]),
    ("TVRS_Guest__c", GUEST_SOQL, "Email__c", ["Id", "Guest_First_Name__c", "Guest_Last_Name__c", "Email__c", "Check_In_Date__c"]),
]


def _get_email(record, email_field):
    """Extract and normalise email from a record."""
    return (record.get(email_field) or "").strip().lower()


def _find_duplicates(records, email_field):
    """Group records by email (case-insensitive) and return groups with 2+ records."""
    by_email = defaultdict(list)
    for r in records:
        email = _get_email(r, email_field)
        if email:
            by_email[email].append(r)
    return {e: recs for e, recs in by_email.items() if len(recs) >= 2}


def _find_cross_object_overlaps(email_sets):
    """Find emails that appear in 2+ objects.

    email_sets: dict of {object_label: set_of_emails}
    Returns: dict of {email: [object_labels]}
    """
    all_emails = set()
    for emails in email_sets.values():
        all_emails |= emails

    overlaps = {}
    for email in all_emails:
        present_in = [label for label, emails in email_sets.items() if email in emails]
        if len(present_in) >= 2:
            overlaps[email] = present_in
    return overlaps


def _field_value(record, field):
    """Get a field value, supporting dotted paths like Owner.Name."""
    parts = field.split(".")
    val = record
    for p in parts:
        if isinstance(val, dict):
            val = val.get(p)
        else:
            return None
    return val


def _fmt_val(val):
    """Format a value for HTML display."""
    if val is None:
        return "—"
    if isinstance(val, (int, float)):
        return f"{val:,.0f}"
    return str(val)


def _record_link(record_id, label, instance_url, obj_api_name):
    """Build a Salesforce Lightning link."""
    return f'<a href="{instance_url}/lightning/r/{obj_api_name}/{record_id}/view">{label}</a>'


def build_report_html(object_results, cross_overlaps, instance_url):
    """Build the full HTML email body.

    object_results: list of (label, api_name, email_field, display_fields, duplicates_dict, total_queried)
    cross_overlaps: dict of {email: [object_labels]}
    """
    today = date.today().strftime("%B %d, %Y")

    # Summary stats
    total_dup_groups = sum(len(dups) for _, _, _, _, dups, _ in object_results)
    total_excess = sum(
        sum(len(recs) - 1 for recs in dups.values())
        for _, _, _, _, dups, _ in object_results
    )

    html = f"""\
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  body {{ font-family: 'Segoe UI', Arial, sans-serif; color: #2c3e50; margin: 0; padding: 0; background: #f5f6fa; }}
  .container {{ max-width: 1000px; margin: 0 auto; background: #fff; }}
  .header {{ background: linear-gradient(135deg, #c0392b, #e74c3c); color: white; padding: 30px 35px; }}
  .header h1 {{ margin: 0 0 5px; font-size: 22px; font-weight: 600; }}
  .header p {{ margin: 0; opacity: 0.85; font-size: 13px; }}
  .content {{ padding: 25px 35px 35px; }}
  .summary-cards {{ display: flex; gap: 16px; margin: 0 0 25px; flex-wrap: wrap; }}
  .card {{ flex: 1; min-width: 120px; background: #f8f9fa; border-radius: 8px; padding: 18px 20px; text-align: center; }}
  .card .number {{ font-size: 28px; font-weight: 700; color: #2c3e50; }}
  .card .label {{ font-size: 11px; color: #7f8c8d; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 4px; }}
  .card.danger {{ background: #fdedec; border: 1px solid #e74c3c; }}
  .card.warning {{ background: #fef9e7; border: 1px solid #f39c12; }}
  .card.info {{ background: #eaf4fb; border: 1px solid #3498db; }}
  h2 {{ color: #2c3e50; font-size: 16px; margin: 28px 0 12px; padding-bottom: 6px; border-bottom: 2px solid #e74c3c; }}
  h3 {{ color: #34495e; font-size: 14px; margin: 22px 0 8px; }}
  table {{ border-collapse: collapse; width: 100%; margin: 10px 0 20px; font-size: 12px; }}
  th {{ background: #f8f9fa; color: #2c3e50; font-weight: 600; text-align: left; padding: 8px 10px; border-bottom: 2px solid #dee2e6; white-space: nowrap; }}
  td {{ padding: 6px 10px; border-bottom: 1px solid #eee; vertical-align: top; max-width: 200px; overflow: hidden; text-overflow: ellipsis; }}
  tr:hover td {{ background: #f8f9fa; }}
  .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  a {{ color: #2a6496; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .callout {{ background: #eaf4fb; border-left: 4px solid #3498db; padding: 14px 18px; margin: 15px 0; border-radius: 0 4px 4px 0; font-size: 13px; line-height: 1.5; }}
  .obj-header {{ background: #f8f9fa; border: 1px solid #e8ecf1; border-radius: 6px; padding: 14px 18px; margin: 20px 0 10px; }}
  .obj-header .obj-title {{ font-size: 15px; font-weight: 700; color: #2c3e50; margin: 0 0 4px; }}
  .obj-header .obj-stats {{ font-size: 12px; color: #7f8c8d; }}
  .group-sep td {{ background: #f0f0f0; padding: 4px 10px; font-weight: 600; font-size: 11px; color: #555; }}
  .footer {{ padding: 20px 35px; background: #f8f9fa; font-size: 11px; color: #95a5a6; border-top: 1px solid #eee; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 3px; font-size: 11px; font-weight: 600; }}
  .badge-lead {{ background: #d5f5e3; color: #27ae60; }}
  .badge-contact {{ background: #d6eaf8; color: #2980b9; }}
  .badge-account {{ background: #fdebd0; color: #e67e22; }}
  .badge-opp {{ background: #f5b7b1; color: #c0392b; }}
  .badge-guest {{ background: #e8daef; color: #8e44ad; }}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h1>Duplicate Records Report</h1>
    <p>Cross-object duplicate detection by email &bull; Generated {today}</p>
  </div>
  <div class="content">

    <div class="summary-cards">
      <div class="card danger">
        <div class="number">{total_dup_groups:,}</div>
        <div class="label">Duplicate Groups</div>
      </div>
      <div class="card warning">
        <div class="number">{total_excess:,}</div>
        <div class="label">Excess Records</div>
      </div>
      <div class="card info">
        <div class="number">{len(cross_overlaps):,}</div>
        <div class="label">Cross-Object Overlaps</div>
      </div>
"""

    # Per-object summary cards
    for label, _, _, _, dups, total_queried in object_results:
        n_groups = len(dups)
        html += f"""\
      <div class="card">
        <div class="number">{n_groups:,}</div>
        <div class="label">{label} Dupes</div>
      </div>
"""

    html += "    </div>\n\n"

    html += """\
    <div class="callout">
      <strong>About this report:</strong> Identifies records sharing the same email address (case-insensitive)
      within each Salesforce object, plus cross-object overlaps where the same email exists in multiple object types.
      "Excess records" counts records beyond the first in each duplicate group (i.e., how many could potentially be merged).
    </div>
"""

    # ── Per-object duplicate sections ──
    for label, api_name, email_field, display_fields, dups, total_queried in object_results:
        n_groups = len(dups)
        excess = sum(len(recs) - 1 for recs in dups.values())
        # Sort by group size descending
        sorted_groups = sorted(dups.items(), key=lambda x: -len(x[1]))

        html += f"""\
    <div class="obj-header">
      <p class="obj-title">{label}</p>
      <p class="obj-stats">{total_queried:,} records with email &bull; {n_groups:,} duplicate groups &bull; {excess:,} excess records</p>
    </div>
"""
        if not sorted_groups:
            html += '    <p style="color:#7f8c8d; font-size:13px;">No duplicates found.</p>\n'
            continue

        # Size distribution
        size_dist = Counter(len(recs) for recs in dups.values())
        html += "    <table>\n"
        html += '      <tr><th>Group Size</th><th class="num">Groups</th></tr>\n'
        for size in sorted(size_dist, reverse=True):
            html += f'      <tr><td>{size} records</td><td class="num">{size_dist[size]:,}</td></tr>\n'
        html += "    </table>\n"

        # One row per duplicate group: email, count, names, date range
        name_field = "Name"
        if label == "TVRS_Guest__c":
            name_field = None  # we'll build name from first+last

        html += "    <table>\n"
        html += '      <tr><th>Email</th><th class="num">Count</th><th>Names</th><th>Created (oldest)</th><th>Created (newest)</th></tr>\n'

        shown = sorted_groups[:200]
        for email, recs in shown:
            count = len(recs)

            # Collect names
            if label == "TVRS_Guest__c":
                names = []
                for r in recs:
                    n = f"{r.get('Guest_First_Name__c') or ''} {r.get('Guest_Last_Name__c') or ''}".strip()
                    rec_id = r.get("Id", "")
                    if rec_id:
                        names.append(_record_link(rec_id, n or "—", instance_url, api_name))
                    else:
                        names.append(n or "—")
            else:
                names = []
                for r in recs:
                    n = r.get("Name") or "—"
                    rec_id = r.get("Id", "")
                    if rec_id:
                        names.append(_record_link(rec_id, n, instance_url, api_name))
                    else:
                        names.append(n)

            names_str = ", ".join(names)

            # Date range from CreatedDate or Check_In_Date__c
            date_field = "Check_In_Date__c" if label == "TVRS_Guest__c" else "CreatedDate"
            dates = sorted(
                [r.get(date_field, "") for r in recs if r.get(date_field)],
            )
            oldest = (dates[0][:10] if dates else "—")
            newest = (dates[-1][:10] if dates else "—")

            html += (
                f"      <tr>"
                f"<td>{email}</td>"
                f'<td class="num">{count}</td>'
                f"<td>{names_str}</td>"
                f"<td>{oldest}</td>"
                f"<td>{newest}</td>"
                f"</tr>\n"
            )

        if len(sorted_groups) > 200:
            html += f'      <tr><td colspan="5" style="color:#7f8c8d; font-style:italic;">... and {len(sorted_groups) - 200} more duplicate groups not shown</td></tr>\n'

        html += "    </table>\n"

    # ── Cross-object overlaps ──
    html += '    <h2>Cross-Object Overlaps</h2>\n'
    html += '    <div class="callout"><strong>Emails that exist across 2+ object types.</strong> '
    html += "These may indicate unconverted leads, redundant contacts, or data that should be merged.</div>\n"

    badge_cls = {
        "Account": "badge-account",
        "Contact": "badge-contact",
        "Lead": "badge-lead",
        "Opportunity": "badge-opp",
        "TVRS_Guest__c": "badge-guest",
    }

    # Sort by number of objects (most overlaps first), then alphabetically
    sorted_overlaps = sorted(cross_overlaps.items(), key=lambda x: (-len(x[1]), x[0]))

    # Overlap summary
    overlap_by_count = Counter(len(objs) for objs in cross_overlaps.values())
    html += "    <table>\n"
    html += '      <tr><th>Objects Overlapping</th><th class="num">Emails</th></tr>\n'
    for n_objs in sorted(overlap_by_count, reverse=True):
        html += f'      <tr><td>{n_objs} objects</td><td class="num">{overlap_by_count[n_objs]:,}</td></tr>\n'
    html += "    </table>\n"

    # Overlap by object pair
    pair_counts = Counter()
    for email, objs in cross_overlaps.items():
        for i, a in enumerate(objs):
            for b in objs[i + 1:]:
                pair_counts[(a, b)] += 1
    if pair_counts:
        html += "    <h3>Overlap by Object Pair</h3>\n"
        html += "    <table>\n"
        html += '      <tr><th>Object A</th><th>Object B</th><th class="num">Shared Emails</th></tr>\n'
        for (a, b), cnt in pair_counts.most_common():
            html += f"      <tr><td>{a}</td><td>{b}</td>"
            html += f'<td class="num">{cnt:,}</td></tr>\n'
        html += "    </table>\n"

    # Detail table (show up to 200 overlaps)
    html += "    <h3>Detail (top 200)</h3>\n"
    html += "    <table>\n"
    html += '      <tr><th>Email</th><th>Found In</th><th class="num"># Objects</th></tr>\n'
    for email, objs in sorted_overlaps[:200]:
        badges = " ".join(
            f'<span class="badge {badge_cls.get(o, "")}">{o}</span>' for o in objs
        )
        html += f'      <tr><td>{email}</td><td>{badges}</td><td class="num">{len(objs)}</td></tr>\n'
    if len(sorted_overlaps) > 200:
        html += f'      <tr><td colspan="3" style="color:#7f8c8d; font-style:italic;">... and {len(sorted_overlaps) - 200} more not shown</td></tr>\n'
    html += "    </table>\n"

    # ── Stay frequency from TVRS_Guest__c ──
    guest_result = next((r for r in object_results if r[0] == "TVRS_Guest__c"), None)
    if guest_result:
        _, _, email_field, _, _, total_queried = guest_result
        # Recompute all-guest email groups (including non-duplicates) for stay frequency
        # We'll use the total_queried and duplicate info
        html += "    <h2>TVRS Guest Stay Frequency</h2>\n"
        html += '    <p style="font-size:12px;color:#7f8c8d;">Distribution of how many guest records share the same email (proxy for repeat stays)</p>\n'

    html += """  </div>
  <div class="footer">
    This is an automated report. Data sourced from Salesforce Account, Contact, Lead, Opportunity, and TVRS Guest records.
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

    # Query each object and find duplicates
    object_results = []  # (label, api_name, email_field, display_fields, dups, total_queried)
    email_sets = {}  # {label: set of all lowercased emails}

    for label, soql, email_field, display_fields in OBJECTS:
        print(f"Querying {label}...")
        records = sf_client.query(sf_holder, soql)
        total = len(records)
        print(f"  {label}: {total:,} records with email")

        # Build the full email set for cross-object comparison
        all_emails = set()
        for r in records:
            email = _get_email(r, email_field)
            if email:
                all_emails.add(email)
        email_sets[label] = all_emails

        # Find within-object duplicates
        dups = _find_duplicates(records, email_field)
        excess = sum(len(recs) - 1 for recs in dups.values())
        print(f"  {label}: {len(dups):,} duplicate groups, {excess:,} excess records")

        object_results.append((label, label, email_field, display_fields, dups, total))

    # Cross-object overlaps
    print("Computing cross-object overlaps...")
    cross_overlaps = _find_cross_object_overlaps(email_sets)
    print(f"Found {len(cross_overlaps):,} emails in 2+ objects")

    # Build and send report
    print("Building report...")
    html = build_report_html(object_results, cross_overlaps, instance_url)

    total_groups = sum(len(dups) for _, _, _, _, dups, _ in object_results)
    today = date.today().strftime("%B %d, %Y")
    subject = f"Duplicate Records Report — {today} ({total_groups:,} duplicate groups)"
    recipients = ["francis.phan@vinesofmendoza.com"]

    print(f"Sending to {', '.join(recipients)}...")
    email_sender.send_report(subject, html, recipients)
    print("Done!")


if __name__ == "__main__":
    main()
