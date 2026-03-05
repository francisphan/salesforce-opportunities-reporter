"""Contact cleanup report: flag duplicate Contacts by email, classify same-account vs cross-account."""

from collections import Counter, defaultdict
from datetime import date, datetime

from src import sf_client, email_sender

CONTACT_SOQL = """
    SELECT Id, Name, Email, Phone, AccountId, Account.Name,
           CreatedDate, LastModifiedDate, OwnerId, Owner.Name
    FROM Contact
    WHERE Email != null
    ORDER BY CreatedDate ASC
"""

# Indirect AccountContactRelations — contacts intentionally linked to multiple accounts
ACR_SOQL = """
    SELECT ContactId, AccountId, IsDirect, IsActive, Roles
    FROM AccountContactRelation
    WHERE IsDirect = false
"""

BATCH_SIZE = 200

OPP_COUNT_TEMPLATE = """
    SELECT AccountId, COUNT(Id) cnt
    FROM Opportunity
    WHERE AccountId IN ({ids})
    GROUP BY AccountId
"""


def _lower(val):
    return (val or "").strip().lower()


def _days_ago(date_str):
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


def _account_name_from_record(record):
    acc = record.get("Account") or {}
    return acc.get("Name", "—")


def _ids_csv(ids):
    return ",".join(f"'{i}'" for i in ids)


def _find_contact_dupes(contacts):
    """Group contacts by email and return groups with 2+ contacts."""
    by_email = defaultdict(list)
    for c in contacts:
        email = _lower(c.get("Email"))
        if email:
            by_email[email].append(c)
    return {e: recs for e, recs in by_email.items() if len(recs) >= 2}


def _classify_group(contacts):
    """Classify a duplicate group as same-account, cross-account, or no-account."""
    account_ids = set()
    has_null = False
    for c in contacts:
        aid = c.get("AccountId")
        if aid:
            account_ids.add(aid)
        else:
            has_null = True

    if len(account_ids) <= 1 and not has_null:
        return "same_account"
    if len(account_ids) <= 1 and has_null:
        return "partial_account"  # some have account, some don't
    return "cross_account"


def _query_opp_counts(sf_holder, account_ids):
    """Query opportunity counts per account in batches. Returns {account_id: count}."""
    opp_counts = {}
    account_list = list(account_ids)
    for i in range(0, len(account_list), BATCH_SIZE):
        batch = account_list[i:i + BATCH_SIZE]
        soql = OPP_COUNT_TEMPLATE.format(ids=_ids_csv(batch))
        results = sf_client.query(sf_holder, soql)
        for r in results:
            opp_counts[r["AccountId"]] = r["cnt"]
    return opp_counts


def build_report_html(same_account, cross_account, partial_account,
                      acr_index, opp_counts, instance_url):
    """Build the HTML email.

    same_account: list of (email, [contacts]) — all contacts share the same Account
    cross_account: list of (email, [contacts]) — contacts on different Accounts
    partial_account: list of (email, [contacts]) — mix of with/without Account
    acr_index: {contact_id: [indirect ACR records]}
    opp_counts: {account_id: opportunity_count}
    """
    today = date.today().strftime("%B %d, %Y")
    total_groups = len(same_account) + len(cross_account) + len(partial_account)
    total_excess = (
        sum(len(cs) - 1 for _, cs in same_account)
        + sum(len(cs) - 1 for _, cs in cross_account)
        + sum(len(cs) - 1 for _, cs in partial_account)
    )

    # Count cross-account groups that have indirect ACRs (intentional multi-account)
    cross_with_acr = 0
    for email, contacts in cross_account:
        if any(c["Id"] in acr_index for c in contacts):
            cross_with_acr += 1

    html = f"""\
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  body {{ font-family: 'Segoe UI', Arial, sans-serif; color: #2c3e50; margin: 0; padding: 0; background: #f5f6fa; }}
  .container {{ max-width: 1020px; margin: 0 auto; background: #fff; }}
  .header {{ background: linear-gradient(135deg, #2980b9, #3498db); color: white; padding: 30px 35px; }}
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
  h2 {{ color: #2c3e50; font-size: 16px; margin: 28px 0 12px; padding-bottom: 6px; border-bottom: 2px solid #3498db; }}
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
  .callout.ok {{ border-left-color: #27ae60; background: #d5f5e3; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 3px; font-size: 11px; font-weight: 600; margin: 1px 2px; }}
  .badge-same {{ background: #d5f5e3; color: #27ae60; }}
  .badge-cross {{ background: #fdebd0; color: #e67e22; }}
  .badge-acr {{ background: #d6eaf8; color: #2980b9; }}
  .badge-old {{ background: #fadbd8; color: #c0392b; }}
  .badge-opps {{ background: #e8daef; color: #8e44ad; }}
  .footer {{ padding: 20px 35px; background: #f8f9fa; font-size: 11px; color: #95a5a6; border-top: 1px solid #eee; }}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h1>Contact Cleanup Report</h1>
    <p>Duplicate Contacts by email &bull; Same-account vs cross-account classification &bull; Generated {today}</p>
  </div>
  <div class="content">

    <div class="summary-cards">
      <div class="card danger">
        <div class="number">{total_groups:,}</div>
        <div class="label">Duplicate Groups</div>
      </div>
      <div class="card success">
        <div class="number">{len(same_account):,}</div>
        <div class="label">Same-Account (easy merge)</div>
      </div>
      <div class="card warning">
        <div class="number">{len(cross_account):,}</div>
        <div class="label">Cross-Account</div>
      </div>
      <div class="card info">
        <div class="number">{cross_with_acr:,}</div>
        <div class="label">Cross w/ Indirect ACR</div>
      </div>
      <div class="card">
        <div class="number">{total_excess:,}</div>
        <div class="label">Excess Records</div>
      </div>
    </div>

    <div class="callout">
      <strong>About this report:</strong> Contacts sharing the same email are grouped and classified:<br>
      <strong>Same-Account</strong> = all contacts in the group belong to the same Account &mdash; straightforward merge candidates.<br>
      <strong>Cross-Account</strong> = contacts on different Accounts &mdash; may be intentional (person works with multiple companies) or may indicate duplicate Accounts too.<br>
      <strong>Indirect ACR</strong> = the contact has an AccountContactRelation to another Account (Salesforce already knows about the multi-account link).<br>
      <strong>Partial-Account</strong> = some contacts have an Account, some don't &mdash; the orphans should be merged into the one with an Account.
    </div>
"""

    # ── Size distribution ──
    all_groups = same_account + cross_account + partial_account
    size_dist = Counter(len(cs) for _, cs in all_groups)
    html += "    <h2>Group Size Distribution</h2>\n"
    html += "    <table>\n"
    html += '      <tr><th>Contacts per Email</th><th class="num">Groups</th><th class="num">Same-Acct</th><th class="num">Cross-Acct</th><th class="num">Partial</th></tr>\n'
    for size in sorted(size_dist, reverse=True):
        sa = sum(1 for _, cs in same_account if len(cs) == size)
        ca = sum(1 for _, cs in cross_account if len(cs) == size)
        pa = sum(1 for _, cs in partial_account if len(cs) == size)
        html += f'      <tr><td>{size} contacts</td><td class="num">{size_dist[size]:,}</td><td class="num">{sa:,}</td><td class="num">{ca:,}</td><td class="num">{pa:,}</td></tr>\n'
    html += "    </table>\n"

    # ── Owner distribution for same-account dupes ──
    owner_dist = Counter()
    for _, contacts in same_account:
        for c in contacts:
            owner_dist[_owner_name(c)] += 1
    html += "    <h2>Same-Account Duplicates by Contact Owner</h2>\n"
    html += "    <table>\n"
    html += '      <tr><th>Owner</th><th class="num">Duplicate Contacts</th></tr>\n'
    for owner, count in owner_dist.most_common(20):
        html += f'      <tr><td>{owner}</td><td class="num">{count:,}</td></tr>\n'
    html += "    </table>\n"

    # ═══════════════════════════════════════════════════
    # SAME-ACCOUNT DUPES — easy merges
    # ═══════════════════════════════════════════════════
    sa_excess = sum(len(cs) - 1 for _, cs in same_account)
    html += f'    <h2>Same-Account Duplicates ({len(same_account):,} groups, {sa_excess:,} excess)</h2>\n'
    html += '    <div class="callout ok"><strong>Low risk:</strong> These contacts share an email and belong to the same Account. They can be safely merged in most cases — keep the oldest or most active, merge the rest.</div>\n'
    html += "    <table>\n"
    html += (
        "      <tr>"
        "<th>Email</th>"
        '<th class="num">Count</th>'
        "<th>Account</th>"
        '<th class="num">Opps</th>'
        "<th>Contacts</th>"
        "<th>Owners</th>"
        "<th>Created Range</th>"
        "</tr>\n"
    )

    sa_sorted = sorted(same_account, key=lambda x: -len(x[1]))
    for email, contacts in sa_sorted[:300]:
        count = len(contacts)
        acct_name = _account_name_from_record(contacts[0])
        acct_id = contacts[0].get("AccountId", "")
        acct_link = f'<a href="{instance_url}/lightning/r/Account/{acct_id}/view">{acct_name}</a>' if acct_id else acct_name
        n_opps = opp_counts.get(acct_id, 0)

        ct_links = []
        for c in contacts:
            link = f'<a href="{instance_url}/lightning/r/Contact/{c["Id"]}/view">{c.get("Name", "—")}</a>'
            ct_links.append(link)

        owners = ", ".join(sorted(set(_owner_name(c) for c in contacts)))
        dates = sorted(c.get("CreatedDate", "")[:10] for c in contacts if c.get("CreatedDate"))
        date_range = f"{dates[0]} — {dates[-1]}" if len(dates) >= 2 else (dates[0] if dates else "—")

        opps_badge = f' <span class="badge badge-opps">{n_opps} opps</span>' if n_opps else ""

        html += (
            f"      <tr>"
            f"<td>{email}</td>"
            f'<td class="num">{count}</td>'
            f"<td>{acct_link}{opps_badge}</td>"
            f'<td class="num">{n_opps}</td>'
            f"<td>{', '.join(ct_links)}</td>"
            f"<td>{owners}</td>"
            f"<td>{date_range}</td>"
            f"</tr>\n"
        )

    if len(sa_sorted) > 300:
        html += f'      <tr><td colspan="7" style="color:#7f8c8d; font-style:italic;">... and {len(sa_sorted) - 300} more groups not shown</td></tr>\n'
    html += "    </table>\n"

    # ═══════════════════════════════════════════════════
    # CROSS-ACCOUNT DUPES — needs investigation
    # ═══════════════════════════════════════════════════
    ca_excess = sum(len(cs) - 1 for _, cs in cross_account)
    html += f'    <h2>Cross-Account Duplicates ({len(cross_account):,} groups, {ca_excess:,} excess)</h2>\n'
    html += '    <div class="callout action"><strong>Needs review:</strong> These contacts share an email but belong to different Accounts. Some may be intentional (consultant linked to multiple companies via AccountContactRelation). Others may indicate duplicate Accounts that should also be merged.</div>\n'
    html += "    <table>\n"
    html += (
        "      <tr>"
        "<th>Email</th>"
        '<th class="num">Count</th>'
        "<th>Accounts</th>"
        "<th>Contacts</th>"
        "<th>Has Indirect ACR</th>"
        "<th>Created Range</th>"
        "</tr>\n"
    )

    ca_sorted = sorted(cross_account, key=lambda x: -len(x[1]))
    for email, contacts in ca_sorted[:300]:
        count = len(contacts)

        # Account info
        acct_parts = []
        for c in contacts:
            aid = c.get("AccountId", "")
            aname = _account_name_from_record(c)
            n_opps = opp_counts.get(aid, 0)
            link = f'<a href="{instance_url}/lightning/r/Account/{aid}/view">{aname}</a>' if aid else aname
            opps_note = f" ({n_opps} opps)" if n_opps else ""
            acct_parts.append(f"{link}{opps_note}")
        # Deduplicate account display
        seen_accts = set()
        unique_acct_parts = []
        for c in contacts:
            aid = c.get("AccountId", "")
            if aid and aid not in seen_accts:
                seen_accts.add(aid)
                aname = _account_name_from_record(c)
                n_opps = opp_counts.get(aid, 0)
                link = f'<a href="{instance_url}/lightning/r/Account/{aid}/view">{aname}</a>'
                opps_note = f" ({n_opps} opps)" if n_opps else ""
                unique_acct_parts.append(f"{link}{opps_note}")

        ct_links = []
        for c in contacts:
            link = f'<a href="{instance_url}/lightning/r/Contact/{c["Id"]}/view">{c.get("Name", "—")}</a>'
            ct_links.append(link)

        has_acr = any(c["Id"] in acr_index for c in contacts)
        acr_badge = '<span class="badge badge-acr">Yes</span>' if has_acr else "No"

        dates = sorted(c.get("CreatedDate", "")[:10] for c in contacts if c.get("CreatedDate"))
        date_range = f"{dates[0]} — {dates[-1]}" if len(dates) >= 2 else (dates[0] if dates else "—")

        html += (
            f"      <tr>"
            f"<td>{email}</td>"
            f'<td class="num">{count}</td>'
            f"<td>{'<br>'.join(unique_acct_parts)}</td>"
            f"<td>{', '.join(ct_links)}</td>"
            f"<td>{acr_badge}</td>"
            f"<td>{date_range}</td>"
            f"</tr>\n"
        )

    if len(ca_sorted) > 300:
        html += f'      <tr><td colspan="6" style="color:#7f8c8d; font-style:italic;">... and {len(ca_sorted) - 300} more groups not shown</td></tr>\n'
    html += "    </table>\n"

    # ═══════════════════════════════════════════════════
    # PARTIAL-ACCOUNT DUPES
    # ═══════════════════════════════════════════════════
    if partial_account:
        pa_excess = sum(len(cs) - 1 for _, cs in partial_account)
        html += f'    <h2>Partial-Account Duplicates ({len(partial_account):,} groups, {pa_excess:,} excess)</h2>\n'
        html += '    <div class="callout">Some contacts in each group have an Account, others are orphaned. The orphan contacts should likely be merged into the one with an Account.</div>\n'
        html += "    <table>\n"
        html += (
            "      <tr>"
            "<th>Email</th>"
            '<th class="num">Count</th>'
            "<th>Contacts (with Account)</th>"
            "<th>Contacts (no Account)</th>"
            "<th>Created Range</th>"
            "</tr>\n"
        )

        pa_sorted = sorted(partial_account, key=lambda x: -len(x[1]))
        for email, contacts in pa_sorted[:200]:
            count = len(contacts)
            with_acct = []
            without_acct = []
            for c in contacts:
                link = f'<a href="{instance_url}/lightning/r/Contact/{c["Id"]}/view">{c.get("Name", "—")}</a>'
                if c.get("AccountId"):
                    aname = _account_name_from_record(c)
                    with_acct.append(f"{link} ({aname})")
                else:
                    without_acct.append(link)

            dates = sorted(c.get("CreatedDate", "")[:10] for c in contacts if c.get("CreatedDate"))
            date_range = f"{dates[0]} — {dates[-1]}" if len(dates) >= 2 else (dates[0] if dates else "—")

            html += (
                f"      <tr>"
                f"<td>{email}</td>"
                f'<td class="num">{count}</td>'
                f"<td>{', '.join(with_acct) or '—'}</td>"
                f"<td>{', '.join(without_acct) or '—'}</td>"
                f"<td>{date_range}</td>"
                f"</tr>\n"
            )

        if len(pa_sorted) > 200:
            html += f'      <tr><td colspan="5" style="color:#7f8c8d; font-style:italic;">... and {len(pa_sorted) - 200} more groups not shown</td></tr>\n'
        html += "    </table>\n"

    html += """  </div>
  <div class="footer">
    This is an automated read-only report. No records were modified. Data sourced from Salesforce Contact, Account, and AccountContactRelation objects.
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

    print("Querying Contacts...")
    contacts = sf_client.query(sf_holder, CONTACT_SOQL)
    print(f"  {len(contacts):,} contacts with email")

    print("Querying indirect AccountContactRelations...")
    acrs = sf_client.query(sf_holder, ACR_SOQL)
    print(f"  {len(acrs):,} indirect ACRs")

    # Build ACR index: {contact_id: [acr records]}
    acr_index = defaultdict(list)
    for acr in acrs:
        acr_index[acr["ContactId"]].append(acr)

    # Find duplicate groups
    print("Finding duplicate groups...")
    dupes = _find_contact_dupes(contacts)
    print(f"  {len(dupes):,} duplicate groups")

    # Classify
    same_account = []
    cross_account = []
    partial_account = []

    for email, group in dupes.items():
        cls = _classify_group(group)
        if cls == "same_account":
            same_account.append((email, group))
        elif cls == "cross_account":
            cross_account.append((email, group))
        else:
            partial_account.append((email, group))

    print(f"  Same-account: {len(same_account):,}")
    print(f"  Cross-account: {len(cross_account):,}")
    print(f"  Partial-account: {len(partial_account):,}")

    # Collect all account IDs from duplicate groups for opp count query
    all_account_ids = set()
    for _, group in same_account + cross_account + partial_account:
        for c in group:
            if c.get("AccountId"):
                all_account_ids.add(c["AccountId"])

    print(f"Querying opportunity counts for {len(all_account_ids):,} accounts...")
    opp_counts = _query_opp_counts(sf_holder, all_account_ids)
    print(f"  {len(opp_counts):,} accounts have opportunities")

    # Build and send report
    print("Building report...")
    html = build_report_html(
        same_account, cross_account, partial_account,
        acr_index, opp_counts, instance_url,
    )

    today = date.today().strftime("%B %d, %Y")
    total = len(same_account) + len(cross_account) + len(partial_account)
    subject = f"Contact Cleanup Report — {today} ({total:,} duplicate groups)"
    recipients = ["francis.phan@vinesofmendoza.com"]

    print(f"Sending to {', '.join(recipients)}...")
    email_sender.send_report(subject, html, recipients)
    print("Done!")


if __name__ == "__main__":
    main()
