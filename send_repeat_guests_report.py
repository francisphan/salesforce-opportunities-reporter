"""Find guests with multiple stays and report on their opportunity status."""

from collections import defaultdict
from datetime import date

from src import sf_client, email_sender

BATCH_SIZE = 200

GUESTS_SOQL = """
    SELECT Id, Guest_First_Name__c, Guest_Last_Name__c, Email__c,
           Check_In_Date__c, Check_Out_Date__c,
           Account__c, Account__r.Name, Related_Opportunity__c,
           Villa_number__c, Assigned_Sales_Rep__c, Language__c
    FROM TVRS_Guest__c
    WHERE Email__c != null
    ORDER BY Check_In_Date__c DESC
"""

OPPS_BY_ACCOUNT_TEMPLATE = """
    SELECT Id, Name, StageName, Amount, AccountId, IsClosed, IsWon, CreatedDate
    FROM Opportunity
    WHERE AccountId IN ({ids})
    ORDER BY CreatedDate DESC
"""


def _ids_csv(ids):
    return ",".join(f"'{id_}'" for id_ in ids)


def _query_opps_for_accounts(sf_holder, account_ids):
    """Query all opportunities linked to the given account IDs."""
    opps_by_account = defaultdict(list)
    account_list = list(account_ids)
    for i in range(0, len(account_list), BATCH_SIZE):
        batch = account_list[i:i + BATCH_SIZE]
        soql = OPPS_BY_ACCOUNT_TEMPLATE.format(ids=_ids_csv(batch))
        opps = sf_client.query(sf_holder, soql)
        for o in opps:
            opps_by_account[o["AccountId"]].append(o)
    return opps_by_account


def _group_repeat_guests(records):
    """Group guest records by email and return those with 2+ stays."""
    by_email = defaultdict(list)
    for r in records:
        email = (r.get("Email__c") or "").strip().lower()
        if email:
            by_email[email].append(r)
    return {e: stays for e, stays in by_email.items() if len(stays) >= 2}


def _build_guest_summary(email, stays, opps_by_account):
    """Build a summary dict for a single repeat guest."""
    first = stays[0]
    name = f"{first.get('Guest_First_Name__c') or ''} {first.get('Guest_Last_Name__c') or ''}".strip() or "Unknown"
    account_name = None
    acc = first.get("Account__r")
    if isinstance(acc, dict):
        account_name = acc.get("Name")

    # Collect all account IDs for this guest
    account_ids = {s["Account__c"] for s in stays if s.get("Account__c")}

    # Find opportunities via Related_Opportunity__c or Account
    direct_opp_ids = {s["Related_Opportunity__c"] for s in stays if s.get("Related_Opportunity__c")}
    account_opps = []
    for aid in account_ids:
        account_opps.extend(opps_by_account.get(aid, []))

    # Deduplicate (direct opps are a subset of account opps usually)
    all_opp_ids = direct_opp_ids | {o["Id"] for o in account_opps}
    has_opportunity = bool(all_opp_ids)

    # Stay dates (sorted most recent first)
    stay_dates = sorted(
        [s.get("Check_In_Date__c") for s in stays if s.get("Check_In_Date__c")],
        reverse=True,
    )
    first_stay = stay_dates[-1] if stay_dates else None
    last_stay = stay_dates[0] if stay_dates else None

    # Opportunity details
    opp_details = []
    for o in account_opps:
        opp_details.append({
            "id": o["Id"],
            "name": o.get("Name", "—"),
            "stage": o.get("StageName", "—"),
            "amount": o.get("Amount"),
            "is_won": o.get("IsWon", False),
            "is_closed": o.get("IsClosed", False),
        })

    return {
        "name": name,
        "email": email,
        "account_name": account_name,
        "stay_count": len(stays),
        "first_stay": first_stay,
        "last_stay": last_stay,
        "has_opportunity": has_opportunity,
        "opportunities": opp_details,
        "language": first.get("Language__c"),
        "assigned_rep": first.get("Assigned_Sales_Rep__c"),
    }


def _fmt_amount(amount):
    if amount is None:
        return "N/A"
    return f"${amount:,.0f}"


def _opp_stage_color(stage, is_won, is_closed):
    if is_won:
        return "#27ae60"
    if is_closed:
        return "#c0392b"
    return "#2c3e50"


def build_report_html(guests_with_opps, guests_without_opps, instance_url):
    """Build HTML email for the repeat guests report."""
    total = len(guests_with_opps) + len(guests_without_opps)
    today = date.today().strftime("%B %d, %Y")

    html = f"""\
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  body {{ font-family: 'Segoe UI', Arial, sans-serif; color: #2c3e50; margin: 0; padding: 0; background: #f5f6fa; }}
  .container {{ max-width: 960px; margin: 0 auto; background: #fff; }}
  .header {{ background: linear-gradient(135deg, #2c3e50, #8e44ad); color: white; padding: 30px 35px; }}
  .header h1 {{ margin: 0 0 5px; font-size: 22px; font-weight: 600; }}
  .header p {{ margin: 0; opacity: 0.85; font-size: 13px; }}
  .content {{ padding: 25px 35px 35px; }}
  .summary-cards {{ display: flex; gap: 16px; margin: 0 0 25px; }}
  .card {{ flex: 1; background: #f8f9fa; border-radius: 8px; padding: 18px 20px; text-align: center; }}
  .card .number {{ font-size: 28px; font-weight: 700; color: #2c3e50; }}
  .card .label {{ font-size: 12px; color: #7f8c8d; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 4px; }}
  .card.highlight {{ background: #eaf4fb; border: 1px solid #3498db; }}
  .card.warning {{ background: #fef9e7; border: 1px solid #f39c12; }}
  h2 {{ color: #2c3e50; font-size: 16px; margin: 28px 0 12px; padding-bottom: 6px; border-bottom: 2px solid #8e44ad; }}
  table {{ border-collapse: collapse; width: 100%; margin: 10px 0 20px; font-size: 13px; }}
  th {{ background: #f8f9fa; color: #2c3e50; font-weight: 600; text-align: left; padding: 8px 10px; border-bottom: 2px solid #dee2e6; }}
  td {{ padding: 7px 10px; border-bottom: 1px solid #eee; vertical-align: top; }}
  tr:hover td {{ background: #f8f9fa; }}
  .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  .opp-tag {{ display: inline-block; padding: 2px 8px; border-radius: 3px; font-size: 11px; font-weight: 600; }}
  .opp-won {{ background: #d5f5e3; color: #27ae60; }}
  .opp-open {{ background: #d6eaf8; color: #2980b9; }}
  .opp-lost {{ background: #fadbd8; color: #c0392b; }}
  .no-opp {{ color: #e67e22; font-weight: 600; }}
  a {{ color: #2a6496; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .callout {{ background: #eaf4fb; border-left: 4px solid #8e44ad; padding: 14px 18px; margin: 15px 0; border-radius: 0 4px 4px 0; font-size: 13px; line-height: 1.5; }}
  .footer {{ padding: 20px 35px; background: #f8f9fa; font-size: 11px; color: #95a5a6; border-top: 1px solid #eee; }}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h1>Repeat Guest Report</h1>
    <p>Guests with multiple stays &bull; Generated {today}</p>
  </div>
  <div class="content">

    <div class="summary-cards">
      <div class="card">
        <div class="number">{total:,}</div>
        <div class="label">Repeat Guests</div>
      </div>
      <div class="card highlight">
        <div class="number">{len(guests_with_opps):,}</div>
        <div class="label">With Opportunity</div>
      </div>
      <div class="card warning">
        <div class="number">{len(guests_without_opps):,}</div>
        <div class="label">No Opportunity</div>
      </div>
    </div>

    <div class="callout">
      <strong>About this report:</strong> Lists all guests who have stayed multiple times based on TVRS Guest records,
      grouped by email. "Has Opportunity" means the guest's Salesforce Account has at least one associated Opportunity
      (or the guest record has a directly linked Opportunity).
    </div>
"""

    # ── Section 1: Guests WITH opportunities ──
    html += f'    <h2>Repeat Guests with Opportunities ({len(guests_with_opps)})</h2>\n'
    html += "    <table>\n"
    html += (
        "      <tr>"
        "<th>Guest</th>"
        "<th>Email</th>"
        "<th>Account</th>"
        '<th class="num">Stays</th>'
        "<th>First Stay</th>"
        "<th>Last Stay</th>"
        "<th>Opportunity</th>"
        '<th class="num">Amount</th>'
        "</tr>\n"
    )

    for g in guests_with_opps:
        # Show each opportunity as a separate sub-row if multiple, else inline
        opps = g["opportunities"]
        if opps:
            first_opp = opps[0]
            opp_link = f'<a href="{instance_url}/lightning/r/Opportunity/{first_opp["id"]}/view">{first_opp["name"]}</a>'
            if first_opp["is_won"]:
                stage_tag = f'<span class="opp-tag opp-won">{first_opp["stage"]}</span>'
            elif first_opp["is_closed"]:
                stage_tag = f'<span class="opp-tag opp-lost">{first_opp["stage"]}</span>'
            else:
                stage_tag = f'<span class="opp-tag opp-open">{first_opp["stage"]}</span>'
            opp_cell = f"{opp_link}<br>{stage_tag}"
            amount_cell = _fmt_amount(first_opp["amount"])
        else:
            opp_cell = "—"
            amount_cell = "—"

        html += (
            f'      <tr>'
            f'<td>{g["name"]}</td>'
            f'<td>{g["email"]}</td>'
            f'<td>{g["account_name"] or "—"}</td>'
            f'<td class="num">{g["stay_count"]}</td>'
            f'<td>{g["first_stay"] or "—"}</td>'
            f'<td>{g["last_stay"] or "—"}</td>'
            f'<td>{opp_cell}</td>'
            f'<td class="num">{amount_cell}</td>'
            f'</tr>\n'
        )

        # Additional opportunities (if more than 1)
        for opp in opps[1:]:
            opp_link = f'<a href="{instance_url}/lightning/r/Opportunity/{opp["id"]}/view">{opp["name"]}</a>'
            if opp["is_won"]:
                stage_tag = f'<span class="opp-tag opp-won">{opp["stage"]}</span>'
            elif opp["is_closed"]:
                stage_tag = f'<span class="opp-tag opp-lost">{opp["stage"]}</span>'
            else:
                stage_tag = f'<span class="opp-tag opp-open">{opp["stage"]}</span>'
            html += (
                f'      <tr>'
                f'<td colspan="6"></td>'
                f'<td>{opp_link}<br>{stage_tag}</td>'
                f'<td class="num">{_fmt_amount(opp["amount"])}</td>'
                f'</tr>\n'
            )

    html += "    </table>\n"

    # ── Section 2: Guests WITHOUT opportunities ──
    html += f'    <h2>Repeat Guests without Opportunities ({len(guests_without_opps)})</h2>\n'
    html += '    <div class="callout">These repeat guests have no associated Salesforce Opportunity. They may represent upsell or re-engagement targets.</div>\n'
    html += "    <table>\n"
    html += (
        "      <tr>"
        "<th>Guest</th>"
        "<th>Email</th>"
        "<th>Account</th>"
        "<th>Language</th>"
        '<th class="num">Stays</th>'
        "<th>First Stay</th>"
        "<th>Last Stay</th>"
        "<th>Sales Rep</th>"
        "</tr>\n"
    )

    for g in guests_without_opps:
        html += (
            f'      <tr>'
            f'<td>{g["name"]}</td>'
            f'<td>{g["email"]}</td>'
            f'<td>{g["account_name"] or "—"}</td>'
            f'<td>{g["language"] or "—"}</td>'
            f'<td class="num">{g["stay_count"]}</td>'
            f'<td>{g["first_stay"] or "—"}</td>'
            f'<td>{g["last_stay"] or "—"}</td>'
            f'<td>{g["assigned_rep"] or "—"}</td>'
            f'</tr>\n'
        )

    html += "    </table>\n"

    # ── Stay frequency breakdown ──
    all_guests = guests_with_opps + guests_without_opps
    from collections import Counter
    stay_dist = Counter(g["stay_count"] for g in all_guests)

    html += "    <h2>Stay Frequency Distribution</h2>\n"
    html += "    <table>\n"
    html += '      <tr><th>Number of Stays</th><th class="num">Guests</th><th class="num">With Opp</th><th class="num">Without Opp</th></tr>\n'
    for cnt in sorted(stay_dist):
        n = stay_dist[cnt]
        w_opp = sum(1 for g in guests_with_opps if g["stay_count"] == cnt)
        wo_opp = sum(1 for g in guests_without_opps if g["stay_count"] == cnt)
        html += f'      <tr><td>{cnt} stays</td><td class="num">{n}</td><td class="num">{w_opp}</td><td class="num">{wo_opp}</td></tr>\n'

    html += "    </table>\n"

    html += """  </div>
  <div class="footer">
    This is an automated report. Data sourced from Salesforce TVRS Guest records.
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

    # Query all guest records with email
    print("Querying guest records...")
    records = sf_client.query(sf_holder, GUESTS_SOQL)
    print(f"Found {len(records)} guest records with email")

    # Group by email to find repeat guests
    repeat_guests = _group_repeat_guests(records)
    print(f"Found {len(repeat_guests)} repeat guests (2+ stays)")

    # Collect all account IDs for repeat guests
    account_ids = set()
    for stays in repeat_guests.values():
        for s in stays:
            if s.get("Account__c"):
                account_ids.add(s["Account__c"])

    # Query opportunities for these accounts
    print(f"Querying opportunities for {len(account_ids)} accounts...")
    opps_by_account = _query_opps_for_accounts(sf_holder, account_ids)

    # Build guest summaries
    guests_with_opps = []
    guests_without_opps = []

    for email, stays in repeat_guests.items():
        summary = _build_guest_summary(email, stays, opps_by_account)
        if summary["has_opportunity"]:
            guests_with_opps.append(summary)
        else:
            guests_without_opps.append(summary)

    # Sort: most stays first
    guests_with_opps.sort(key=lambda g: (-g["stay_count"], g["name"]))
    guests_without_opps.sort(key=lambda g: (-g["stay_count"], g["name"]))

    print(f"With opportunities: {len(guests_with_opps)}")
    print(f"Without opportunities: {len(guests_without_opps)}")

    # Build and send report
    print("Building report...")
    html = build_report_html(guests_with_opps, guests_without_opps, instance_url)

    today = date.today().strftime("%B %d, %Y")
    subject = f"Repeat Guest Report — {today} ({len(repeat_guests)} guests)"
    recipients = ["francis.phan@vinesofmendoza.com"]

    print(f"Sending to {', '.join(recipients)}...")
    email_sender.send_report(subject, html, recipients)
    print("Done!")


if __name__ == "__main__":
    main()
