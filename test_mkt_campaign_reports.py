#!/usr/bin/env python3
"""Send test MKT Campaign reports with dummy data — no Salesforce connection needed.

Usage:
    python test_mkt_campaign_reports.py --email you@example.com
    python test_mkt_campaign_reports.py --email you@example.com --report individual
    python test_mkt_campaign_reports.py --email you@example.com --report overview
"""

import argparse
from datetime import date, timedelta

from dotenv import load_dotenv

load_dotenv()

from src import email_sender
from send_mkt_campaign_report import render_individual_report
from send_mkt_campaign_overview import render_overview_report

INSTANCE_URL = "https://thevinesofmendoza2.lightning.force.com"

# Sales reps for dummy data
REPS = [
    {"name": "Brenda Garcia", "email": "brenda@example.com"},
    {"name": "Pablo Martinez", "email": "pablo@example.com"},
    {"name": "Bryan Gilligan", "email": "bryan@example.com"},
]

STAGES = ["Deep Discovery", "Awaiting Demo", "Formal Proposal", "Negotiation"]

ACCOUNTS = [
    "Acme Wine Estates", "Napa Valley Holdings", "Bordeaux Partners",
    "Tuscany Ventures", "Sonoma Capital", "Patagonia Group",
    "Rioja International", "Barossa Investments", "Douro Heritage",
    "Stellenbosch Trust", "Willamette Corp", "Champagne Collective",
]


def _fake_date(days_ago: int) -> str:
    return (date.today() - timedelta(days=days_ago)).isoformat()


def generate_dummy_opps() -> list[dict]:
    """Generate realistic dummy opportunities across multiple reps and stages."""
    opps = []
    opp_num = 0

    scenarios = [
        # (rep_index, stage, amount, created_days_ago, last_activity_days_ago, touches)
        # Brenda — active and stale mix
        (0, "Deep Discovery", 75000, 20, 2, 5),
        (0, "Deep Discovery", 60000, 18, 3, 4),
        (0, "Deep Discovery", 55000, 25, 10, 2),  # stale
        (0, "Deep Discovery", 75000, 30, None, 0),  # no activity
        (0, "Awaiting Demo", 60000, 15, 1, 8),
        (0, "Awaiting Demo", 55000, 12, 5, 6),
        # Pablo — mostly active
        (1, "Deep Discovery", 60000, 22, 4, 3),
        (1, "Awaiting Demo", 75000, 10, 1, 10),
        (1, "Awaiting Demo", 55000, 14, 2, 7),
        (1, "Awaiting Demo", 60000, 8, 0, 12),
        (1, "Awaiting Demo", 75000, 16, 9, 3),  # stale
        (1, "Formal Proposal", 75000, 5, 1, 15),
        # Bryan — the rep whose individual report we'll send
        (2, "Deep Discovery", 60000, 28, 12, 1),  # stale
        (2, "Deep Discovery", 75000, 21, 3, 6),
        (2, "Awaiting Demo", 55000, 14, 6, 4),
        (2, "Formal Proposal", 60000, 7, 0, 9),
        (2, "Negotiation", 75000, 3, 0, 11),
    ]

    for rep_idx, stage, amount, created_ago, activity_ago, touches in scenarios:
        rep = REPS[rep_idx]
        account = ACCOUNTS[opp_num % len(ACCOUNTS)]
        opp_num += 1

        opps.append({
            "Id": f"006FAKE{opp_num:04d}000TEST",
            "Name": f"PV 2026 — {account}",
            "StageName": stage,
            "Amount": amount,
            "OwnerId": f"005FAKE{rep_idx:04d}",
            "Owner": {"Name": rep["name"], "Email": rep["email"]},
            "AccountId": f"001FAKE{opp_num:04d}",
            "Account": {"Name": account},
            "LeadSource": "Internal MKT Campaign",
            "CreatedDate": _fake_date(created_ago),
            "LastActivityDate": _fake_date(activity_ago) if activity_ago is not None else None,
            "_touch_count": touches,
        })

    return opps


def send_individual_test(email: str, opps: list[dict], cc: list[str] | None = None):
    """Send an individual report as if the recipient is Bryan."""
    bryan_opps = [o for o in opps if o["Owner"]["Name"] == "Bryan Gilligan"]
    subject, html = render_individual_report(bryan_opps, INSTANCE_URL, "Bryan")
    subject = f"[TEST] {subject}"
    print(f"Sending individual report ({len(bryan_opps)} opps) to {email}...")
    email_sender.send_report(subject, html, [email], cc=cc)


def send_overview_test(email: str, opps: list[dict], cc: list[str] | None = None):
    """Send the management overview."""
    subject, html = render_overview_report(opps)
    subject = f"[TEST] {subject}"
    print(f"Sending overview report ({len(opps)} opps) to {email}...")
    email_sender.send_report(subject, html, [email], cc=cc)


def main():
    parser = argparse.ArgumentParser(
        description="Send test MKT Campaign reports with dummy data (no SF needed)"
    )
    parser.add_argument("--email", required=True, help="Recipient email address")
    parser.add_argument("--cc", nargs="+", help="CC email address(es)")
    parser.add_argument(
        "--report",
        choices=["individual", "overview", "both"],
        default="both",
        help="Which report to send (default: both)",
    )
    args = parser.parse_args()

    cc = args.cc or []
    opps = generate_dummy_opps()
    print(f"Generated {len(opps)} dummy opportunities")

    if args.report in ("individual", "both"):
        send_individual_test(args.email, opps, cc=cc or None)

    if args.report in ("overview", "both"):
        send_overview_test(args.email, opps, cc=cc or None)

    print("Done!")


if __name__ == "__main__":
    main()
