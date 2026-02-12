#!/usr/bin/env python3
"""Weekly Salesforce Opportunity report — entry point."""

import os
import sys
from collections import defaultdict
from datetime import date

from dotenv import load_dotenv

load_dotenv()

from src import sf_client, opportunities, email_sender, report_template


def load_subscribers() -> set[str]:
    """Load subscriber emails from SUBSCRIBERS env var (comma-separated)."""
    raw = os.environ.get("SUBSCRIBERS", "")
    return {e.strip().lower() for e in raw.split(",") if e.strip()}


def load_cc() -> list[str]:
    """Load CC emails from REPORT_CC env var (comma-separated, optional)."""
    raw = os.environ.get("REPORT_CC", "")
    return [e.strip() for e in raw.split(",") if e.strip()]


def main():
    # Load subscribers
    subscribers = load_subscribers()
    if not subscribers:
        print("No subscribers configured (set SUBSCRIBERS env var). Skipping.")
        sys.exit(0)
    print(f"Loaded {len(subscribers)} subscriber(s)")

    # Connect to Salesforce
    print("Connecting to Salesforce...")
    sf = sf_client.connect()
    sf_holder = [sf]
    instance_url = f"https://{sf.sf_instance}"
    print(f"Connected to {sf.sf_instance}")

    # Fetch qualifying opportunities
    print("Querying opportunities...")
    opps = opportunities.get_human_touched_opportunities(sf_holder)
    print(f"Found {len(opps)} opportunities with 2+ human touches")

    if not opps:
        print("No qualifying opportunities. Skipping email.")
        sys.exit(0)

    # Group opportunities by owner email
    opps_by_owner = defaultdict(list)
    for opp in opps:
        owner_email = (opp.get("Owner", {}) or {}).get("Email", "")
        if owner_email:
            opps_by_owner[owner_email.lower()].append(opp)

    # Send personalized emails to subscribers who own opportunities
    today = date.today().strftime("%B %d, %Y")
    cc = load_cc()
    sent = 0

    for owner_email, owner_opps in opps_by_owner.items():
        if owner_email not in subscribers:
            continue

        owner_name = (owner_opps[0].get("Owner", {}) or {}).get("Name", "there")
        subject, html = report_template.render_report(
            owner_opps, today, instance_url, owner_name,
        )

        print(f"Sending {len(owner_opps)} opportunities to {owner_email}...")
        email_sender.send_report(subject, html, [owner_email], cc=cc)
        sent += 1

    print(f"Done. Sent reports to {sent} owner(s).")


if __name__ == "__main__":
    main()
