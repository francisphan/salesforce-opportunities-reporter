#!/usr/bin/env python3
"""Weekly Salesforce Opportunity report — entry point."""

import sys
from datetime import date

import yaml

from src import sf_client, opportunities, email_sender, report_template


def load_subscribers(path: str = "subscribers.yaml") -> list[str]:
    with open(path) as f:
        data = yaml.safe_load(f)
    return data.get("subscribers", []) if data else []


def main():
    # Load subscribers
    recipients = load_subscribers()
    if not recipients:
        print("No subscribers configured. Skipping report.")
        sys.exit(0)

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

    # Render report
    today = date.today().strftime("%B %d, %Y")
    subject, html = report_template.render_report(opps, today, instance_url)

    # Send email
    print(f"Sending report to {len(recipients)} recipient(s)...")
    email_sender.send_report(subject, html, recipients)
    print("Done.")


if __name__ == "__main__":
    main()
