#!/usr/bin/env python3
"""Auto-tag TVOM Vineyards opportunities with the PV Campaign 2026.

Finds open opportunities where Service__c = 'TVOM Vineyards' that are not yet
associated with the PV Campaign 2026, and sets their CampaignId.

Intended to run daily (e.g. via cron or GitHub Actions) to catch new opps
created by sales reps without requiring them to manually set the campaign.

Usage:
    python3 tag_pv_campaign_opps.py              # Tag all untagged opps
    python3 tag_pv_campaign_opps.py --dry-run    # Preview without updating
"""

import sys

from dotenv import load_dotenv

load_dotenv()

from src import sf_client

PV_CAMPAIGN_ID = "701PW00001T1KhrYAF"  # PV Campaign 2026
CAMPAIGN_START = "2026-03-31T00:00:00Z"
CAMPAIGN_END = "2027-01-01T00:00:00Z"

FIND_UNTAGGED_SOQL = f"""
    SELECT Id, Name, StageName, Owner.Name, CreatedDate
    FROM Opportunity
    WHERE Service__c = 'TVOM Vineyards'
      AND CampaignId = null
      AND CreatedDate >= {CAMPAIGN_START}
      AND CreatedDate < {CAMPAIGN_END}
    ORDER BY CreatedDate DESC
"""


def main():
    dry_run = "--dry-run" in sys.argv

    print("Connecting to Salesforce...")
    sf = sf_client.connect()
    sf_holder = [sf]
    print(f"Connected to {sf.sf_instance}")

    print("Finding untagged TVOM Vineyards opportunities...")
    opps = sf_client.query(sf_holder, FIND_UNTAGGED_SOQL)
    print(f"Found {len(opps)} untagged opportunities")

    if not opps:
        print("Nothing to tag.")
        return

    for opp in opps:
        owner = (opp.get("Owner") or {}).get("Name", "?")
        print(f"  {opp['Name']:50s}  {opp['StageName']:20s}  {owner}")

    if dry_run:
        print(f"\nDRY RUN — would tag {len(opps)} opportunities with PV Campaign 2026")
        return

    print(f"\nTagging {len(opps)} opportunities with CampaignId = {PV_CAMPAIGN_ID}...")
    tagged = 0
    for opp in opps:
        try:
            sf.Opportunity.update(opp["Id"], {"CampaignId": PV_CAMPAIGN_ID})
            tagged += 1
        except Exception as e:
            print(f"  ERROR tagging {opp['Name']}: {e}")

    print(f"Done. Tagged {tagged}/{len(opps)} opportunities.")


if __name__ == "__main__":
    main()
