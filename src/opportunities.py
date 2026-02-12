"""Query open Salesforce Opportunities and count human touches (Tasks only)."""

from collections import defaultdict
from datetime import datetime, timezone, timedelta

from src import sf_client

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
STALE_THRESHOLD_DAYS = 60  # 2 months

OPEN_OPPS_SOQL = """
    SELECT Id, Name, StageName, Amount,
           OwnerId, Owner.Name, Owner.Email,
           AccountId, Account.Name, Account.PersonEmail, LastModifiedDate
    FROM Opportunity
    WHERE IsClosed = false
      AND CreatedDate = LAST_N_MONTHS:6
      AND (NOT Name LIKE '%TVG%')
    ORDER BY LastModifiedDate DESC
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
    """Format IDs for SOQL IN clause: 'id1','id2','id3'."""
    return ",".join(f"'{id_}'" for id_ in ids)


def _batch_ids(ids: list[str]) -> list[list[str]]:
    """Split IDs into batches for SOQL IN clauses."""
    return [ids[i:i + BATCH_SIZE] for i in range(0, len(ids), BATCH_SIZE)]


def _query_batched(sf_holder: list, template: str, ids: list[str]) -> list[dict]:
    """Run a SOQL query template in batches over a list of IDs."""
    results = []
    for batch in _batch_ids(ids):
        soql = template.format(ids=_ids_csv(batch))
        results.extend(sf_client.query(sf_holder, soql))
    return results


def _get_human_user_ids(sf_holder: list, user_ids: list[str]) -> set[str]:
    """Query users and return the set of IDs that are human (not automated)."""
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


def _parse_sf_datetime(dt_str: str) -> datetime:
    """Parse a Salesforce datetime string to a timezone-aware datetime."""
    # Salesforce formats: 2025-12-01T11:54:37.000+0000 or ...Z
    if dt_str.endswith("Z"):
        dt_str = dt_str[:-1] + "+00:00"
    elif dt_str.endswith("+0000"):
        dt_str = dt_str[:-5] + "+00:00"
    return datetime.fromisoformat(dt_str)


def get_human_touched_opportunities(sf_holder: list) -> list[dict]:
    """Return all open opportunities (last 6 months, excluding TVG), enriched with touch data.

    Each opportunity is enriched with:
      _touch_count: int — number of human Tasks (0 if none)
      _last_touched: str — ISO date of most recent human Task, or "Never"
      _is_stale: bool — True if last touch was > 2 months ago or never touched
      Owner.Email: str — owner's email for per-person routing
    """
    # Step 1: Get open opportunities (last 6 months, excluding TVG)
    opps = sf_client.query(sf_holder, OPEN_OPPS_SOQL)
    if not opps:
        return []

    opp_ids = [o["Id"] for o in opps]

    # Step 2: Get all Tasks linked to these opportunities
    tasks = _query_batched(sf_holder, TASKS_SOQL_TEMPLATE, opp_ids)

    # Step 3: Collect all user IDs that created Tasks and filter to humans
    all_user_ids = {t["CreatedById"] for t in tasks}
    human_ids = _get_human_user_ids(sf_holder, list(all_user_ids)) if all_user_ids else set()

    # Step 4: Count human touches and track last touch date per opportunity
    touch_count = defaultdict(int)
    last_touch = {}  # opp_id -> most recent CreatedDate string
    for t in tasks:
        if t["CreatedById"] in human_ids:
            opp_id = t["WhatId"]
            touch_count[opp_id] += 1
            created = t["CreatedDate"]
            if opp_id not in last_touch or created > last_touch[opp_id]:
                last_touch[opp_id] = created

    # Step 5: Enrich all opportunities with touch data
    now = datetime.now(timezone.utc)
    stale_cutoff = now - timedelta(days=STALE_THRESHOLD_DAYS)

    for opp in opps:
        opp_id = opp["Id"]
        opp["_touch_count"] = touch_count.get(opp_id, 0)
        if opp_id in last_touch:
            opp["_last_touched"] = last_touch[opp_id][:10]  # YYYY-MM-DD
            last_dt = _parse_sf_datetime(last_touch[opp_id])
            opp["_is_stale"] = last_dt < stale_cutoff
        else:
            opp["_last_touched"] = "Never"
            opp["_is_stale"] = True  # no touches at all = needs attention

    # Sort: stale first (high priority), then by touch count descending
    opps.sort(key=lambda o: (not o["_is_stale"], -o["_touch_count"]))
    return opps
