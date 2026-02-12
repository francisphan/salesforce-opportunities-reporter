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
    SELECT Id, Name, StageName, Amount, CloseDate,
           OwnerId, Owner.Name, Owner.Email,
           AccountId, Account.Name, LastModifiedDate
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
    """Return open opportunities with >= 2 human touches, enriched with touch data.

    Each opportunity is enriched with:
      _touch_count: int — number of human Tasks
      _last_touched: str — ISO date of most recent human Task
      _is_stale: bool — True if last touch was > 2 months ago
      Owner.Email: str — owner's email for per-person routing
    """
    # Step 1: Get open opportunities (last 6 months, excluding TVG)
    opps = sf_client.query(sf_holder, OPEN_OPPS_SOQL)
    if not opps:
        return []

    opp_ids = [o["Id"] for o in opps]

    # Step 2: Get all Tasks linked to these opportunities
    tasks = _query_batched(sf_holder, TASKS_SOQL_TEMPLATE, opp_ids)

    # Step 3: Collect all user IDs that created Tasks
    all_user_ids = set()
    for t in tasks:
        all_user_ids.add(t["CreatedById"])

    if not all_user_ids:
        return []

    # Step 4: Determine which users are human
    human_ids = _get_human_user_ids(sf_holder, list(all_user_ids))

    # Step 5: Count human touches and track last touch date per opportunity
    touch_count = defaultdict(int)
    last_touch = {}  # opp_id -> most recent CreatedDate string
    for t in tasks:
        if t["CreatedById"] in human_ids:
            opp_id = t["WhatId"]
            touch_count[opp_id] += 1
            created = t["CreatedDate"]
            if opp_id not in last_touch or created > last_touch[opp_id]:
                last_touch[opp_id] = created

    # Step 6: Filter for >= 2 touches and enrich
    now = datetime.now(timezone.utc)
    stale_cutoff = now - timedelta(days=STALE_THRESHOLD_DAYS)
    opp_map = {o["Id"]: o for o in opps}
    qualifying = []

    for opp_id, count in touch_count.items():
        if count >= 2:
            opp = opp_map[opp_id]
            opp["_touch_count"] = count
            opp["_last_touched"] = last_touch[opp_id][:10]  # YYYY-MM-DD
            last_dt = _parse_sf_datetime(last_touch[opp_id])
            opp["_is_stale"] = last_dt < stale_cutoff
            qualifying.append(opp)

    # Sort: stale first (high priority), then by touch count descending
    qualifying.sort(key=lambda o: (not o["_is_stale"], -o["_touch_count"]))
    return qualifying
