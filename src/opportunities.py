"""Query open Salesforce Opportunities and count human touches."""

from collections import defaultdict

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

OPEN_OPPS_SOQL = """
    SELECT Id, Name, StageName, Amount, CloseDate,
           OwnerId, Owner.Name, AccountId, Account.Name, LastModifiedDate
    FROM Opportunity
    WHERE IsClosed = false
    ORDER BY LastModifiedDate DESC
"""

TASKS_SOQL_TEMPLATE = """
    SELECT Id, WhatId, CreatedById, CreatedDate
    FROM Task
    WHERE WhatId IN ({ids})
"""

EVENTS_SOQL_TEMPLATE = """
    SELECT Id, WhatId, CreatedById, CreatedDate
    FROM Event
    WHERE WhatId IN ({ids})
"""

FIELD_HISTORY_SOQL_TEMPLATE = """
    SELECT OpportunityId, Field, OldValue, NewValue, CreatedById, CreatedDate
    FROM OpportunityFieldHistory
    WHERE OpportunityId IN ({ids})
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


def get_human_touched_opportunities(sf_holder: list) -> list[dict]:
    """Return open opportunities with >= 2 human touches, sorted by touch count."""
    # Step 1: Get all open opportunities
    opps = sf_client.query(sf_holder, OPEN_OPPS_SOQL)
    if not opps:
        return []

    opp_ids = [o["Id"] for o in opps]

    # Step 2: Get all touches (activities + field history)
    tasks = _query_batched(sf_holder, TASKS_SOQL_TEMPLATE, opp_ids)
    events = _query_batched(sf_holder, EVENTS_SOQL_TEMPLATE, opp_ids)
    field_history = _query_batched(sf_holder, FIELD_HISTORY_SOQL_TEMPLATE, opp_ids)

    # Step 3: Collect all user IDs that performed touches
    all_user_ids = set()
    for t in tasks:
        all_user_ids.add(t["CreatedById"])
    for e in events:
        all_user_ids.add(e["CreatedById"])
    for fh in field_history:
        all_user_ids.add(fh["CreatedById"])

    if not all_user_ids:
        return []

    # Step 4: Determine which users are human
    human_ids = _get_human_user_ids(sf_holder, list(all_user_ids))

    # Step 5: Count human touches per opportunity (O(n) single pass)
    touch_count = defaultdict(int)
    for t in tasks:
        if t["CreatedById"] in human_ids:
            touch_count[t["WhatId"]] += 1
    for e in events:
        if e["CreatedById"] in human_ids:
            touch_count[e["WhatId"]] += 1
    for fh in field_history:
        if fh["CreatedById"] in human_ids:
            touch_count[fh["OpportunityId"]] += 1

    # Step 6: Filter for >= 2 touches and enrich
    opp_map = {o["Id"]: o for o in opps}
    qualifying = []
    for opp_id, count in touch_count.items():
        if count >= 2:
            opp = opp_map[opp_id]
            opp["_touch_count"] = count
            qualifying.append(opp)

    qualifying.sort(key=lambda o: o["_touch_count"], reverse=True)
    return qualifying
