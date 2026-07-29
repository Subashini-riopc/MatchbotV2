"""RISOS VoterHistory land -> stage projection: an UNPIVOT, not a flat
column_mappings-driven projection like provider_sql.py's.

VoterHistory's land table (RISOS_VOTERHISTORY_LAND) has 8 repeated
"election slot" column groups — DATE_1/ELECTION_1/TYPE_1/PRECINCT_1/PARTY_1
through _8 — one voter row per land row, with up to 8 elections packed
into it side by side. The legacy system (risos/models/voter_history.py)
turns each populated slot into its own row against a real `dimensions
.Election` entity, one row per (voter, election) pair — this module mirrors
that shape in SQL: one output row per non-empty slot, UNION ALL'd across
all 8 slots, landing in RISOS_VOTERHISTORY_STAGE.

This is deliberately NOT expressed via ProviderConfig.column_mappings —
that schema is a 1:1 raw-column -> canonical-attribute mapping, and has no
way to say "repeat this mapping 8 times, varying the column suffix, and
turn each repetition into its own output row." See
config/providers/provider_risos_voterhistory.yaml's module docstring.

VoterHistory never runs person-linkage matching (matches_dataset: false in
its YAML — legacy's PeripheralDataRow, not IndividualDataRow), so this
module's output never touches RILDS_STAGE/WINNERS/matching at all — it's
called directly by run_pipeline.py for any matches_dataset=false file type,
landing in its own dedicated stage table instead.
"""

from __future__ import annotations

# Legacy (risos/models/voter_history.py) zero-pads VOTER_ID to 11 chars
# (same width as the primary Voter file — see provider_risos.yaml's
# member_external_id zero_pad) and PRECINCT_i to 4 chars, per-election-slot,
# in its own ImporterField setup — replicated here for parity.
_VOTER_ID_WIDTH = 11
_PRECINCT_WIDTH = 4

# Matches provider_risos.yaml's birth_date format for the primary Voter
# file — RISOS's DATE_i columns use the same MM/DD/YYYY shape (confirmed
# live against RISOS_VOTERHISTORY_LAND).
_DATE_FORMAT_SQL = "MM/DD/YYYY"

STAGE_TABLE = "RISOS_VOTERHISTORY_STAGE"

# The 8 repeated slot column-name groups, e.g. slot 1 -> DATE_1, ELECTION_1,
# TYPE_1, PRECINCT_1, PARTY_1. Matches land_sql.py's sanitized column
# naming for a header token like "DATE 1" -> "DATE_1".
_NUM_SLOTS = 8


def _slot_columns(slot: int) -> dict[str, str]:
    return {
        "date": f"DATE_{slot}",
        "election": f"ELECTION_{slot}",
        "type": f"TYPE_{slot}",
        "precinct": f"PRECINCT_{slot}",
        "party": f"PARTY_{slot}",
    }


def render_voter_history_projection_sql(
    land_table: str, pipeline_run_id: int
) -> str:
    """Render the SELECT that unpivots ``land_table`` into one row per
    (voter, election) pair, ready to INSERT INTO RISOS_VOTERHISTORY_STAGE.

    One UNION ALL branch per slot (1..8): a slot only produces a row when
    at least one of its 5 columns is non-null for that voter — an entirely
    empty slot (a voter who only voted in fewer than 8 elections) is
    skipped rather than emitted as an all-NULL row. A separate final branch
    emits exactly one row per voter with EVERY slot empty, marked
    did_not_vote = TRUE (mirrors legacy's rilds_did_not_vote bulk-update:
    "rows where precinct/party/type are all null across every election
    slot").

    ``pipeline_run_id`` scopes to only the rows THIS run landed — same
    reasoning as provider_sql.py's render_provider_projection_sql
    (RISOS_VOTERHISTORY_LAND is never truncated between runs, so without
    this filter every historical run's rows would be re-transformed and
    re-inserted on every subsequent call).
    """
    slot_branches = []
    for slot in range(1, _NUM_SLOTS + 1):
        cols = _slot_columns(slot)
        slot_branches.append(
            f"""SELECT
        land.id AS source_row_id,
        NULLIF(LPAD(TRIM(land.VOTER_ID), {_VOTER_ID_WIDTH}, '0'), '') AS voter_id,
        TRY_TO_DATE(land.{cols['date']}, '{_DATE_FORMAT_SQL}') AS election_date,
        NULLIF(TRIM(land.{cols['election']}), '') AS election_name,
        NULLIF(TRIM(land.{cols['type']}), '') AS vote_type,
        NULLIF(LPAD(TRIM(land.{cols['precinct']}), {_PRECINCT_WIDTH}, '0'), '') AS precinct,
        NULLIF(TRIM(land.{cols['party']}), '') AS party,
        FALSE AS did_not_vote
    FROM {land_table} AS land
    WHERE land.pipeline_run_id = {pipeline_run_id}
        AND (
            land.{cols['date']} IS NOT NULL OR land.{cols['election']} IS NOT NULL
            OR land.{cols['type']} IS NOT NULL OR land.{cols['precinct']} IS NOT NULL
            OR land.{cols['party']} IS NOT NULL
        )"""
        )

    all_slot_conditions = " AND ".join(
        f"(land.{c['date']} IS NULL AND land.{c['election']} IS NULL "
        f"AND land.{c['type']} IS NULL AND land.{c['precinct']} IS NULL "
        f"AND land.{c['party']} IS NULL)"
        for c in (_slot_columns(slot) for slot in range(1, _NUM_SLOTS + 1))
    )
    did_not_vote_branch = f"""SELECT
        land.id AS source_row_id,
        NULLIF(LPAD(TRIM(land.VOTER_ID), {_VOTER_ID_WIDTH}, '0'), '') AS voter_id,
        NULL::DATE AS election_date,
        NULL AS election_name,
        NULL AS vote_type,
        NULL AS precinct,
        NULL AS party,
        TRUE AS did_not_vote
    FROM {land_table} AS land
    WHERE land.pipeline_run_id = {pipeline_run_id}
        AND {all_slot_conditions}"""

    return "\nUNION ALL\n".join(slot_branches + [did_not_vote_branch])
