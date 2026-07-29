"""Unit tests for voter_history_sql.py — the RISOS VoterHistory unpivot.

VoterHistory's land table has 8 repeated election-slot column groups
(DATE_1/ELECTION_1/TYPE_1/PRECINCT_1/PARTY_1 ... _8) — this module turns
each populated slot into its own output row (one row per (voter, election)
pair a voter actually participated in), plus one did_not_vote=TRUE row per
voter whose 8 slots are all empty. See provider_risos_voterhistory.yaml's
module docstring for why this needed its own generator rather than reusing
provider_sql.py's flat column_mappings-driven projection.
"""

from __future__ import annotations

from matchbot_snowflake.voter_history_sql import (
    STAGE_TABLE,
    render_voter_history_projection_sql,
)


def test_stage_table_name() -> None:
    assert STAGE_TABLE == "RISOS_VOTERHISTORY_STAGE"


def test_generates_one_branch_per_slot_plus_did_not_vote() -> None:
    sql = render_voter_history_projection_sql("RISOS_VOTERHISTORY_LAND", 1)
    # 8 slot branches + 1 did_not_vote branch = 9 SELECTs, 8 UNION ALLs.
    assert sql.count("UNION ALL") == 8
    assert sql.count("SELECT") == 9


def test_every_slot_column_referenced() -> None:
    sql = render_voter_history_projection_sql("RISOS_VOTERHISTORY_LAND", 1)
    for slot in range(1, 9):
        assert f"land.DATE_{slot}" in sql
        assert f"land.ELECTION_{slot}" in sql
        assert f"land.TYPE_{slot}" in sql
        assert f"land.PRECINCT_{slot}" in sql
        assert f"land.PARTY_{slot}" in sql


def test_voter_id_zero_padded_to_11_in_every_branch() -> None:
    sql = render_voter_history_projection_sql("RISOS_VOTERHISTORY_LAND", 1)
    assert sql.count("LPAD(TRIM(land.VOTER_ID), 11, '0')") == 9


def test_precinct_zero_padded_to_4_in_slot_branches() -> None:
    sql = render_voter_history_projection_sql("RISOS_VOTERHISTORY_LAND", 1)
    for slot in range(1, 9):
        assert f"LPAD(TRIM(land.PRECINCT_{slot}), 4, '0')" in sql


def test_election_date_parsed_with_mm_dd_yyyy_format() -> None:
    sql = render_voter_history_projection_sql("RISOS_VOTERHISTORY_LAND", 1)
    for slot in range(1, 9):
        assert f"TRY_TO_DATE(land.DATE_{slot}, 'MM/DD/YYYY')" in sql


def test_slot_branch_filters_out_fully_empty_slots() -> None:
    """A slot only produces a row when at least one of its 5 columns is
    non-null — an entirely empty slot (voter didn't vote in that election)
    must not become an all-NULL row."""
    sql = render_voter_history_projection_sql("RISOS_VOTERHISTORY_LAND", 1)
    assert "land.DATE_1 IS NOT NULL OR land.ELECTION_1 IS NOT NULL" in sql
    assert "OR land.TYPE_1 IS NOT NULL OR land.PRECINCT_1 IS NOT NULL" in sql
    assert "OR land.PARTY_1 IS NOT NULL" in sql


def test_did_not_vote_branch_requires_all_8_slots_empty() -> None:
    sql = render_voter_history_projection_sql("RISOS_VOTERHISTORY_LAND", 1)
    assert "TRUE AS did_not_vote" in sql
    # The did_not_vote WHERE clause must AND together all 8 slots' "every
    # column is NULL" condition — not just check slot 1.
    for slot in range(1, 9):
        assert f"land.DATE_{slot} IS NULL AND land.ELECTION_{slot} IS NULL" in sql
        assert f"AND land.TYPE_{slot} IS NULL AND land.PRECINCT_{slot} IS NULL" in sql
        assert f"AND land.PARTY_{slot} IS NULL" in sql


def test_did_not_vote_branch_nulls_out_election_fields() -> None:
    sql = render_voter_history_projection_sql("RISOS_VOTERHISTORY_LAND", 1)
    did_not_vote_branch = sql.split("UNION ALL")[-1]
    assert "NULL::DATE AS election_date" in did_not_vote_branch
    assert "NULL AS election_name" in did_not_vote_branch
    assert "NULL AS vote_type" in did_not_vote_branch
    assert "NULL AS precinct" in did_not_vote_branch
    assert "NULL AS party" in did_not_vote_branch


def test_pipeline_run_id_scopes_every_branch() -> None:
    """Without this filter, every historical run's rows would be
    re-transformed and re-inserted on every subsequent call (same failure
    mode documented in provider_sql.py's render_provider_projection_sql)."""
    sql = render_voter_history_projection_sql("RISOS_VOTERHISTORY_LAND", 42)
    assert sql.count("land.pipeline_run_id = 42") == 9


def test_land_table_name_is_parameterized() -> None:
    sql = render_voter_history_projection_sql("SOME_OTHER_LAND", 1)
    assert "FROM SOME_OTHER_LAND AS land" in sql
    assert sql.count("FROM SOME_OTHER_LAND AS land") == 9
