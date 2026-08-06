"""Unit tests for config_bridge.py's PROVIDER_FOLDER_MAP row generation.

Covers the two matching-identifier columns (column_mappings, matching_
identifiers) added to PROVIDER_FOLDER_MAP: the full raw->canonical mapping
and the resolved-chain "matched on" list, computed the same way the AWS
orchestrator / notify_sql.py's email already do — see config_bridge.py's
module comment for why this is a ported copy rather than a shared import.
"""

from __future__ import annotations

from pathlib import Path

from matchbot_snowflake.config_bridge import (
    build_provider_folder_map_rows,
    matching_identifiers_for_provider,
    render_merge_sql,
)
from matchbot_snowflake.config_models import load_config

CONFIG_DIR = Path(__file__).resolve().parents[3] / "config"


def _row(rows: list[dict], provider_id: str) -> dict:
    return next(r for r in rows if r["provider_id"] == provider_id)


def test_ride_column_mappings_matches_yaml() -> None:
    app_config = load_config(CONFIG_DIR)
    rows = build_provider_folder_map_rows(app_config)
    ride = _row(rows, "ride_enrollment")

    assert ride["column_mappings"]["SASID"] == "member_external_id"
    assert ride["column_mappings"]["FIRSTNAME"] == "first_name"
    assert ride["column_mappings"]["LASTNAME"] == "last_name"


def test_ride_matching_identifiers_is_external_id_only() -> None:
    """RIDE maps no ssn/birth_date/address1 — filter_chain_by_provider_
    attributes drops every rule except deterministic_external_id, so
    matching_identifiers must be exactly RIDE's own external_id_column
    ("sasid") — not the generic "External ID" label, so a report reader
    can tell which actual identifier RIDE matched on."""
    app_config = load_config(CONFIG_DIR)
    rows = build_provider_folder_map_rows(app_config)
    ride = _row(rows, "ride_enrollment")

    assert ride["matching_identifiers"] == ["sasid"]


def test_risos_voter_column_mappings_includes_birth_date() -> None:
    """DATE_OF_BIRTH -> birth_date is mapped in provider_risos.yaml — this
    is the field the user asked about ('why don't we have date of birth')."""
    app_config = load_config(CONFIG_DIR)
    rows = build_provider_folder_map_rows(app_config)
    risos = _row(rows, "risos_voter")

    assert risos["column_mappings"]["DATE_OF_BIRTH"] == "birth_date"


def test_risos_voter_matching_identifiers_includes_birth_date() -> None:
    """RISOS maps first_name/last_name/birth_date/address1/city/state/zip,
    so the resolved chain includes far more than just its external id —
    Birth Date specifically must appear since deterministic_name_dob's
    keys resolve against RISOS's mapped attributes. The external-id entry
    is RISOS's own external_id_column ("voter_id"), not the generic
    "External ID" label — this is what distinguishes RISOS's report from
    RIDE's ("sasid") at a glance."""
    app_config = load_config(CONFIG_DIR)
    rows = build_provider_folder_map_rows(app_config)
    risos = _row(rows, "risos_voter")

    assert "Birth Date" in risos["matching_identifiers"]
    assert "voter_id" in risos["matching_identifiers"]
    assert "External ID" not in risos["matching_identifiers"]
    assert "First Name" in risos["matching_identifiers"]
    assert "Last Name" in risos["matching_identifiers"]


def test_risos_voter_matching_identifiers_includes_address_fields() -> None:
    """Regression test for a real bug: _DERIVED_ATTRIBUTE_SOURCE was
    missing address1_std/zip5/ssn4 entries, so
    filter_chain_by_provider_attributes silently dropped
    deterministic_name_addr_city_state and deterministic_name_addr_zip
    from RISOS's resolved chain even though RISOS maps address1/city/
    state/zip — confirmed live via an email showing only
    voter_id/First Name/Last Name/Birth Date. Address/City/State/Zip must
    all appear now, with clean display labels (not the raw attribute
    names like "Address1 Std"/"Zip5" the title-case fallback would
    otherwise produce)."""
    app_config = load_config(CONFIG_DIR)
    rows = build_provider_folder_map_rows(app_config)
    risos = _row(rows, "risos_voter")

    assert "Address" in risos["matching_identifiers"]
    assert "City" in risos["matching_identifiers"]
    assert "State" in risos["matching_identifiers"]
    assert "Zip" in risos["matching_identifiers"]
    assert "Address1 Std" not in risos["matching_identifiers"]
    assert "Zip5" not in risos["matching_identifiers"]


def test_generic_external_id_label_is_replaced_with_the_real_column_name() -> None:
    """Both RIDE and RISOS resolve deterministic_external_id, but each must
    show ITS OWN external_id_column, not the shared generic "External ID"
    label -- this is the whole point of storing external_id_column
    alongside matching_identifiers on the same PROVIDER_FOLDER_MAP row."""
    app_config = load_config(CONFIG_DIR)
    rows = build_provider_folder_map_rows(app_config)
    ride = _row(rows, "ride_enrollment")
    risos = _row(rows, "risos_voter")

    assert ride["external_id_column"] in ride["matching_identifiers"]
    assert risos["external_id_column"] in risos["matching_identifiers"]
    assert ride["matching_identifiers"] != risos["matching_identifiers"][:1]


def test_land_only_file_type_has_empty_matching_identifiers() -> None:
    """risos_voterhistory has matches_dataset=false (land + transform only,
    never runs person-linkage matching) — matching_identifiers must be an
    empty list, not an attempt to compute one against a chain that never
    actually runs for this file type."""
    app_config = load_config(CONFIG_DIR)
    rows = build_provider_folder_map_rows(app_config)
    voterhistory = _row(rows, "risos_voterhistory")

    assert voterhistory["matching_identifiers"] == []


def test_matching_identifiers_for_provider_matches_the_row_builder() -> None:
    """The standalone helper and the row-builder's call to it must agree —
    guards against the two ever silently diverging."""
    app_config = load_config(CONFIG_DIR)
    ride_provider = app_config.providers["ride_enrollment"]
    assert matching_identifiers_for_provider(ride_provider, app_config) == ["sasid"]


def test_render_merge_sql_wraps_structured_columns_in_parse_json() -> None:
    """column_mappings/matching_identifiers are VARIANT/ARRAY-typed — the
    generated SQL must PARSE_JSON each one from a JSON text literal rather
    than attempt to hold them in a plain VALUES(...) row constructor (which
    can't type a literal as VARIANT/ARRAY directly)."""
    app_config = load_config(CONFIG_DIR)
    rows = build_provider_folder_map_rows(app_config)
    sql = render_merge_sql(rows)

    assert sql.count("PARSE_JSON(") == len(rows) * 2
    assert "column_mappings = source.column_mappings" in sql
    assert "matching_identifiers = source.matching_identifiers" in sql


def test_render_merge_sql_empty_rows_is_a_noop_comment() -> None:
    assert render_merge_sql([]) == "-- no providers configured; nothing to merge"
