"""Unit tests for provider_sql.py's combined_columns SQL generation.

Covers CombinedColumnSpec (config_models.py): concatenating multiple raw
source columns into one canonical attribute, e.g. RISOS's STREET_NUMBER +
STREET_NAME into address1 — a real fix for a bare house number otherwise
being the only thing every address-anchored matcher compared. See
provider_risos.yaml's combined_columns comment and canonical.py's Polars
counterpart (tests/test_canonical_combined_columns.py) for the AWS side.
"""

from __future__ import annotations

from pathlib import Path

from matchbot.config.loader import load_config

from matchbot_snowflake.provider_sql import render_provider_projection_sql

CONFIG_DIR = Path(__file__).resolve().parents[3] / "config"


def _risos_projection_sql() -> str:
    app_config = load_config(CONFIG_DIR)
    provider = app_config.provider("risos_voter")
    return render_provider_projection_sql(
        provider,
        app_config.global_config.standardization,
        land_table="RISOS_VOTER_LAND",
        pipeline_run_id=1,
    )


def test_address1_concatenates_street_number_and_street_name() -> None:
    sql = _risos_projection_sql()
    assert "land.STREET_NUMBER" in sql
    assert "land.STREET_NAME" in sql
    # Concatenated with the configured separator (a literal space), not
    # left as two independent columns.
    assert "|| ' ' ||" in sql


def test_combined_columns_are_null_safe_via_coalesce() -> None:
    """A row missing one piece must still produce the piece it has, not a
    fully-NULL address1 — CONCAT/|| propagates NULL if ANY operand is
    NULL, so each raw ref must be COALESCE'd to '' first."""
    sql = _risos_projection_sql()
    assert "COALESCE(land.STREET_NUMBER, '')" in sql
    assert "COALESCE(land.STREET_NAME, '')" in sql


def test_combined_address1_still_goes_through_its_configured_transform() -> None:
    """provider_risos.yaml declares address1: {upper: true, trim: true} —
    this must still apply on TOP of the combined expression, not be
    silently bypassed (a real bug caught before this test existed: an
    earlier version routed combined_columns straight to a bare trim/
    NULLIF, skipping the provider's own configured transform entirely)."""
    sql = _risos_projection_sql()
    idx = sql.find("AS address1")
    assert idx != -1
    address1_expr = sql[:idx]
    # UPPER(TRIM(...)) must wrap the combined (STREET_NUMBER || ' ' ||
    # STREET_NAME) expression, not just NULLIF(TRIM(...), '') alone.
    assert "UPPER(TRIM((COALESCE(land.STREET_NUMBER" in address1_expr


def test_address2_is_null_since_risos_leaves_it_unmapped() -> None:
    """address2 is deliberately not in RISOS's column_mappings or
    combined_columns — RISOS has no genuine second address line."""
    sql = _risos_projection_sql()
    assert "NULL AS address2" in sql
