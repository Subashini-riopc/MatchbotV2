"""Regression tests for three real bugs caught during live Snowflake
validation (build step 3 — see docs/snowflake-implementation-plan.md):

1. provider_sql.py quoted raw column references as lowercase string
   literals (``land."firstname"``), which is a case-sensitive identifier
   that doesn't match RIDE_LAND's actual (unquoted-DDL, uppercase-folded)
   column FIRSTNAME — Snowflake rejected it as an invalid identifier.
2. derive_sql.py used ARRAY_REMOVE(array, NULL) to drop NULL placeholders
   left by TRANSFORM's suffix-stripping lambda. Snowflake's ARRAY_REMOVE
   removes elements *equal to* its second argument, and NULL never equals
   anything (including itself) — so ARRAY_REMOVE(arr, NULL) always failed
   to match and returned NULL for the whole array, silently propagating
   NULL through first_name_std/last_name_std/every metaphone column.
3. render_provider_projection_sql's FROM {land_table} had no
   pipeline_run_id filter, so it re-projected every historical run's rows
   in the land table (never truncated between runs) on every call — a
   live 8th CALL RUN_MATCH_PIPELINE staged 6895 rows (7 accumulated runs
   x 985) instead of 985.

All three were invisible to plain Python unit tests (which only check
the SQL *text*, not execute it) until run against a live warehouse — kept
here as permanent regressions against re-introducing any of them.
"""

from __future__ import annotations

from pathlib import Path

from matchbot.config.loader import load_config

from matchbot_snowflake.provider_sql import render_provider_projection_sql

CONFIG_DIR = Path(__file__).resolve().parents[3] / "config"


def _ride_projection_sql() -> str:
    app_config = load_config(CONFIG_DIR)
    provider = app_config.provider("ride_enrollment")
    return render_provider_projection_sql(
        provider,
        app_config.global_config.standardization,
        land_table="RIDE_LAND",
        pipeline_run_id=1,
    )


def test_land_column_references_are_unquoted() -> None:
    """No quoted-lowercase land."..." references — see bug #1 above."""
    sql = _ride_projection_sql()
    assert 'land."' not in sql


def test_land_column_references_use_configured_casing() -> None:
    """Raw columns come through as land.<COLUMN>, matching
    config/providers/provider_ride_enrollment.yaml's column_mappings keys
    verbatim (uppercase in RIDE's case) with no quoting."""
    sql = _ride_projection_sql()
    assert "land.FIRSTNAME" in sql
    assert "land.LASTNAME" in sql
    assert "land.SASID" in sql


def test_derive_sql_uses_array_compact_not_array_remove_null() -> None:
    """std_name_sql must use ARRAY_COMPACT to drop NULL placeholders — see
    bug #2 above. ARRAY_REMOVE(arr, NULL) is never valid here."""
    sql = _ride_projection_sql()
    assert "ARRAY_COMPACT(" in sql
    assert "ARRAY_REMOVE(" not in sql, (
        "ARRAY_REMOVE(array, NULL) silently returns NULL on Snowflake instead "
        "of removing NULL elements — use ARRAY_COMPACT instead"
    )


def test_unmapped_birth_date_is_typed_null() -> None:
    """RIDE maps no birth_date column at all, so canonical_sql() must emit
    a typed NULL::DATE, not a bare NULL — YEAR()/MONTH()/DAY() (called on
    birth_date in the final SELECT) reject an untyped NULL argument
    outright ('Function EXTRACT does not support NULL argument type'),
    caught during build-step-3 live validation."""
    sql = _ride_projection_sql()
    assert "NULL::DATE" in sql


def test_projection_filters_land_table_by_pipeline_run_id() -> None:
    """See bug #3 above — without this filter, every historical run's
    rows in the (never-truncated) land table get re-staged on every call."""
    sql = _ride_projection_sql()
    assert "land.pipeline_run_id = 1" in sql


def test_skip_if_null_and_pipeline_run_id_filters_combine_with_and() -> None:
    """RIDE configures skip_if_null on member_external_id (see
    config/providers/provider_ride_enrollment.yaml) — that condition must
    coexist with the pipeline_run_id filter, not replace it."""
    sql = _ride_projection_sql()
    assert sql.count("WHERE") == 1
    assert "land.pipeline_run_id = 1 AND" in sql or "AND land.pipeline_run_id = 1" in sql
