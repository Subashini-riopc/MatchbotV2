"""Unit tests for matcher_registry.py / matchers/deterministic.py.

Verifies the SQL fragments generated for the real config/global.yaml
matcher chain match the expected shape (priority order, method labels,
join/guard predicates) — a regression fixture for the highest-risk piece
of the demo's matching logic. See docs/snowflake-implementation-plan.md.
"""

from __future__ import annotations

from pathlib import Path

from matchbot.config.loader import load_config

from matchbot_snowflake.matcher_registry import build_sql_fragments

CONFIG_DIR = Path(__file__).resolve().parents[3] / "config"


def test_real_chain_produces_four_fragments_in_order() -> None:
    app_config = load_config(CONFIG_DIR)
    fragments = build_sql_fragments(app_config.global_config.matching.matchers, "sasid")

    assert [f.name for f in fragments] == [
        "deterministic_external_id",
        "deterministic_ssn",
        "deterministic_name_dob",
        "deterministic_name_addr",
    ]
    assert [f.priority for f in fragments] == [1, 2, 3, 4]


def test_external_id_matcher_reports_exact_sasid() -> None:
    app_config = load_config(CONFIG_DIR)
    fragments = build_sql_fragments(app_config.global_config.matching.matchers, "sasid")
    external_id = next(f for f in fragments if f.name == "deterministic_external_id")

    assert external_id.method_label == "EXACT_SASID"
    assert "s.rilds_id" in external_id.join_predicate_sql
    # Reference side has no rilds_id column — RIDE's configured
    # external_id_column ("sasid") is what it's actually compared against.
    assert "r.sasid" in external_id.join_predicate_sql
    assert "r.rilds_id" not in external_id.join_predicate_sql


def test_other_deterministic_matchers_report_plain_exact() -> None:
    app_config = load_config(CONFIG_DIR)
    fragments = build_sql_fragments(app_config.global_config.matching.matchers, "sasid")

    for name in ("deterministic_ssn", "deterministic_name_dob", "deterministic_name_addr"):
        fragment = next(f for f in fragments if f.name == name)
        assert fragment.method_label == "EXACT"


def test_birth_date_compares_natively_not_as_string() -> None:
    """birth_date is a DATE column on both sides — must NOT be cast through
    VARCHAR for comparison (see matchers/deterministic.py's
    _NON_STRING_KEYS), only NULL-checked in the guard, not blank-checked."""
    app_config = load_config(CONFIG_DIR)
    fragments = build_sql_fragments(app_config.global_config.matching.matchers, "sasid")
    name_dob = next(f for f in fragments if f.name == "deterministic_name_dob")

    assert "s.birth_date = r.birth_date" in name_dob.join_predicate_sql
    assert "birth_date::VARCHAR" not in name_dob.join_predicate_sql
    assert "s.birth_date IS NOT NULL" in name_dob.guard_predicate_sql


def test_multi_key_matchers_and_all_keys_in_join_and_guard() -> None:
    app_config = load_config(CONFIG_DIR)
    fragments = build_sql_fragments(app_config.global_config.matching.matchers, "sasid")
    name_addr = next(f for f in fragments if f.name == "deterministic_name_addr")

    for key in ("first_name_std", "last_name_std", "address1"):
        assert f"s.{key}" in name_addr.join_predicate_sql
        assert f"s.{key}" in name_addr.guard_predicate_sql
        assert f"r.{key}" in name_addr.join_predicate_sql


def test_external_id_column_is_provider_specific_not_hardcoded() -> None:
    """A different provider configuring a different external_id_column
    (e.g. 'ccri_id' instead of RIDE's 'sasid') must change the generated
    reference-side column with zero code change — proves this isn't
    RIDE-specific logic (same genericness bar as
    test_land_sql.py's different-provider-shape test)."""
    app_config = load_config(CONFIG_DIR)
    fragments = build_sql_fragments(app_config.global_config.matching.matchers, "ccri_id")
    external_id = next(f for f in fragments if f.name == "deterministic_external_id")

    assert "s.rilds_id" in external_id.join_predicate_sql
    assert "r.ccri_id" in external_id.join_predicate_sql
    assert "r.rilds_id" not in external_id.join_predicate_sql
    assert "r.sasid" not in external_id.join_predicate_sql
