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


def test_real_chain_produces_eight_fragments_in_order() -> None:
    """The current 6-rule deterministic chain (external_id, ssn, name_ssn4,
    name_dob, name_addr_city_state, name_addr_zip) plus 2 fuzzy tiers.
    deterministic_name_addr_full and deterministic_name_addr were dropped
    entirely — not part of the requested rule set.
    deterministic_lastname_addr_state_zip and
    deterministic_firstname_addr_state_zip (each dropping one of the two
    name fields, anchored only by state+zip) were also removed per
    explicit request — both carried real household-collision risk (people
    sharing a last name + address, or a first name + address, could
    false-match). Rule 6 (deterministic_name_state_zip: first_name_std,
    last_name_std, state, zip5) was then changed to
    deterministic_name_addr_zip (state swapped for address1_std).
    fuzzy_name_addr_combined (nothing held fully exact — six weak/partial
    signals corroborating each other) was removed per explicit request —
    only 2 fuzzy rules remain. Their order was then swapped per explicit
    request: fuzzy_exact_name_addr now runs before fuzzy_name_exact_addr."""
    app_config = load_config(CONFIG_DIR)
    fragments = build_sql_fragments(app_config.global_config.matching.matchers, "sasid")

    assert [f.name for f in fragments] == [
        "deterministic_external_id",
        "deterministic_ssn",
        "deterministic_name_ssn4",
        "deterministic_name_dob",
        "deterministic_name_addr_city_state",
        "deterministic_name_addr_zip",
        "fuzzy_exact_name_addr",
        "fuzzy_name_exact_addr",
    ]
    assert [f.priority for f in fragments] == list(range(1, 9))


def test_external_id_matcher_reports_its_own_name() -> None:
    """method_label is the matcher's own config/global.yaml name verbatim
    (not a coarse EXACT_SASID bucket) — see matchers/deterministic.py's
    _method_label."""
    app_config = load_config(CONFIG_DIR)
    fragments = build_sql_fragments(app_config.global_config.matching.matchers, "sasid")
    external_id = next(f for f in fragments if f.name == "deterministic_external_id")

    assert external_id.method_label == "deterministic_external_id"
    assert "s.rilds_id" in external_id.join_predicate_sql
    # Reference side has no rilds_id column — RIDE's configured
    # external_id_column ("sasid") is what it's actually compared against.
    assert "r.sasid" in external_id.join_predicate_sql
    assert "r.rilds_id" not in external_id.join_predicate_sql


def test_other_deterministic_matchers_report_their_own_names() -> None:
    app_config = load_config(CONFIG_DIR)
    fragments = build_sql_fragments(app_config.global_config.matching.matchers, "sasid")

    for name in (
        "deterministic_ssn",
        "deterministic_name_ssn4",
        "deterministic_name_dob",
        "deterministic_name_addr_city_state",
        "deterministic_name_addr_zip",
    ):
        fragment = next(f for f in fragments if f.name == name)
        assert fragment.method_label == name


def test_fuzzy_matchers_report_their_own_names_and_real_thresholds() -> None:
    """fuzzy_name_exact_addr's thresholds were raised (0.85/0.65 -> 0.90/
    0.75) when its scoring was reworked to be name-only (address1_std/
    zip5 dropped to weight 0 — see config/global.yaml's comment there)."""
    app_config = load_config(CONFIG_DIR)
    fragments = build_sql_fragments(app_config.global_config.matching.matchers, "sasid")

    for name, expected_accept, expected_review in (
        ("fuzzy_name_exact_addr", 0.90, 0.75),
        ("fuzzy_exact_name_addr", 0.85, 0.65),
    ):
        fragment = next(f for f in fragments if f.name == name)
        assert fragment.method_label == name
        assert fragment.accept_threshold == expected_accept
        assert fragment.review_threshold == expected_review
        assert fragment.score_sql != "1.0"  # a real weighted-score expression, not the deterministic default


def test_name_dob_uses_hash_based_join() -> None:
    """deterministic_name_dob's key list (first_name_std, last_name_std,
    birth_date) is registered in derive_sql.py's HASH_COLUMN_BY_KEYS, so
    its join collapses to a single name_dob_hash equality check instead of
    a 3-column AND chain — see matchers/deterministic.py's
    hash_column_for_keys() branch. The hash computation itself (in
    provider_sql.py via derive_sql.py's deterministic_hash_sql) is what
    enforces birth_date comparing natively (not cast through VARCHAR) plus
    case/whitespace normalization for the two name keys — see
    test_derive_sql.py's hash-formula tests for that."""
    app_config = load_config(CONFIG_DIR)
    fragments = build_sql_fragments(app_config.global_config.matching.matchers, "sasid")
    name_dob = next(f for f in fragments if f.name == "deterministic_name_dob")

    assert name_dob.join_predicate_sql == "s.name_dob_hash = r.name_dob_hash"
    assert name_dob.guard_predicate_sql == "s.name_dob_hash IS NOT NULL"


def test_name_addr_zip_uses_hash_based_join() -> None:
    """deterministic_name_addr_zip's key list (first_name_std,
    last_name_std, address1_std, zip5) is registered in
    HASH_COLUMN_BY_KEYS -> single-column hash join, same reasoning as
    test_name_dob_uses_hash_based_join."""
    app_config = load_config(CONFIG_DIR)
    fragments = build_sql_fragments(app_config.global_config.matching.matchers, "sasid")
    name_addr_zip = next(f for f in fragments if f.name == "deterministic_name_addr_zip")

    assert name_addr_zip.join_predicate_sql == "s.name_addr_zip_hash = r.name_addr_zip_hash"
    assert name_addr_zip.guard_predicate_sql == "s.name_addr_zip_hash IS NOT NULL"


def test_single_key_matchers_still_use_plain_column_comparison() -> None:
    """deterministic_ssn (1 key) has no hash column benefit and is not in
    HASH_COLUMN_BY_KEYS — must still emit a direct column comparison, not
    attempt a (nonexistent) hash column."""
    app_config = load_config(CONFIG_DIR)
    fragments = build_sql_fragments(app_config.global_config.matching.matchers, "sasid")
    ssn = next(f for f in fragments if f.name == "deterministic_ssn")

    assert "hash" not in ssn.join_predicate_sql.lower()
    assert "s.ssn" in ssn.join_predicate_sql
    assert "r.ssn" in ssn.join_predicate_sql


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
