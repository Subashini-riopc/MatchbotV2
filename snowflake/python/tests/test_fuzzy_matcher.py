"""Unit tests for matchers/fuzzy.py.

Verifies the SQL fragments generated for fuzzy MatcherSpecs match
FuzzyMatcher's Python semantics (matching/fuzzy.py) — weighted-fraction
scoring, exact-anchor join selection, and NULL-safe similarity. A
regression fixture for the riskiest new piece of the matcher expansion
(genuinely new code, unlike the deterministic side which reuses an
already-generic existing generator). See
docs/snowflake-implementation-plan.md's matching-logic discussion.
"""

from __future__ import annotations

from pathlib import Path

from matchbot.config.loader import load_config

from matchbot_snowflake.config_models import FieldComparison, MatcherSpec
from matchbot_snowflake.matcher_registry import build_sql_fragments
from matchbot_snowflake.matchers.fuzzy import build_fuzzy_fragment

CONFIG_DIR = Path(__file__).resolve().parents[3] / "config"


def _spec(name: str, comparisons: list[FieldComparison], **kwargs) -> MatcherSpec:
    return MatcherSpec(name=name, type="fuzzy", comparisons=comparisons, **kwargs)


def test_exact_anchor_becomes_the_join_predicate() -> None:
    """A comparison configured as method='exact', threshold=1.0 must become
    the actual SQL JOIN condition — the cheap, selective narrowing filter
    this module's docstring describes, not just another scored comparison."""
    spec = _spec(
        "fuzzy_name_exact_addr",
        [
            FieldComparison(attribute="first_name_std", method="jaro_winkler", weight=45, threshold=0.8),
            FieldComparison(attribute="address1_std", method="exact", weight=100, threshold=1.0),
        ],
    )
    fragment = build_fuzzy_fragment(spec, external_id_column="sasid")
    assert "s.address1_std" in fragment.join_predicate_sql
    assert "r.address1_std" in fragment.join_predicate_sql
    # The non-anchor comparison must NOT leak into the join — only scored.
    assert "first_name_std" not in fragment.join_predicate_sql


def test_first_exact_comparison_wins_when_multiple_are_exact() -> None:
    spec = _spec(
        "multi_anchor",
        [
            FieldComparison(attribute="zip5", method="exact", weight=15, threshold=1.0),
            FieldComparison(attribute="ssn4", method="exact", weight=20, threshold=1.0),
        ],
    )
    fragment = build_fuzzy_fragment(spec, external_id_column="sasid")
    assert "s.zip5" in fragment.join_predicate_sql
    assert "zip5" in fragment.guard_predicate_sql


def test_no_exact_comparison_falls_back_to_blocking_key() -> None:
    spec = _spec(
        "all_fuzzy",
        [
            FieldComparison(attribute="first_name_std", method="jaro_winkler", weight=50, threshold=0.8),
            FieldComparison(attribute="address1_std", method="jaro_winkler", weight=50, threshold=0.8),
        ],
    )
    fragment = build_fuzzy_fragment(spec, external_id_column="sasid")
    assert "last_name_metaphone1" in fragment.join_predicate_sql


def test_score_sql_is_weighted_fraction_of_agreeing_comparisons() -> None:
    """Mirrors FuzzyMatcher._score(): sum(weight where similarity >=
    threshold) / total_weight — every comparison contributes its weight
    once, gated by its own IFF(... >= threshold, weight, 0.0) term, divided
    by the sum of ALL weights (not just the anchor's)."""
    spec = _spec(
        "weighted",
        [
            FieldComparison(attribute="first_name_std", method="jaro_winkler", weight=30, threshold=0.8),
            FieldComparison(attribute="address1_std", method="exact", weight=70, threshold=1.0),
        ],
    )
    fragment = build_fuzzy_fragment(spec, external_id_column="sasid")
    assert "/ 100.0)" in fragment.score_sql  # total_weight = 30 + 70
    assert "IFF(" in fragment.score_sql
    assert "MATCHBOT_JARO_WINKLER" in fragment.score_sql


def test_jaro_winkler_comparison_calls_the_registered_udf() -> None:
    spec = _spec(
        "jw",
        [FieldComparison(attribute="first_name_std", method="jaro_winkler", weight=50, threshold=0.8)],
    )
    fragment = build_fuzzy_fragment(spec, external_id_column="sasid")
    assert "MATCHBOT_JARO_WINKLER(" in fragment.score_sql


def test_null_on_either_side_scores_zero_not_null() -> None:
    """Mirrors _similarity()'s 'a is None or b is None -> 0.0' — a missing
    value must produce a real 0.0 score contribution (via IFF's NULL
    guard), never let a NULL comparison propagate into the SUM and corrupt
    every other comparison's contribution."""
    spec = _spec(
        "nullsafe",
        [FieldComparison(attribute="first_name_std", method="jaro_winkler", weight=50, threshold=0.8)],
    )
    fragment = build_fuzzy_fragment(spec, external_id_column="sasid")
    assert "IS NULL OR" in fragment.score_sql
    assert "0.0," in fragment.score_sql


def test_thresholds_carried_through_from_spec() -> None:
    spec = _spec(
        "thresholds",
        [FieldComparison(attribute="first_name_std", method="jaro_winkler", weight=50, threshold=0.8)],
        accept_threshold=0.75,
        review_threshold=0.55,
    )
    fragment = build_fuzzy_fragment(spec, external_id_column="sasid")
    assert fragment.accept_threshold == 0.75
    assert fragment.review_threshold == 0.55


def test_empty_comparisons_never_matches() -> None:
    spec = _spec("empty", [])
    fragment = build_fuzzy_fragment(spec, external_id_column="sasid")
    assert fragment.join_predicate_sql == "1 = 0"
    assert fragment.score_sql == "0.0"


def test_unsupported_method_raises() -> None:
    import pytest

    spec = _spec(
        "unsupported",
        [FieldComparison(attribute="last_name_std", method="levenshtein", weight=50, threshold=0.8)],
    )
    with pytest.raises(ValueError, match="levenshtein"):
        build_fuzzy_fragment(spec, external_id_column="sasid")


def test_real_config_fuzzy_matchers_all_build_without_error() -> None:
    """Every fuzzy matcher actually declared in config/global.yaml must
    build a valid fragment end to end — the integration-level counterpart
    to this file's unit tests above."""
    app_config = load_config(CONFIG_DIR)
    fragments = build_sql_fragments(app_config.global_config.matching.matchers, "sasid")
    fuzzy_fragments = [f for f in fragments if f.method_label == "FUZZY"]
    assert len(fuzzy_fragments) == 3
    for fragment in fuzzy_fragments:
        assert fragment.score_sql not in ("1.0", "0.0")
        assert 0.0 < fragment.accept_threshold <= 1.0
        assert 0.0 < fragment.review_threshold <= fragment.accept_threshold
