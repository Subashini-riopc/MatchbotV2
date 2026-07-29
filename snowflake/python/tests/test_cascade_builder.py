"""Unit tests for cascade_builder.py.

Verifies the assembled cascade SQL has the structural properties required
for correctness — one UNION ALL branch per fragment in priority order, the
ROW_NUMBER() partition/order clause, and the run_id parameter threaded into
every branch (so a cascade query for one pipeline_run_id never scans
another run's staged rows). Does not execute against a live warehouse — see
docs/snowflake-implementation-plan.md build step 4 for that validation.
"""

from __future__ import annotations

import pytest

from matchbot_snowflake.cascade_builder import build_cascade_sql, build_writeback_sql
from matchbot_snowflake.matcher_registry import MatcherSqlFragment


def _fixture_fragments() -> list[MatcherSqlFragment]:
    """A small, hand-built fixture mirroring the real chain's shape (not
    the real config) so this test doesn't depend on config/global.yaml
    staying byte-for-byte the same — see test_matcher_registry.py for the
    tests that DO check against real config."""
    return [
        MatcherSqlFragment(
            name="deterministic_external_id",
            priority=1,
            join_predicate_sql='s."rilds_id" = r."rilds_id"',
            guard_predicate_sql='s."rilds_id" IS NOT NULL',
            method_label="EXACT_SASID",
        ),
        MatcherSqlFragment(
            name="deterministic_ssn",
            priority=2,
            join_predicate_sql='s."ssn" = r."ssn"',
            guard_predicate_sql='s."ssn" IS NOT NULL',
            method_label="EXACT",
        ),
    ]


def test_raises_on_empty_fragment_list() -> None:
    with pytest.raises(ValueError):
        build_cascade_sql([])


def test_cascade_has_one_union_branch_per_fragment() -> None:
    sql = build_cascade_sql(_fixture_fragments())
    assert sql.count("UNION ALL") == 1  # 2 fragments -> 1 UNION ALL joining them


def test_cascade_orders_branches_by_priority() -> None:
    sql = build_cascade_sql(_fixture_fragments())
    external_id_pos = sql.index("EXACT_SASID")
    ssn_pos = sql.index("'EXACT'")
    assert external_id_pos < ssn_pos, "higher-priority (lower number) matcher must appear first"


def test_cascade_uses_row_number_partitioned_by_stage_id() -> None:
    sql = build_cascade_sql(_fixture_fragments())
    assert "ROW_NUMBER() OVER (" in sql
    assert "PARTITION BY stage_id" in sql
    assert "ORDER BY priority ASC, idcol_id ASC" in sql
    assert "WHERE rn = 1" in sql


def test_run_id_param_threaded_into_every_branch() -> None:
    sql = build_cascade_sql(_fixture_fragments(), run_id_param=":my_run_id")
    assert sql.count("s.pipeline_run_id = :my_run_id") == 2  # one per fragment


def test_each_fragment_guard_and_join_present_in_its_branch() -> None:
    sql = build_cascade_sql(_fixture_fragments())
    assert 's."rilds_id" = r."rilds_id"' in sql
    assert 's."rilds_id" IS NOT NULL' in sql
    assert 's."ssn" = r."ssn"' in sql
    assert 's."ssn" IS NOT NULL' in sql


def _fuzzy_fixture_fragment() -> MatcherSqlFragment:
    return MatcherSqlFragment(
        name="fuzzy_name_exact_addr",
        priority=3,
        join_predicate_sql='s."address1_std" = r."address1_std"',
        guard_predicate_sql='s."address1_std" IS NOT NULL',
        method_label="FUZZY",
        score_sql='MATCHBOT_JARO_WINKLER(s."first_name_std", r."first_name_std")',
        accept_threshold=0.8,
        review_threshold=0.6,
    )


def test_each_branch_picks_best_scoring_candidate_per_stage_row() -> None:
    """The per-branch subquery must rank by score DESC (best candidate
    wins within a branch), not just pass every join hit through — a fuzzy
    join's candidates are NOT already reduced to one-per-stage-row the way
    a deterministic equi-join effectively is."""
    sql = build_cascade_sql([_fuzzy_fixture_fragment()])
    assert "ORDER BY score DESC, idcol_id ASC" in sql
    assert sql.count("ROW_NUMBER() OVER (") == 2  # per-branch best + cross-branch priority winner


def test_branch_filters_out_scores_below_review_threshold() -> None:
    """A fuzzy candidate scoring below its own review_threshold must never
    reach the cross-branch ranking — it isn't a real candidate, just
    something the narrowing join happened to surface."""
    sql = build_cascade_sql([_fuzzy_fixture_fragment()])
    assert "WHERE score >= review_threshold" in sql


def test_cascade_output_includes_score_and_is_confirmed() -> None:
    sql = build_cascade_sql(_fixture_fragments())
    assert "(score >= accept_threshold) AS is_confirmed" in sql
    assert sql.strip().endswith(
        "SELECT stage_id, idcol_id, method, score, is_confirmed\nFROM ranked\nWHERE rn = 1"
    )


def test_deterministic_fragment_score_defaults_to_certain_accept() -> None:
    """A deterministic MatcherSqlFragment built without explicit score/
    threshold args must default to the always-1.0/always-exact behavior —
    every join hit is score 1.0, which always clears accept_threshold=1.0,
    so it's always routed to RILDS_MATCHED, never RILDS_ERROR
    (LOW_CONFIDENCE only exists for real fuzzy scores below 1.0)."""
    fragment = _fixture_fragments()[0]
    assert fragment.score_sql == "1.0"
    assert fragment.accept_threshold == 1.0
    assert fragment.review_threshold == 1.0


def test_writeback_sql_covers_matched_low_confidence_unmatched_and_error_paths() -> None:
    statements = build_writeback_sql()
    assert set(statements) == {
        "update_stage_matched",
        "update_stage_low_confidence",
        "update_stage_unmatched",
        "insert_matched",
        "insert_low_confidence",
        "insert_error",
    }
    # Every statement must scope to a single run — never touch other runs'
    # staged rows.
    for sql in statements.values():
        assert ":run_id" in sql


def test_matched_and_low_confidence_writeback_split_on_is_confirmed() -> None:
    statements = build_writeback_sql()
    assert "w.is_confirmed" in statements["update_stage_matched"]
    assert "NOT w.is_confirmed" in statements["update_stage_low_confidence"]
    assert "w.is_confirmed" in statements["insert_matched"]
    assert "NOT w.is_confirmed" in statements["insert_low_confidence"]
    assert "LOW_CONFIDENCE" in statements["insert_low_confidence"]
    assert "'NO_MATCH'" in statements["insert_error"]
