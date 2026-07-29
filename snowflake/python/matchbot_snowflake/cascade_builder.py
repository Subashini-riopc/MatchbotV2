"""Assembles the ordered matcher fragments into one set-based cascade query.

Implements "first matcher to accept wins" (matching/match.py's MatchStage —
"The first matcher to reach a terminal decision ... wins") as a single SQL
statement instead of a per-row Python loop: each matcher contributes one
UNION ALL branch (its join, restricted by its own guard and by
pipeline_run_id), then ROW_NUMBER() OVER (PARTITION BY stage_id ORDER BY
priority) picks each staged row's highest-priority match across all
branches at once.

See docs/snowflake-implementation-plan.md's "Cascade query shape" section
for the design rationale, including why blocking is implicit in the
equi-joins for this exact-match-only demo (no separate blocking-index step
like matching/blocking.py's Python path).

Deterministic vs. fuzzy branches, and the two-stage ranking this requires:
a deterministic fragment's join is exact-equality — every row it produces
IS a match (score is always exactly 1.0, see matcher_registry.py's
_BuiltFragment default). A fuzzy fragment's join (see matchers/fuzzy.py) is
only a cheap NARROWING filter — a candidate it produces might still score
anywhere from 0 to 1, including below that matcher's own review_threshold,
in which case it isn't a real candidate at all and must never win the
cross-branch priority race. That needs two ranking passes, not one:

1. WITHIN each branch, pick the single best-scoring reference candidate per
   stage row (mirrors FuzzyMatcher.match()'s ``if s > best_score: best_score,
   best = s, cand`` — the Python matcher already only ever considers its
   own single best candidate, never multiple candidates from one matcher),
   and drop it entirely if that best score is below the branch's own
   review_threshold (deterministic branches: threshold is 1.0 and score is
   always exactly 1.0 for any join hit, so this is a no-op filter for them).
2. ACROSS branches, rank each stage row's surviving per-branch winners by
   priority (lower wins) as before, then classify the overall winner as
   MATCHED (score >= that branch's accept_threshold) or LOW_CONFIDENCE
   (score >= review_threshold but < accept_threshold) — the caller
   (run_pipeline.py) routes those two outcomes to RILDS_MATCHED vs.
   RILDS_ERROR(decision=LOW_CONFIDENCE) respectively.
"""

from __future__ import annotations

from matchbot_snowflake.matcher_registry import MatcherSqlFragment


def _branch_sql(fragment: MatcherSqlFragment, *, run_id_param: str) -> str:
    """One fragment's raw (stage_id, idcol_id, score) candidate rows —
    NOT yet reduced to one row per stage_id; see _branch_best_sql(), which
    wraps this to pick each stage row's single best-scoring candidate
    within this one branch, mirroring FuzzyMatcher.match()'s per-matcher
    best-candidate selection."""
    return f"""    SELECT
        s.id AS stage_id,
        r.idcol_id AS idcol_id,
        {fragment.priority} AS priority,
        '{fragment.method_label}' AS method,
        ({fragment.score_sql}) AS score,
        {fragment.accept_threshold} AS accept_threshold,
        {fragment.review_threshold} AS review_threshold
    FROM RILDS_STAGE s
    JOIN RILDS_REFERENCE r ON {fragment.join_predicate_sql}
    WHERE {fragment.guard_predicate_sql}
      AND s.pipeline_run_id = {run_id_param}"""


def _branch_best_sql(fragment: MatcherSqlFragment, *, run_id_param: str) -> str:
    """This branch's raw candidates, reduced to one row per stage_id (the
    highest-scoring candidate, ties broken by idcol_id for reproducibility
    — same deterministic-tiebreak reasoning as the cross-branch ranking
    below), filtered to only scores clearing this branch's own
    review_threshold. A deterministic branch's score is always exactly 1.0
    and review_threshold is always 1.0, so this filter is a no-op for
    those — every join hit already clears it.
    """
    raw = _branch_sql(fragment, run_id_param=run_id_param)
    return f"""    SELECT stage_id, idcol_id, priority, method, score, accept_threshold
    FROM (
        SELECT
            stage_id, idcol_id, priority, method, score, accept_threshold,
            ROW_NUMBER() OVER (
                PARTITION BY stage_id
                ORDER BY score DESC, idcol_id ASC
            ) AS best_rn
        FROM (
{raw}
        )
        WHERE score >= review_threshold
    )
    WHERE best_rn = 1"""


def build_cascade_sql(
    fragments: list[MatcherSqlFragment],
    *,
    run_id_param: str = ":run_id",
) -> str:
    """Render the full WITH ... SELECT cascade query for one pipeline run.

    The tie-break on idcol_id in both ranking passes' ORDER BY guarantees a
    single, reproducible winner even when multiple candidates tie on score
    (always true for deterministic branches, since every hit scores exactly
    1.0) — mirrors DeterministicMatcher.match()'s implicit
    first-candidate-wins behavior with a deterministic substitute, since
    "first" has no natural meaning in a declarative set operation.

    Returns (stage_id, idcol_id, method, score, is_confirmed) winners —
    is_confirmed is TRUE when score >= that branch's accept_threshold
    (route to RILDS_MATCHED) and FALSE when it only cleared review_threshold
    (route to RILDS_ERROR as LOW_CONFIDENCE) — the caller
    (procedures/run_pipeline.py) is responsible for the subsequent
    UPDATE RILDS_STAGE / INSERT INTO RILDS_MATCHED / INSERT INTO RILDS_ERROR
    statements that consume this result.
    """
    if not fragments:
        raise ValueError("build_cascade_sql requires at least one matcher fragment")

    branches_sql = "\n    UNION ALL\n".join(
        _branch_best_sql(f, run_id_param=run_id_param) for f in fragments
    )

    return f"""WITH branch_winners AS (
{branches_sql}
),
ranked AS (
    SELECT
        stage_id,
        idcol_id,
        priority,
        method,
        score,
        (score >= accept_threshold) AS is_confirmed,
        ROW_NUMBER() OVER (
            PARTITION BY stage_id
            ORDER BY priority ASC, idcol_id ASC
        ) AS rn
    FROM branch_winners
)
SELECT stage_id, idcol_id, method, score, is_confirmed
FROM ranked
WHERE rn = 1"""


def build_writeback_sql(*, run_id_param: str = ":run_id") -> dict[str, str]:
    """SQL for the set-based writes that consume build_cascade_sql's
    winners (materialized upstream as a temp table/CTE named WINNERS by
    the caller — see procedures/run_pipeline.py — with columns stage_id,
    idcol_id, method, score, is_confirmed per build_cascade_sql's SELECT).

    Three outcomes per staged row, mirroring FuzzyMatcher.match()'s
    MATCHED/AMBIGUOUS/UNMATCHED trichotomy (matching/fuzzy.py) — a
    deterministic-only winner is always is_confirmed=TRUE (see
    matcher_registry.py's _BuiltFragment default score/thresholds, which
    make a deterministic branch's score always exactly 1.0 >= its 1.0
    accept_threshold):

    1. is_confirmed (score >= that branch's accept_threshold) -> RILDS_MATCHED.
    2. WINNERS row present but NOT confirmed (score >= review_threshold but
       < accept_threshold — only reachable via a fuzzy branch) ->
       RILDS_ERROR, decision='LOW_CONFIDENCE', real match_score, reason
       naming which matcher and score. Flagged for manual review, not
       auto-matched (see matching/fuzzy.py's module docstring: "AMBIGUOUS is
       a routing label, not a blocking pause").
    3. No WINNERS row at all (every branch's guard/join/review_threshold
       filter excluded this stage row) -> RILDS_ERROR, decision='NO_MATCH',
       0.0 score — same meaning as before this matcher expansion.

    Returns a dict of statement-name -> SQL so the caller can execute each
    in the right order and log/inspect them individually.
    """
    identity_cols = (
        "first_name, middle_name, last_name, birth_date, gender, "
        "first_name_std, last_name_std, first_name_metaphone1, last_name_metaphone1, "
        "last_name8, birth_year, birth_month, birth_day, rilds_id, lasid, "
        "ssn, address1, address2, city, state, zip"
    )
    identity_cols_s = ", ".join(f"s.{c.strip()}" for c in identity_cols.split(","))

    return {
        "update_stage_matched": f"""
UPDATE RILDS_STAGE s
SET idcol_id = w.idcol_id,
    match_score = w.score,
    match_status = 'MATCHED'
FROM WINNERS w
WHERE s.id = w.stage_id
  AND w.is_confirmed
  AND s.pipeline_run_id = {run_id_param}
""".strip(),
        "update_stage_low_confidence": f"""
UPDATE RILDS_STAGE s
SET match_score = w.score,
    match_status = 'LOW_CONFIDENCE'
FROM WINNERS w
WHERE s.id = w.stage_id
  AND NOT w.is_confirmed
  AND s.pipeline_run_id = {run_id_param}
""".strip(),
        "update_stage_unmatched": f"""
UPDATE RILDS_STAGE s
SET match_score = 0.0,
    match_status = 'NO_MATCH'
WHERE s.pipeline_run_id = {run_id_param}
  AND s.id NOT IN (SELECT stage_id FROM WINNERS)
""".strip(),
        "insert_matched": f"""
INSERT INTO RILDS_MATCHED (
    pipeline_run_id, stage_id, idcol_id, match_score, match_method,
    {identity_cols}
)
SELECT
    s.pipeline_run_id, s.id, w.idcol_id, w.score, w.method,
    {identity_cols_s}
FROM RILDS_STAGE s
JOIN WINNERS w ON s.id = w.stage_id
WHERE w.is_confirmed
  AND s.pipeline_run_id = {run_id_param}
""".strip(),
        "insert_low_confidence": f"""
INSERT INTO RILDS_ERROR (
    pipeline_run_id, stage_id, decision, match_score, reason,
    {identity_cols}
)
SELECT
    s.pipeline_run_id, s.id, 'LOW_CONFIDENCE', w.score,
    w.method || ': candidate idcol_id=' || w.idcol_id || ' score=' || w.score,
    {identity_cols_s}
FROM RILDS_STAGE s
JOIN WINNERS w ON s.id = w.stage_id
WHERE NOT w.is_confirmed
  AND s.pipeline_run_id = {run_id_param}
""".strip(),
        "insert_error": f"""
INSERT INTO RILDS_ERROR (
    pipeline_run_id, stage_id, decision, match_score, reason,
    {identity_cols}
)
SELECT
    s.pipeline_run_id, s.id, 'NO_MATCH', 0.0, 'no candidate matched',
    {identity_cols_s}
FROM RILDS_STAGE s
WHERE s.pipeline_run_id = {run_id_param}
  AND s.id NOT IN (SELECT stage_id FROM WINNERS)
""".strip(),
    }
