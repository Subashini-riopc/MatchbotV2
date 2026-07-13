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
"""

from __future__ import annotations

from matchbot_snowflake.matcher_registry import MatcherSqlFragment


def _branch_sql(fragment: MatcherSqlFragment, *, run_id_param: str) -> str:
    return f"""    SELECT
        s.id AS stage_id,
        r.idcol_id AS idcol_id,
        {fragment.priority} AS priority,
        '{fragment.method_label}' AS method
    FROM RILDS_STAGE s
    JOIN RILDS_REFERENCE r ON {fragment.join_predicate_sql}
    WHERE {fragment.guard_predicate_sql}
      AND s.pipeline_run_id = {run_id_param}"""


def build_cascade_sql(
    fragments: list[MatcherSqlFragment],
    *,
    run_id_param: str = ":run_id",
) -> str:
    """Render the full WITH ... SELECT cascade query for one pipeline run.

    The tie-break on idcol_id in ROW_NUMBER()'s ORDER BY guarantees exactly
    one winner per stage row even if a priority level's join produces more
    than one candidate (e.g. two reference rows sharing the same SSN) — a
    scenario the Python DeterministicMatcher.match() resolves implicitly by
    taking the *first* candidate in its input list, which has no single
    natural analog in a declarative set operation; MIN(idcol_id) is a
    deliberate, deterministic substitute so results are reproducible run to
    run rather than depending on Snowflake's unspecified row order.

    Returns only the SELECT of (stage_id, idcol_id, method) winners — the
    caller (procedures/run_pipeline.py) is responsible for the subsequent
    UPDATE RILDS_STAGE / INSERT INTO RILDS_MATCHED / INSERT INTO RILDS_ERROR
    statements that consume this result.
    """
    if not fragments:
        raise ValueError("build_cascade_sql requires at least one matcher fragment")

    branches_sql = "\n    UNION ALL\n".join(_branch_sql(f, run_id_param=run_id_param) for f in fragments)

    return f"""WITH candidate_matches AS (
{branches_sql}
),
ranked AS (
    SELECT
        stage_id,
        idcol_id,
        priority,
        method,
        ROW_NUMBER() OVER (
            PARTITION BY stage_id
            ORDER BY priority ASC, idcol_id ASC
        ) AS rn
    FROM candidate_matches
)
SELECT stage_id, idcol_id, method
FROM ranked
WHERE rn = 1"""


def build_writeback_sql(*, run_id_param: str = ":run_id") -> dict[str, str]:
    """SQL for the set-based writes that consume build_cascade_sql's
    winners (materialized upstream as a temp table/CTE named WINNERS by
    the caller — see procedures/run_pipeline.py).

    Returns a dict of statement-name -> SQL so the caller can execute each
    in the right order and log/inspect them individually.
    """
    return {
        "update_stage_matched": f"""
UPDATE RILDS_STAGE s
SET idcol_id = w.idcol_id,
    match_score = 1.0,
    match_status = 'MATCHED'
FROM WINNERS w
WHERE s.id = w.stage_id
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
    first_name, middle_name, last_name, birth_date, gender,
    first_name_std, last_name_std, first_name_metaphone1, last_name_metaphone1,
    last_name8, birth_year, birth_month, birth_day, rilds_id, lasid,
    ssn, address1, address2, city, state, zip
)
SELECT
    s.pipeline_run_id, s.id, w.idcol_id, 1.0, w.method,
    s.first_name, s.middle_name, s.last_name, s.birth_date, s.gender,
    s.first_name_std, s.last_name_std, s.first_name_metaphone1, s.last_name_metaphone1,
    s.last_name8, s.birth_year, s.birth_month, s.birth_day, s.rilds_id, s.lasid,
    s.ssn, s.address1, s.address2, s.city, s.state, s.zip
FROM RILDS_STAGE s
JOIN WINNERS w ON s.id = w.stage_id
WHERE s.pipeline_run_id = {run_id_param}
""".strip(),
        "insert_error": f"""
INSERT INTO RILDS_ERROR (
    pipeline_run_id, stage_id, decision, match_score, reason,
    first_name, middle_name, last_name, birth_date, gender,
    first_name_std, last_name_std, first_name_metaphone1, last_name_metaphone1,
    last_name8, birth_year, birth_month, birth_day, rilds_id, lasid,
    ssn, address1, address2, city, state, zip
)
SELECT
    s.pipeline_run_id, s.id, 'NO_MATCH', 0.0, 'no candidate matched',
    s.first_name, s.middle_name, s.last_name, s.birth_date, s.gender,
    s.first_name_std, s.last_name_std, s.first_name_metaphone1, s.last_name_metaphone1,
    s.last_name8, s.birth_year, s.birth_month, s.birth_day, s.rilds_id, s.lasid,
    s.ssn, s.address1, s.address2, s.city, s.state, s.zip
FROM RILDS_STAGE s
WHERE s.pipeline_run_id = {run_id_param}
  AND s.id NOT IN (SELECT stage_id FROM WINNERS)
""".strip(),
    }
