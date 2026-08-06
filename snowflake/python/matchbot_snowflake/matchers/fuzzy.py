"""SQL fragment generator for MatcherSpec.type == "fuzzy".

Reproduces matching/fuzzy.py::FuzzyMatcher's exact scoring semantics as SQL:
for each configured comparison, compute a per-field similarity in [0, 1];
if that similarity clears the comparison's own threshold, its full weight
counts toward the score; the final score is (sum of counted weight) /
(total weight across all comparisons) — see FuzzyMatcher._score(). A
missing value on either side scores 0.0 for that comparison (not skipped —
see _similarity()'s "a is None or b is None -> 0.0"), so it simply
contributes nothing rather than disqualifying the whole matcher, unlike a
deterministic matcher's all-or-nothing guard.

Unlike a deterministic matcher, nothing here is required to be exactly
equal for a stage/reference pair to be considered at all — a fuzzy matcher
scores similarity, it doesn't gate on equality. That makes the SQL join
itself a real design problem: comparing every staged row against all ~400K
reference rows with no filter (a true cross join) is prohibitively
expensive at real data volumes. Two narrowing strategies, chosen per
matcher based on its configured comparisons:

1. Exact-anchored (the common case — any matcher with at least one
   `exact`/threshold=1.0 comparison, e.g. fuzzy_name_exact_addr's
   address1_std+zip5, or fuzzy_exact_name_addr's first_name_std+
   last_name_std): the FIRST such comparison's normalized equality becomes the actual SQL
   JOIN condition (cheap, selective, same shape as a deterministic
   matcher's join) — see _find_exact_anchor(). The score is then computed
   over ALL configured comparisons (including the anchor field, which
   necessarily contributes its full weight for every candidate the join
   already selected, since the join already required it to match).

2. Blocking-narrowed (only if a matcher has NO exact/threshold=1.0
   comparison at all — no matcher in the current config/global.yaml chain
   hits this path today, since every fuzzy rule includes at least one exact
   corroborating field): falls back to a cheap phonetic filter
   (last_name_metaphone1 equality), mirroring the last_name_only blocking
   key already used elsewhere in config/global.yaml's blocking_keys —
   narrows candidates to "plausibly the same last name" before scoring the
   full weighted comparison set, rather than a true unrestricted cross
   join. Kept as a fallback for any future all-fuzzy matcher, not exercised
   by today's two fuzzy rules.

Both strategies are approximations of "the true candidate set a
from-scratch blocking implementation would produce" — see
docs/snowflake-implementation-plan.md's blocking note (this demo's
deterministic matchers already accept implicit equi-join blocking in lieu
of a separate blocking-index step; the same tradeoff applies here, just
with a phonetic filter instead of an exact one for the no-anchor case).

required comparisons (FieldComparison.required): a hard precondition,
independent of weight — see matching/fuzzy.py's module docstring for the
full rationale (weight=0 does NOT mean "not required"; it just means the
field doesn't move the score, but a candidate can still match via other
fields even if a weight=0 field disagrees entirely). Every required
comparison (not just whichever one _find_exact_anchor() happens to pick
as the join anchor) is folded into join_predicate_sql as an extra AND
condition — a required field the anchor detector didn't already select is
appended as its own equality/similarity-threshold check, so a candidate
failing ANY required field is excluded at the JOIN itself, never reaching
scoring at all. This mirrors matching/fuzzy.py's _passes_required() check
exactly, just enforced via SQL instead of a Python loop.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from matchbot_snowflake.matcher_registry import _BuiltFragment, register_sql_matcher

if TYPE_CHECKING:
    from matchbot_snowflake.config_models import FieldComparison, MatcherSpec

JARO_WINKLER_UDF_NAME = "MATCHBOT_JARO_WINKLER"

# Keys backed by a non-string column type — same set deterministic.py uses,
# duplicated rather than imported since it's a small, stable constant and
# importing across matcher-type modules would create an unnecessary
# coupling between two otherwise-independent generators.
_NON_STRING_KEYS = frozenset({"birth_date", "birth_year", "birth_month", "birth_day"})

# Fallback narrowing filter when a fuzzy matcher has no exact/threshold=1.0
# comparison to anchor the join on — mirrors global.yaml's last_name_only
# blocking key (broad phonetic net on last name alone).
_BLOCKING_FALLBACK_KEY = "last_name_metaphone1"


def _col(alias: str, key: str) -> str:
    """An alias-qualified column reference — see deterministic.py's _col()
    for why this must stay unquoted (Snowflake's default identifier
    case-folding)."""
    return f"{alias}.{key}"


def _norm_sql(column_ref: str, key: str) -> str:
    """Mirror fuzzy.py::_norm(): uppercase+trim for string-typed keys,
    direct comparison for date/numeric-typed keys — same rule as
    deterministic.py's _norm_sql()."""
    if key in _NON_STRING_KEYS:
        return column_ref
    return f"TRIM(UPPER({column_ref}::VARCHAR))"


def _similarity_sql(comparison: "FieldComparison") -> str:
    """SQL expression computing this comparison's similarity in [0, 1] for
    one (s, r) row pair — mirrors fuzzy.py::_similarity(). NULL on either
    side scores 0.0 (via COALESCE), never NULL propagation into the sum.
    """
    key = comparison.attribute
    s_ref = _col("s", key)
    r_ref = _col("r", key)
    s_norm = _norm_sql(s_ref, key)
    r_norm = _norm_sql(r_ref, key)
    missing_check = f"{s_ref} IS NULL OR {r_ref} IS NULL"

    if comparison.method == "exact":
        return f"IFF({missing_check}, 0.0, IFF({s_norm} = {r_norm}, 1.0, 0.0))"
    if comparison.method == "jaro_winkler":
        return f"IFF({missing_check}, 0.0, {JARO_WINKLER_UDF_NAME}({s_norm}, {r_norm}))"
    raise ValueError(
        f"Snowflake fuzzy matcher does not support comparison method "
        f"{comparison.method!r} (attribute {key!r}) — only 'exact' and "
        f"'jaro_winkler' are implemented (see fuzzy.py::_similarity_sql). "
        f"'levenshtein'/'metaphone' would need a dedicated UDF, same as "
        f"MATCHBOT_JARO_WINKLER, before they can be added here."
    )


def _score_sql(comparisons: list["FieldComparison"]) -> str:
    """The full weighted-fraction score expression — mirrors
    FuzzyMatcher._score(): sum(weight for comparisons whose similarity
    clears their own threshold) / total_weight."""
    total_weight = sum(c.weight for c in comparisons) or 1.0
    weighted_terms = " + ".join(
        f"IFF({_similarity_sql(c)} >= {c.threshold}, {c.weight}, 0.0)"
        for c in comparisons
    )
    return f"(({weighted_terms}) / {total_weight})"


def _find_exact_anchor(comparisons: list["FieldComparison"]) -> "FieldComparison | None":
    """The first comparison configured as exact/threshold=1.0, if any — see
    this module's docstring for why that comparison (not a separate
    blocking mechanism) becomes the SQL join's narrowing condition."""
    for c in comparisons:
        if c.method == "exact" and c.threshold >= 1.0:
            return c
    return None


def _required_condition_sql(comparison: "FieldComparison") -> str:
    """A single required comparison's hard-gate condition, suitable for
    AND-ing into join_predicate_sql — mirrors matching/fuzzy.py's
    _passes_required(): the field's similarity must clear its own
    threshold, treating a missing value on either side as failing (never
    NULL-propagating into a silently-true condition)."""
    return f"{_similarity_sql(comparison)} >= {comparison.threshold}"


@register_sql_matcher("fuzzy")
def build_fuzzy_fragment(spec: "MatcherSpec", external_id_column: str) -> _BuiltFragment:
    """Return a _BuiltFragment for one fuzzy MatcherSpec.

    ``external_id_column`` is accepted for signature parity with
    build_deterministic_fragment (both builders are called identically by
    build_sql_fragments) but unused here — no fuzzy matcher in scope
    compares on rilds_id, which only makes sense as an exact match.
    """
    del external_id_column  # unused — see docstring

    if not spec.comparisons:
        # A fuzzy matcher with no comparisons can never score above 0 —
        # same "never matches" case as a keyless deterministic matcher.
        return _BuiltFragment(
            "1 = 0", "1 = 0", spec.name,
            score_sql="0.0",
            accept_threshold=spec.accept_threshold,
            review_threshold=spec.review_threshold,
        )

    anchor = _find_exact_anchor(spec.comparisons)
    if anchor is not None:
        join_predicate = (
            f"{_norm_sql(_col('s', anchor.attribute), anchor.attribute)} = "
            f"{_norm_sql(_col('r', anchor.attribute), anchor.attribute)}"
        )
        guard_predicate = (
            f"{_col('s', anchor.attribute)} IS NOT NULL AND "
            f"TRIM({_col('s', anchor.attribute)}::VARCHAR) != ''"
        )
    else:
        join_predicate = (
            f"{_norm_sql(_col('s', _BLOCKING_FALLBACK_KEY), _BLOCKING_FALLBACK_KEY)} = "
            f"{_norm_sql(_col('r', _BLOCKING_FALLBACK_KEY), _BLOCKING_FALLBACK_KEY)}"
        )
        guard_predicate = (
            f"{_col('s', _BLOCKING_FALLBACK_KEY)} IS NOT NULL AND "
            f"TRIM({_col('s', _BLOCKING_FALLBACK_KEY)}::VARCHAR) != ''"
        )

    # Fold every required=true comparison into the join as an extra AND
    # condition — except the one already selected as the anchor (its
    # equality is already the join_predicate; AND-ing it again would be a
    # redundant no-op, not a bug, but noise). See this module's docstring
    # for why this must be a real join condition, not just weight=0.
    for c in spec.comparisons:
        if c.required and c is not anchor:
            join_predicate = f"{join_predicate} AND {_required_condition_sql(c)}"

    return _BuiltFragment(
        join_predicate,
        guard_predicate,
        spec.name,
        score_sql=_score_sql(spec.comparisons),
        accept_threshold=spec.accept_threshold,
        review_threshold=spec.review_threshold,
    )
