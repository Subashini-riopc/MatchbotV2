"""Fuzzy matcher: weighted per-field similarity with accept/review thresholds.

Each comparison declares a method (exact / levenshtein / jaro_winkler /
metaphone), a weight, and a per-field agreement threshold — all from config.
The candidate's score is the weighted fraction of agreeing fields. The best
candidate is then routed:

* score >= accept_threshold  -> MATCHED
* score >= review_threshold  -> AMBIGUOUS (ERROR table, optional review)
* otherwise                  -> UNMATCHED

No human gate: AMBIGUOUS is a routing label, not a blocking pause.

A comparison marked ``required: true`` (FieldComparison.required) is a hard
precondition, independent of weight: a candidate that fails a required
comparison is disqualified entirely, BEFORE scoring — it never becomes
``best`` no matter how high its weighted score would otherwise be. This is
deliberately separate from weight=0 (which still lets a candidate score and
match via other fields; it just doesn't move the needle for that field) —
added because weight=0 was previously being relied on to mean "not
required," which it doesn't: e.g. a candidate in the wrong zip code/city
could still match purely on address1_std text similarity when zip5 carried
weight=0. See matchers/fuzzy.py (Snowflake SQL) for the equivalent guard.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rapidfuzz.distance import Levenshtein

from matchbot.config.models import FieldComparison, MatcherSpec, StandardizationConfig
from matchbot.domain.enums import MatchDecision, MatchMethod
from matchbot.matching import standardize as S
from matchbot.matching.base import NO_MATCH, MatchOutcome, register_matcher
from matchbot.matching.base import member_key as _member_key

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


def _similarity(method: str, a: Any, b: Any) -> float:
    """Per-field similarity in [0, 1]. Missing on either side -> 0."""
    if a is None or b is None or a == "" or b == "":
        return 0.0
    sa, sb = str(a), str(b)
    if method == "exact":
        return 1.0 if _norm(sa) == _norm(sb) else 0.0
    if method == "jaro_winkler":
        return S.jaro_winkler(_norm(sa), _norm(sb))
    if method == "levenshtein":
        return float(Levenshtein.normalized_similarity(_norm(sa), _norm(sb)))
    if method == "metaphone":
        ma, mb = S.metaphone(sa), S.metaphone(sb)
        return 1.0 if ma is not None and ma == mb else 0.0
    raise ValueError(f"Unknown comparison method: {method!r}")


def _norm(value: str) -> str:
    return value.strip().upper()


@register_matcher("fuzzy")
class FuzzyMatcher:
    """Weighted-similarity matcher with accept/review routing."""

    def __init__(self, spec: MatcherSpec, std_config: StandardizationConfig) -> None:
        self.name = spec.name
        self.comparisons: list[FieldComparison] = list(spec.comparisons)
        self.accept_threshold = spec.accept_threshold
        self.review_threshold = spec.review_threshold
        self._std = std_config
        self._total_weight = sum(c.weight for c in self.comparisons) or 1.0

    def _passes_required(self, record: Mapping[str, Any], cand: Mapping[str, Any]) -> bool:
        """True only if every required=true comparison clears its own
        threshold — checked BEFORE scoring, so a candidate that fails a
        required field is disqualified outright, never scored via its
        other (possibly high-weight) fields."""
        for c in self.comparisons:
            if not c.required:
                continue
            sim = _similarity(c.method, record.get(c.attribute), cand.get(c.attribute))
            if sim < c.threshold:
                return False
        return True

    def _score(self, record: Mapping[str, Any], cand: Mapping[str, Any]) -> float:
        agreeing = 0.0
        for c in self.comparisons:
            sim = _similarity(c.method, record.get(c.attribute), cand.get(c.attribute))
            if sim >= c.threshold:
                agreeing += c.weight
        return agreeing / self._total_weight

    def match(
        self,
        record: Mapping[str, Any],
        candidates: Sequence[Mapping[str, Any]],
    ) -> MatchOutcome:
        best_score = 0.0
        best: Mapping[str, Any] | None = None
        for cand in candidates:
            if not self._passes_required(record, cand):
                continue
            s = self._score(record, cand)
            if s > best_score:
                best_score, best = s, cand

        if best is None:
            return NO_MATCH

        idcol_id = _member_key(best)
        if best_score >= self.accept_threshold:
            return MatchOutcome(
                MatchDecision.MATCHED,
                MatchMethod.FUZZY,
                idcol_id,
                round(best_score, 4),
                f"{self.name}: fuzzy match score={best_score:.3f}",
            )
        if best_score >= self.review_threshold:
            return MatchOutcome(
                MatchDecision.AMBIGUOUS,
                MatchMethod.FUZZY,
                idcol_id,
                round(best_score, 4),
                f"{self.name}: needs review score={best_score:.3f}",
            )
        return NO_MATCH
