"""Unit tests for the fuzzy matcher's scoring and accept/review routing."""

from __future__ import annotations

from matchbot.config.models import FieldComparison, MatcherSpec, StandardizationConfig
from matchbot.domain.enums import MatchDecision
from matchbot.matching.fuzzy import FuzzyMatcher, _similarity

STD = StandardizationConfig()

SPEC = MatcherSpec(
    name="fuzzy_test",
    type="fuzzy",
    accept_threshold=0.80,
    review_threshold=0.50,
    comparisons=[
        FieldComparison(attribute="first_name", method="jaro_winkler", weight=1.0, threshold=0.90),
        FieldComparison(attribute="last_name", method="jaro_winkler", weight=1.0, threshold=0.90),
        FieldComparison(attribute="birth_date", method="exact", weight=2.0, threshold=1.0),
    ],
)


def test_similarity_methods() -> None:
    assert _similarity("exact", "ABC", "abc") == 1.0
    assert _similarity("exact", "ABC", "XYZ") == 0.0
    assert _similarity("jaro_winkler", "MARY", "MARI") > 0.8
    assert _similarity("levenshtein", "MARY", "MARY") == 1.0
    assert _similarity("metaphone", "Smith", "Smyth") == 1.0
    assert _similarity("exact", None, "x") == 0.0


def test_accept_when_strong() -> None:
    m = FuzzyMatcher(SPEC, STD)
    rec = {"first_name": "MARY", "last_name": "JONES", "birth_date": "1990-01-01"}
    cands = [
        {
            "member_id": "M9",
            "first_name": "MARY",
            "last_name": "JONES",
            "birth_date": "1990-01-01",
        }
    ]
    out = m.match(rec, cands)
    assert out.decision is MatchDecision.MATCHED
    assert out.idcol_id == "M9"
    assert out.score == 1.0


def test_review_when_borderline() -> None:
    m = FuzzyMatcher(SPEC, STD)
    # Only birth_date agrees (weight 2 of 4 = 0.5) -> >= review (0.5), < accept (0.8).
    rec = {"first_name": "AAA", "last_name": "BBB", "birth_date": "1990-01-01"}
    cands = [
        {
            "member_id": "M9",
            "first_name": "ZZZ",
            "last_name": "QQQ",
            "birth_date": "1990-01-01",
        }
    ]
    out = m.match(rec, cands)
    assert out.decision is MatchDecision.AMBIGUOUS
    assert out.score == 0.5


def test_unmatched_when_weak() -> None:
    m = FuzzyMatcher(SPEC, STD)
    rec = {"first_name": "AAA", "last_name": "BBB", "birth_date": "1990-01-01"}
    cands = [
        {
            "member_id": "M9",
            "first_name": "ZZZ",
            "last_name": "QQQ",
            "birth_date": "2000-12-31",
        }
    ]
    out = m.match(rec, cands)
    assert out.decision is MatchDecision.UNMATCHED


def test_no_candidates() -> None:
    m = FuzzyMatcher(SPEC, STD)
    out = m.match({"first_name": "X"}, [])
    assert out.decision is MatchDecision.UNMATCHED


# --- required=true: a hard precondition, independent of weight -------------
# See FieldComparison.required's docstring (config/models.py) for the full
# rationale: weight=0 alone does NOT gate a candidate out, it just means the
# field doesn't move the score — a candidate can still match via other
# fields. required=true actually disqualifies a candidate outright, before
# scoring, regardless of weight.

REQUIRED_SPEC = MatcherSpec(
    name="fuzzy_required_test",
    type="fuzzy",
    accept_threshold=0.80,
    review_threshold=0.50,
    comparisons=[
        FieldComparison(
            attribute="zip", method="exact", weight=0.0, threshold=1.0, required=True
        ),
        FieldComparison(attribute="address", method="jaro_winkler", weight=1.0, threshold=0.90),
    ],
)


def test_required_field_that_fails_disqualifies_the_candidate_even_with_a_perfect_score() -> None:
    """zip disagrees (required=true, weight=0) — even though address is a
    byte-identical match (would score 1.0 on weight alone), the candidate
    must never be selected at all."""
    m = FuzzyMatcher(REQUIRED_SPEC, STD)
    rec = {"zip": "02909", "address": "78 OAK AVE"}
    cands = [{"idcol_id": "1", "zip": "02116", "address": "78 OAK AVE"}]
    out = m.match(rec, cands)
    assert out.decision is MatchDecision.UNMATCHED


def test_required_field_that_passes_allows_normal_scoring() -> None:
    """zip agrees — required=true is satisfied, so scoring proceeds
    normally on the remaining (weighted) fields."""
    m = FuzzyMatcher(REQUIRED_SPEC, STD)
    rec = {"zip": "02909", "address": "78 OAK AVE"}
    cands = [{"idcol_id": "1", "zip": "02909", "address": "78 OAK AVE"}]
    out = m.match(rec, cands)
    assert out.decision is MatchDecision.MATCHED
    assert out.score == 1.0


def test_required_field_missing_on_either_side_disqualifies() -> None:
    """A required field missing entirely (None) must fail the gate — never
    silently treated as satisfied."""
    m = FuzzyMatcher(REQUIRED_SPEC, STD)
    rec = {"zip": None, "address": "78 OAK AVE"}
    cands = [{"idcol_id": "1", "zip": "02909", "address": "78 OAK AVE"}]
    out = m.match(rec, cands)
    assert out.decision is MatchDecision.UNMATCHED


def test_multiple_required_fields_must_all_pass() -> None:
    """Two required fields — a candidate failing EITHER one is
    disqualified, not just the first one checked."""
    spec = MatcherSpec(
        name="fuzzy_multi_required_test",
        type="fuzzy",
        accept_threshold=0.80,
        review_threshold=0.50,
        comparisons=[
            FieldComparison(
                attribute="first_name", method="exact", weight=0.0, threshold=1.0, required=True
            ),
            FieldComparison(
                attribute="zip", method="exact", weight=0.0, threshold=1.0, required=True
            ),
            FieldComparison(attribute="address", method="jaro_winkler", weight=1.0, threshold=0.90),
        ],
    )
    m = FuzzyMatcher(spec, STD)
    rec = {"first_name": "KATHERINE", "zip": "02909", "address": "78 OAK AVE"}

    # first_name fails, zip passes -> still disqualified.
    out = m.match(rec, [{"idcol_id": "1", "first_name": "KATRINA", "zip": "02909", "address": "78 OAK AVE"}])
    assert out.decision is MatchDecision.UNMATCHED

    # first_name passes, zip fails -> still disqualified.
    out = m.match(rec, [{"idcol_id": "2", "first_name": "KATHERINE", "zip": "02116", "address": "78 OAK AVE"}])
    assert out.decision is MatchDecision.UNMATCHED

    # Both pass -> scores normally.
    out = m.match(rec, [{"idcol_id": "3", "first_name": "KATHERINE", "zip": "02909", "address": "78 OAK AVE"}])
    assert out.decision is MatchDecision.MATCHED
