"""Mapping between internal match outcomes and the DB vocabulary.

The pipeline reasons in terms of :class:`MatchDecision` / :class:`MatchMethod`;
the stage/target/error tables use the agreed string vocabulary for
match_status/decision (MATCHED / LOW_CONFIDENCE / NO_MATCH). match_method
itself is the matcher's own config/global.yaml name verbatim (see
method_to_db below) rather than a separate coded vocabulary — centralized
here so the decision->status mapping stays in one place and is easy to
extend.
"""

from __future__ import annotations

from matchbot.domain.enums import MatchDecision, MatchMethod

# stage.match_status / error.decision vocabulary
STATUS_MATCHED = "MATCHED"
STATUS_LOW_CONFIDENCE = "LOW_CONFIDENCE"
STATUS_NO_MATCH = "NO_MATCH"
STATUS_PENDING = "PENDING"


def decision_to_status(decision: MatchDecision) -> str:
    return {
        MatchDecision.MATCHED: STATUS_MATCHED,
        MatchDecision.AMBIGUOUS: STATUS_LOW_CONFIDENCE,
        MatchDecision.UNMATCHED: STATUS_NO_MATCH,
    }[decision]


def method_to_db(method: MatchMethod, matcher_name: str) -> str:
    """Map an internal method + matcher name to the target.match_method vocab.

    Returns the matcher's own config/global.yaml name verbatim (e.g.
    'deterministic_name_dob', 'fuzzy_exact_name_addr') rather than a
    coarse EXACT/EXACT_SASID/LEVENSHTEIN bucket — collapsing every
    non-external_id deterministic tier (or every fuzzy tier) into one
    label made it impossible to tell which of the deterministic/fuzzy
    rules actually matched a given row from match_method alone. NONE is
    the fallback for MatchMethod.NONE (no matcher applied)."""
    if method is MatchMethod.NONE:
        return "NONE"
    return matcher_name
