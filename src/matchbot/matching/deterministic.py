"""Deterministic matcher: all configured keys must agree exactly.

Config-driven (the keys come from :class:`MatcherSpec.keys`), so SSN+DOB,
name+DOB, or any other exact combination is declared in YAML, not code. A
candidate matches only if every key attribute is present and equal on both
sides; the first candidate to satisfy that wins.

Hash-based comparison: for any key list registered in derive.py's
HASH_KEY_SETS (every current matcher with 2+ keys), if ``record`` actually
carries that precomputed hash key (added by
add_derived_columns/add_hash_columns — real pipeline data always has it),
comparing that one string is equivalent to, and cheaper than, normalizing
and comparing every individual key per candidate (mirrors the
Snowflake-side join collapsing to a single hash equality — see
snowflake/python/matchbot_snowflake/matchers/deterministic.py). A record
that doesn't carry the hash key at all (hand-built test fixtures, or any
caller that skips add_derived_columns) falls back to the original per-key
comparison loop, so this optimization is purely additive — never a
required contract callers must satisfy. A key list with no registered
hash at all (currently just rilds_id/ssn, both single-key — see
derive.py's HASH_KEY_SETS docstring for why) always uses the per-key path.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from matchbot.config.models import MatcherSpec, StandardizationConfig
from matchbot.domain.enums import MatchDecision, MatchMethod
from matchbot.matching.base import NO_MATCH, MatchOutcome, register_matcher
from matchbot.matching.base import member_key as _member_key
from matchbot.matching.derive import HASH_KEY_SETS

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

# Reverse lookup: a matcher's key tuple -> its precomputed hash column
# name, if one is registered. Built once at import time from derive.py's
# HASH_KEY_SETS (key tuple order matters and must match a matcher's own
# spec.keys order exactly — see that module's docstring).
_HASH_COLUMN_BY_KEYS: dict[tuple[str, ...], str] = {
    keys: hash_col for hash_col, keys in HASH_KEY_SETS.items()
}


def _norm(value: Any) -> Any:
    """Normalize a value for exact comparison (uppercase/trim strings)."""
    if isinstance(value, str):
        return value.strip().upper()
    return value


@register_matcher("deterministic")
class DeterministicMatcher:
    """Exact-equality matcher over a configured set of key attributes."""

    def __init__(self, spec: MatcherSpec, std_config: StandardizationConfig) -> None:
        self.name = spec.name
        self.keys = list(spec.keys)
        self._std = std_config
        # None if this key list has no registered hash column (e.g. the
        # single-key rilds_id/ssn matchers) — checked once at construction,
        # not per-record, since a matcher's keys never change after init.
        self._hash_column = _HASH_COLUMN_BY_KEYS.get(tuple(spec.keys))

    def match(
        self,
        record: Mapping[str, Any],
        candidates: Sequence[Mapping[str, Any]],
    ) -> MatchOutcome:
        # Only take the hash path if the record ACTUALLY carries that hash
        # key (dict membership, not "is the value None") — a record built
        # without add_derived_columns/add_hash_columns (hand-built test
        # fixtures, an older cached frame, a caller that only computed
        # first_name_std/last_name_std/etc. directly) simply won't have the
        # key in its dict at all, and must fall back to the original
        # per-key comparison rather than be silently treated as "hash is
        # None -> no match". A record that DOES carry the key with value
        # None means add_hash_columns genuinely found a missing/blank
        # underlying key for this row — that (and only that) is a real
        # NO_MATCH, handled inside _match_by_hash.
        if self._hash_column is not None and self._hash_column in record:
            return self._match_by_hash(record, candidates)
        return self._match_by_keys(record, candidates)

    def _match_by_hash(
        self,
        record: Mapping[str, Any],
        candidates: Sequence[Mapping[str, Any]],
    ) -> MatchOutcome:
        record_hash = record[self._hash_column]
        if record_hash is None:
            # Mirrors _match_by_keys' "any missing key -> NO_MATCH" guard:
            # add_hash_columns already makes the hash None whenever any
            # underlying key was missing/blank, so a None hash here means
            # the same thing a missing individual key meant before.
            return NO_MATCH
        for cand in candidates:
            if self._hash_column in cand and cand[self._hash_column] == record_hash:
                return MatchOutcome(
                    decision=MatchDecision.MATCHED,
                    method=MatchMethod.DETERMINISTIC,
                    idcol_id=_member_key(cand),
                    score=1.0,
                    reason=f"{self.name}: exact match on {'+'.join(self.keys)}",
                )
        return NO_MATCH

    def _match_by_keys(
        self,
        record: Mapping[str, Any],
        candidates: Sequence[Mapping[str, Any]],
    ) -> MatchOutcome:
        # A record with any missing key cannot match deterministically.
        rec_vals = {}
        for k in self.keys:
            v = record.get(k)
            if v is None or (isinstance(v, str) and not v.strip()):
                return NO_MATCH
            rec_vals[k] = _norm(v)

        for cand in candidates:
            if all(_norm(cand.get(k)) == rec_vals[k] for k in self.keys):
                return MatchOutcome(
                    decision=MatchDecision.MATCHED,
                    method=MatchMethod.DETERMINISTIC,
                    idcol_id=_member_key(cand),
                    score=1.0,
                    reason=f"{self.name}: exact match on {'+'.join(self.keys)}",
                )
        return NO_MATCH
