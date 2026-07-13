"""SQL fragment generator for MatcherSpec.type == "deterministic".

Reproduces matching/deterministic.py::DeterministicMatcher.match()'s exact
semantics as SQL:

* guard: every key in spec.keys must be non-NULL and non-blank on the
  staged row, or this matcher contributes nothing for that row (mirrors
  "A record with any missing key cannot match deterministically" —
  DeterministicMatcher.match(), the early-return NO_MATCH before any
  candidate is even considered).
* join: every key must be exactly equal (case/whitespace-normalized, same
  as deterministic.py's _norm()) between the staged row and a candidate
  reference row.

All 4 matchers in the current config/global.yaml chain
(deterministic_external_id, deterministic_ssn, deterministic_name_dob,
deterministic_name_addr) are this one type — this is the only generator
needed for the demo's exact-match-parity scope.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from matchbot_snowflake.matcher_registry import register_sql_matcher

if TYPE_CHECKING:
    from matchbot.config.models import MatcherSpec

# method_to_db()'s exact rule (matching/vocab.py): a deterministic matcher
# whose NAME contains "external_id" or "sasid" reports as EXACT_SASID;
# every other deterministic matcher reports as plain EXACT. Reproduced
# verbatim here rather than imported, since vocab.py's function takes a
# matcher NAME string and a MatchMethod enum — trivial logic, cheaper to
# mirror than to add a runtime dependency on matchbot's enum module for one
# string check.
_SASID_NAME_MARKERS = ("external_id", "sasid")


def _method_label(matcher_name: str) -> str:
    lowered = matcher_name.lower()
    if any(marker in lowered for marker in _SASID_NAME_MARKERS):
        return "EXACT_SASID"
    return "EXACT"


# Keys backed by a non-string column type: _norm() (deterministic.py) only
# uppercase/trims actual Python str values and leaves everything else (e.g.
# a date object) untouched, comparing it natively. birth_date is DATE in
# both RILDS_STAGE and RILDS_REFERENCE (schema.py), so it must compare as a
# date directly — casting through VARCHAR first is unnecessary and risks a
# false mismatch if the two sides ever format dates differently.
_NON_STRING_KEYS = frozenset({"birth_date", "birth_year", "birth_month", "birth_day"})


# The one canonical attribute whose column name legitimately differs
# between stage and reference — see build_deterministic_fragment's
# external_id_column parameter for the full explanation.
_EXTERNAL_ID_KEY = "rilds_id"


def _col(alias: str, key: str, *, reference_column_override: str | None = None) -> str:
    """An alias-qualified column reference, e.g. s.SSN.

    Unquoted — not s."ssn". Snowflake folds unquoted identifiers (both in
    DDL and in queries) to UPPERCASE by default; a quoted lowercase
    reference is a case-sensitive literal that doesn't match the actual
    column (same bug, same fix, as provider_sql.py's canonical_sql() —
    see that module's comment for the live SQL compilation error that
    first caught it: invalid identifier 'LAND."firstname"').

    reference_column_override swaps in a different column name only for
    the reference-side alias ('r') and only when set — used for the
    rilds_id key, whose reference-side column is provider-specific (e.g.
    SASID for RIDE) rather than a real rilds_id column.
    """
    if reference_column_override is not None and alias == "r":
        return f"{alias}.{reference_column_override}"
    return f"{alias}.{key}"


def _norm_sql(column_ref: str, key: str) -> str:
    """Mirror deterministic.py::_norm(): uppercase+trim for string-typed
    keys, direct (untouched) comparison for date/numeric-typed keys."""
    if key in _NON_STRING_KEYS:
        return column_ref
    return f"TRIM(UPPER({column_ref}::VARCHAR))"


def _blank_check_sql(column_ref: str, key: str) -> str:
    """True when column_ref counts as present — mirrors
    DeterministicMatcher.match()'s ``v is None or (isinstance(v, str) and
    not v.strip())`` guard: non-string keys only need a NULL check, string
    keys also need the trimmed-empty check."""
    if key in _NON_STRING_KEYS:
        return f"{column_ref} IS NOT NULL"
    return f"({column_ref} IS NOT NULL AND TRIM({column_ref}::VARCHAR) != '')"


@register_sql_matcher("deterministic")
def build_deterministic_fragment(
    spec: "MatcherSpec", external_id_column: str
) -> tuple[str, str, str]:
    """Return (join_predicate_sql, guard_predicate_sql, method_label) for
    one deterministic MatcherSpec.

    ``external_id_column`` is the current provider's
    ProviderConfig.external_id_column (e.g. 'sasid' for RIDE). Only
    consulted when spec.keys contains 'rilds_id': RILDS_STAGE has a real,
    generic rilds_id column (populated by provider_sql.py the same way
    for every provider), but RILDS_REFERENCE has no rilds_id column at
    all — each provider's external id lives under its own column there
    (SASID, CCRI_ID, ...). Mirrors storage/postgres.py's
    d["rilds_id"] = d.get(external_id_column), which does the same
    dynamic resolution on the Python/Postgres side.
    """
    if not spec.keys:
        # A deterministic matcher with no keys can never match anything —
        # same as the Python path, where DeterministicMatcher.__init__
        # would just build a matcher whose `for k in self.keys` loop never
        # runs and immediately returns NO_MATCH. Encode that explicitly
        # rather than emit SQL with an empty AND/ON clause.
        return ("1 = 0", "1 = 0", _method_label(spec.name))

    def _ref_override(key: str) -> str | None:
        return external_id_column if key == _EXTERNAL_ID_KEY else None

    join_conditions = " AND ".join(
        f"{_norm_sql(_col('s', key), key)} = "
        f"{_norm_sql(_col('r', key, reference_column_override=_ref_override(key)), key)}"
        for key in spec.keys
    )
    guard_conditions = " AND ".join(
        _blank_check_sql(_col("s", key), key) for key in spec.keys
    )

    return (join_conditions, guard_conditions, _method_label(spec.name))
