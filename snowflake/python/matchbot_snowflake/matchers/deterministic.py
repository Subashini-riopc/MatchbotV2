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

All 6 matchers in the current config/global.yaml chain
(deterministic_external_id, deterministic_ssn, deterministic_name_ssn4,
deterministic_name_dob, deterministic_name_addr_city_state, and
deterministic_name_addr_zip) are this one type.
deterministic_fn_addr (first_name + address only, no last name) was
removed as too loose a last-resort tier, then reintroduced as
deterministic_firstname_addr_state_zip (same key shape), then removed
again along with deterministic_lastname_addr_state_zip per explicit
request — both carried real household-collision risk (people sharing a
last name + address, or a first name + address, could false-match) with
only state+zip as a narrowing anchor. deterministic_name_addr_full and
deterministic_name_addr were dropped earlier for the same reasoning.
deterministic_name_state_zip (first_name_std, last_name_std, state,
zip5) was then changed to deterministic_name_addr_zip (first_name_std,
last_name_std, address1_std, zip5 — state swapped for address1_std) per
explicit request — not part of the current 6-rule chain.

Hash-based joins: for any matcher whose exact key list is registered in
derive_sql.py's HASH_COLUMN_BY_KEYS (every current matcher with 2+ keys),
the join collapses to a single s.<hash_col> = r.<hash_col> equality check
against a column RILDS_STAGE/RILDS_REFERENCE precompute once (see
provider_sql.py / that table's DDL comments) — instead of the multi-column
AND chain built here for anything not in that registry (currently just
deterministic_external_id/deterministic_ssn, both single-key already, so
hashing has no join-cost benefit and external_id specifically compares
against a provider-varying reference column that can't be hashed once —
see external_id_column below). The guard for a hash-based join is just
"the stage row's hash column is not NULL" — a non-NULL hash already implies
every underlying key was present (see derive_sql.py's NULL-propagation:
any missing key makes the whole hash NULL), so there's no need to re-check
each individual key here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from matchbot_snowflake.derive_sql import hash_column_for_keys
from matchbot_snowflake.matcher_registry import _BuiltFragment, register_sql_matcher

if TYPE_CHECKING:
    from matchbot.config.models import MatcherSpec

# match_method is the matcher's own config/global.yaml name verbatim
# (e.g. "deterministic_name_dob", "deterministic_name_addr_full") rather
# than a coarse EXACT/EXACT_SASID bucket — see matching/vocab.py's
# method_to_db() on the AWS side for the same change (and the reasoning:
# collapsing every non-external_id deterministic tier into one "EXACT"
# label made it impossible to tell which of the 7 rules actually matched
# a given row from rilds_matched.match_method alone).
def _method_label(matcher_name: str) -> str:
    return matcher_name


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
) -> _BuiltFragment:
    """Return a _BuiltFragment for one deterministic MatcherSpec.

    ``external_id_column`` is the current provider's
    ProviderConfig.external_id_column (e.g. 'sasid' for RIDE). Only
    consulted when spec.keys contains 'rilds_id': RILDS_STAGE has a real,
    generic rilds_id column (populated by provider_sql.py the same way
    for every provider), but RILDS_REFERENCE has no rilds_id column at
    all — each provider's external id lives under its own column there
    (SASID, CCRI_ID, ...). Mirrors storage/postgres.py's
    d["rilds_id"] = d.get(external_id_column), which does the same
    dynamic resolution on the Python/Postgres side.

    score_sql/accept_threshold/review_threshold are left at _BuiltFragment's
    defaults (always 1.0/1.0/1.0): a deterministic join is either an exact
    match (score 1.0, always >= the 1.0 accept_threshold) or the row simply
    never appears as a join candidate at all — there is no partial-credit
    or review-band case for this matcher type.
    """
    if not spec.keys:
        # A deterministic matcher with no keys can never match anything —
        # same as the Python path, where DeterministicMatcher.__init__
        # would just build a matcher whose `for k in self.keys` loop never
        # runs and immediately returns NO_MATCH. Encode that explicitly
        # rather than emit SQL with an empty AND/ON clause.
        return _BuiltFragment("1 = 0", "1 = 0", _method_label(spec.name))

    # rilds_id (deterministic_external_id) is never eligible for a hash
    # column even if someone added it to HASH_COLUMN_BY_KEYS by mistake:
    # its reference-side column is provider-varying (external_id_column),
    # so a single precomputed reference-side hash could never be correct
    # for every provider at once. Guarded here defensively, though
    # HASH_COLUMN_BY_KEYS today simply never lists rilds_id/ssn (both
    # single-key) at all.
    hash_column = None
    if _EXTERNAL_ID_KEY not in spec.keys:
        hash_column = hash_column_for_keys(spec.keys)

    if hash_column is not None:
        join_conditions = f"s.{hash_column} = r.{hash_column}"
        # A non-NULL hash already implies every underlying key was present
        # and non-blank — see derive_sql.py's deterministic_hash_sql, whose
        # IFF(...) guard makes the whole hash NULL if any key is missing.
        # No need to re-check each individual key here.
        guard_conditions = f"s.{hash_column} IS NOT NULL"
        return _BuiltFragment(join_conditions, guard_conditions, _method_label(spec.name))

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

    return _BuiltFragment(join_conditions, guard_conditions, _method_label(spec.name))
