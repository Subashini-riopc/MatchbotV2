"""Derived blocking-field computation, shared by STAGE and MEMBER_UNIVERSE.

Both the cleanse stage (for incoming records) and the member-universe seed path
must compute the *same* derived columns so blocking keys line up. Centralizing
that here guarantees they never drift.

Derived columns (mirror the agreed DDL):
    first_name_std, last_name_std        — uppercased, stripped, suffix-removed
    first_name_metaphone1, last_name_metaphone1 — primary metaphone code
    last_name8                           — first 8 chars of last_name_std
    birth_year, birth_month, birth_day   — decomposed from birth_date
    name_ssn4_hash, name_dob_hash, name_addr_city_state_hash,
    name_addr_zip_hash
    — precomputed hash keys for the 4 deterministic matchers with 2+ keys
    (see HASH_KEY_SETS below) — the Snowflake-side counterpart
    (snowflake/python/matchbot_snowflake/derive_sql.py's
    deterministic_hash_sql/HASH_COLUMN_BY_KEYS) is THE reference
    implementation this must stay byte-for-byte identical to; both sides
    exist so DeterministicMatcher.match() can compare one hash string per
    candidate instead of looping over every key, same performance
    motivation as the Snowflake SQL join collapsing to one equality check.

Implemented with Polars expressions where possible; metaphone (a Python
function) is applied via map_elements over the standardized name.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

import polars as pl

from matchbot.matching import standardize as S

if TYPE_CHECKING:
    from matchbot.config.models import StandardizationConfig

# The derived columns this module guarantees to produce.
DERIVED_COLUMNS: tuple[str, ...] = (
    "first_name_std",
    "last_name_std",
    "first_name_metaphone1",
    "last_name_metaphone1",
    "last_name8",
    "birth_year",
    "birth_month",
    "birth_day",
    "address1_std",
    "zip5",
    "ssn4",
    "name_ssn4_hash",
    "name_dob_hash",
    "name_addr_city_state_hash",
    "name_addr_zip_hash",
)

# ---------------------------------------------------------------------------
# Deterministic-matcher hash keys.
#
# Key-set -> hash column name, in matcher-key order (order matters — it IS
# the concatenation order, and must exactly match the corresponding entry
# in snowflake/python/matchbot_snowflake/derive_sql.py's
# HASH_COLUMN_BY_KEYS, config/global.yaml's matcher chain being the single
# source of truth both sides independently encode). Covers the 4 current
# deterministic matchers with 2+ keys — deterministic_external_id
# and deterministic_ssn (1 key each) are intentionally absent (hashing a
# single column collapses nothing, and external_id specifically compares
# against a provider-varying reference column that can't be hashed once —
# see DeterministicMatcher's handling of the rilds_id key).
#
# deterministic_fn_addr (first_name_std, address1_std, state, zip5 — no
# last name) was removed as too loose a last-resort tier, reintroduced as
# deterministic_firstname_addr_state_zip, then removed again (along with
# deterministic_lastname_addr_state_zip) per explicit request — both
# carried real household-collision risk (people sharing a last name +
# address, or a first name + address, could false-match) with only
# state+zip as a narrowing anchor. name_addr_full_hash/name_addr_hash
# (deterministic_name_addr_full/_addr) were dropped earlier for the same
# reason. Rule 6 was then changed from deterministic_name_state_zip
# (first_name_std, last_name_std, state, zip5 — name_state_zip_hash) to
# deterministic_name_addr_zip (first_name_std, last_name_std,
# address1_std, zip5 — name_addr_zip_hash), swapping state for
# address1_std, per explicit request — current rule set is rules 1-6:
# external_id, ssn, name_ssn4, name_dob, name_addr_city_state,
# name_addr_zip.
HASH_KEY_SETS: dict[str, tuple[str, ...]] = {
    "name_ssn4_hash": ("first_name_std", "last_name_std", "ssn4"),
    "name_dob_hash": ("first_name_std", "last_name_std", "birth_date"),
    "name_addr_city_state_hash": (
        "first_name_std", "last_name_std", "address1_std", "city", "state",
    ),
    "name_addr_zip_hash": ("first_name_std", "last_name_std", "address1_std", "zip5"),
}

# birth_date reaches this module already as an ISO "YYYY-MM-DD" string (see
# cleanse.py's _parse_date_series), same canonical text form Snowflake's
# birth_date::VARCHAR cast produces from its native DATE column — so unlike
# matchers/deterministic.py's _norm() (which upper/trims every string
# value), a date-shaped key must NOT be uppercased/trimmed here: there's no
# case to normalize in a YYYY-MM-DD string, and running it through the same
# str.strip().upper() path as a name would be a no-op today but is
# explicitly excluded for clarity and to mirror derive_sql.py's
# _NON_STRING_HASH_KEYS distinction exactly.
_NON_STRING_HASH_KEYS = frozenset({"birth_date"})


def _row_hash(row: dict, keys: tuple[str, ...]) -> str | None:
    """One record's hash value for one matcher's key set, or None if any
    key is missing/blank — mirrors derive_sql.py's deterministic_hash_sql
    exactly: normalize each key (TRIM+UPPER for strings, untouched for
    birth_date), concatenate with '|', SHA-256 hex digest. NULL/None
    propagation matches _blank_check_sql's guard: a partially-missing key
    must never produce a hash (two records both missing the same field
    must not collide into a false match).
    """
    parts: list[str] = []
    for key in keys:
        value = row.get(key)
        if value is None:
            return None
        if key in _NON_STRING_HASH_KEYS:
            text = str(value)
        else:
            if not isinstance(value, str):
                value = str(value)
            text = value.strip().upper()
            if not text:
                return None
        parts.append(text)
    digest_input = "|".join(parts)
    return hashlib.sha256(digest_input.encode("utf-8")).hexdigest()


def add_hash_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Add the 7 deterministic-matcher hash columns to ``df``.

    Row-wise (map_elements over the whole row via struct), not a pure
    column expression: each hash combines several columns per row, and
    Polars has no built-in multi-column SHA-256 expression — this mirrors
    add_derived_columns' own use of map_elements for metaphone (a genuinely
    row-by-row Python computation), not a performance regression unique to
    hashing.
    """
    out = df
    for hash_col, keys in HASH_KEY_SETS.items():
        present_keys = [k for k in keys if k in out.columns]
        if len(present_keys) < len(keys):
            # A key this hash needs isn't even a column on this frame (e.g.
            # a provider that never maps address fields at all) — every row
            # necessarily has a missing key, so the hash is always None.
            out = out.with_columns(pl.lit(None, dtype=pl.Utf8).alias(hash_col))
            continue
        out = out.with_columns(
            pl.struct(list(keys))
            .map_elements(lambda row, keys=keys: _row_hash(row, keys), return_dtype=pl.Utf8)
            .alias(hash_col)
        )
    return out


def add_derived_columns(df: pl.DataFrame, std_config: StandardizationConfig) -> pl.DataFrame:
    """Return ``df`` with all derived blocking columns added (idempotent).

    Missing source columns are tolerated: derived values become null. This makes
    the function safe for providers lacking, e.g., birth_date (like RIDE).
    """
    out = df

    # Standardized names (suffix/prefix-stripped, uppercased, alnum-only).
    if "first_name" in out.columns:
        out = out.with_columns(
            pl.col("first_name")
            .map_elements(lambda v: S.std_name(v, std_config), return_dtype=pl.Utf8)
            .alias("first_name_std")
        )
    else:
        out = out.with_columns(pl.lit(None, dtype=pl.Utf8).alias("first_name_std"))

    if "last_name" in out.columns:
        out = out.with_columns(
            pl.col("last_name")
            .map_elements(lambda v: S.std_name(v, std_config), return_dtype=pl.Utf8)
            .alias("last_name_std")
        )
    else:
        out = out.with_columns(pl.lit(None, dtype=pl.Utf8).alias("last_name_std"))

    # Metaphone codes (primary) off the standardized names.
    out = out.with_columns(
        pl.col("first_name_std")
        .map_elements(S.metaphone, return_dtype=pl.Utf8)
        .alias("first_name_metaphone1"),
        pl.col("last_name_std")
        .map_elements(S.metaphone, return_dtype=pl.Utf8)
        .alias("last_name_metaphone1"),
    )

    # last_name8 — first 8 chars of the standardized last name.
    out = out.with_columns(
        pl.col("last_name_std").str.slice(0, 8).alias("last_name8")
    )

    # Standardized address1 (abbreviation-normalized street text).
    if "address1" in out.columns:
        out = out.with_columns(
            pl.col("address1")
            .map_elements(lambda v: S.std_address(v, std_config), return_dtype=pl.Utf8)
            .alias("address1_std")
        )
    else:
        out = out.with_columns(pl.lit(None, dtype=pl.Utf8).alias("address1_std"))

    # zip5 — zip truncated to 5 digits, ZIP+4 suffix dropped.
    if "zip" in out.columns:
        out = out.with_columns(
            pl.col("zip")
            .map_elements(S.std_zip, return_dtype=pl.Utf8)
            .alias("zip5")
        )
    else:
        out = out.with_columns(pl.lit(None, dtype=pl.Utf8).alias("zip5"))

    # ssn4 — last four digits of a standardized SSN.
    if "ssn" in out.columns:
        out = out.with_columns(
            pl.col("ssn")
            .map_elements(S.ssn4, return_dtype=pl.Utf8)
            .alias("ssn4")
        )
    else:
        out = out.with_columns(pl.lit(None, dtype=pl.Utf8).alias("ssn4"))

    # Decompose birth_date (stored as ISO string or Date) into y/m/d.
    if "birth_date" in out.columns:
        bd = pl.col("birth_date").cast(pl.Utf8, strict=False).str.strptime(
            pl.Date, format="%Y-%m-%d", strict=False
        )
        out = out.with_columns(
            bd.dt.year().cast(pl.Int16).alias("birth_year"),
            bd.dt.month().cast(pl.Int16).alias("birth_month"),
            bd.dt.day().cast(pl.Int16).alias("birth_day"),
        )
    else:
        out = out.with_columns(
            pl.lit(None, dtype=pl.Int16).alias("birth_year"),
            pl.lit(None, dtype=pl.Int16).alias("birth_month"),
            pl.lit(None, dtype=pl.Int16).alias("birth_day"),
        )

    # Deterministic-matcher hash columns — must run last, after
    # first_name_std/last_name_std/address1_std/zip5/ssn4 above are all
    # already materialized as real columns (every HASH_KEY_SETS input
    # except city/state/birth_date is one of those derived columns).
    out = add_hash_columns(out)

    return out
