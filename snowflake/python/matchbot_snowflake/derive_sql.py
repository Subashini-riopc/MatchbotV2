"""SQL port of matching/standardize.py + matching/derive.py.

Generates the SELECT expression list that turns RIDE_LAND's raw text
columns into RILDS_STAGE's canonical + derived columns — the Snowflake
equivalent of CanonicalStage + CleanseStage's transform/standardize/derive
steps, ported to SQL since this must run in-warehouse (see
docs/snowflake-implementation-plan.md, "highest-risk parity point").

Only the 4 matchers in scope for this demo
(deterministic_external_id/ssn/name_dob/name_addr — see config/global.yaml)
are used, so only first_name_std/last_name_std are actually consulted by
matching here. The metaphone/last_name8/birth_year/month/day columns exist
for schema parity with rilds_stage and to leave blocking-key support ready
for when fuzzy matchers are added later — see MATCHBOT_METAPHONE below for
why metaphone specifically needs a Python UDF, not a SQL built-in.

std_name's exact behavior (mirrored here — see standardize.py::std_name):
  1. uppercase, collapse internal whitespace, strip ends
  2. tokenize on single space
  3. drop any token whose value with trailing '.' stripped is a configured
     suffix (JR, SR, II, III, IV, V, VI, ESQ)
  4. if >1 token remains and the first token (period-stripped) is a
     configured prefix (MR, MRS, MS, DR), drop it
  5. join remaining tokens with NO separator
  6. strip all characters that aren't A-Z or 0-9
  7. empty result -> NULL
"""

from __future__ import annotations

from matchbot_snowflake.config_models import StandardizationConfig


def _sql_string_array(values: list[str]) -> str:
    """Render a Python string list as a Snowflake ARRAY_CONSTRUCT literal."""
    quoted = ", ".join("'" + v.upper().replace("'", "''") + "'" for v in values)
    return f"ARRAY_CONSTRUCT({quoted})"


def std_name_sql(raw_column: str, std_config: StandardizationConfig) -> str:
    """SQL expression standardizing ``raw_column`` per std_name()'s rules.

    Uses Snowflake's SPLIT/FILTER/ARRAY_TO_STRING to reproduce the
    tokenize -> drop-suffix -> drop-one-leading-prefix -> rejoin pipeline
    without a per-row UDF, since this part is expressible in native SQL
    array functions.
    """
    suffixes_array = _sql_string_array(std_config.name_suffixes)
    prefixes_array = _sql_string_array(std_config.name_prefixes)

    # Step 1: uppercase + collapse whitespace (REGEXP_REPLACE collapses
    # runs of whitespace to one space, mirroring squash_ws's \s+ -> " ").
    normalized = (
        f"TRIM(REGEXP_REPLACE(UPPER({raw_column}), '\\\\s+', ' '))"
    )

    # Step 2-3: tokenize, drop suffix tokens (RTRIM of '.' before compare,
    # matching Python's token.strip('.')). ARRAY_COMPACT (not
    # ARRAY_REMOVE(arr, NULL)) removes the NULL placeholders TRANSFORM's
    # lambda leaves for dropped tokens — ARRAY_REMOVE's second argument is
    # a *value* to remove via equality, and NULL never equals anything
    # (including itself) in SQL, so ARRAY_REMOVE(arr, NULL) never matches
    # any element and returns NULL overall on Snowflake rather than the
    # original array. Caught during build-step-3 parity validation against
    # real RIDE data (first_name_std/last_name_std/metaphone all came back
    # NULL) — see docs/snowflake-implementation-plan.md.
    tokens_no_suffix = f"""
        ARRAY_COMPACT(
            TRANSFORM(
                SPLIT({normalized}, ' '),
                t -> IFF(
                    ARRAY_CONTAINS(RTRIM(t, '.')::VARIANT, {suffixes_array}),
                    NULL,
                    t
                )
            )
        )
    """

    # Step 4: drop a single leading prefix token if >1 token remains.
    tokens_final = f"""
        IFF(
            ARRAY_SIZE({tokens_no_suffix}) > 1
            AND ARRAY_CONTAINS(
                RTRIM(GET({tokens_no_suffix}, 0)::VARCHAR, '.')::VARIANT,
                {prefixes_array}
            ),
            ARRAY_SLICE({tokens_no_suffix}, 1, ARRAY_SIZE({tokens_no_suffix})),
            {tokens_no_suffix}
        )
    """

    # Step 5-7: join with no separator, strip non-alphanumeric, NULL if empty.
    joined = f"ARRAY_TO_STRING({tokens_final}, '')"
    stripped = f"REGEXP_REPLACE({joined}, '[^A-Z0-9]', '')"
    return f"NULLIF({stripped}, '')"


def std_address_sql(raw_column: str, std_config: StandardizationConfig) -> str:
    """SQL expression standardizing ``raw_column`` per std_address()'s rules:
    uppercase, collapse whitespace, then replace each whole token matching a
    configured address_abbreviations key with its abbreviation (e.g.
    "STREET" -> "ST"). Unmatched tokens pass through unchanged. NULL/empty
    input -> NULL, matching std_name_sql's empty-result handling.

    Token-by-token replacement (not a blanket string REPLACE) so a
    substring match inside an unrelated word never gets corrupted — e.g.
    replacing "EAST" -> "E" must not also rewrite "EASTON" or "NORTHEAST".
    """
    normalized = f"TRIM(REGEXP_REPLACE(UPPER({raw_column}), '\\\\s+', ' '))"
    if not std_config.address_abbreviations:
        return f"NULLIF({normalized}, '')"

    abbrev_map = {k.upper(): v.upper() for k, v in std_config.address_abbreviations.items()}
    case_branches = "\n            ".join(
        f"WHEN t = '{k}' THEN '{v}'" for k, v in abbrev_map.items()
    )
    tokens_abbreviated = f"""
        TRANSFORM(
            SPLIT({normalized}, ' '),
            t -> CASE
                {case_branches}
                ELSE t
            END
        )
    """
    joined = f"ARRAY_TO_STRING({tokens_abbreviated}, ' ')"
    return f"NULLIF({joined}, '')"


def std_zip_sql(raw_column: str) -> str:
    """SQL expression mirroring std_zip(): strip to the first 5 digits,
    dropping any ZIP+4 suffix. NULL if fewer than 5 digits remain."""
    digits = f"REGEXP_REPLACE({raw_column}, '[^0-9]', '')"
    first_five = f"LEFT({digits}, 5)"
    return f"IFF(LENGTH({digits}) >= 5, {first_five}, NULL)"


def ssn4_sql(raw_ssn_column: str) -> str:
    """SQL expression mirroring ssn4(): strip non-digits, left-pad to 9,
    take the last 4. NULL if the standardized SSN isn't exactly 9 digits
    (matches std_ssn()'s malformed-SSN guard — a short/garbled SSN must not
    produce a spurious last-4 value that could false-collide).

    Checks LENGTH(digits) <= 9 explicitly before padding: Snowflake's LPAD
    truncates from the left when the input is already longer than the
    target length, so a 12-digit garbled SSN would otherwise silently come
    out as 9 characters and pass the length check — Python's zfill never
    truncates, so std_ssn() correctly rejects that case via its own
    len(digits) == width comparison. Without this extra guard, the SQL
    version would diverge from Python by accepting inputs Python rejects.
    """
    digits = f"REGEXP_REPLACE({raw_ssn_column}, '[^0-9]', '')"
    padded = f"LPAD({digits}, 9, '0')"
    return (
        f"IFF({digits} IS NOT NULL AND LENGTH({digits}) BETWEEN 1 AND 9, "
        f"RIGHT({padded}, 4), NULL)"
    )


def std_gender_sql(raw_column: str, std_config: StandardizationConfig) -> str:
    """SQL expression mirroring std_gender(): uppercase, look up in
    gender_map (case-insensitive), fall back to the uppercased raw value."""
    normalized = f"TRIM(REGEXP_REPLACE(UPPER({raw_column}), '\\\\s+', ' '))"
    if not std_config.gender_map:
        return f"NULLIF({normalized}, '')"

    case_branches = "\n        ".join(
        f"WHEN {normalized} = '{k.upper()}' THEN '{v.upper()}'"
        for k, v in std_config.gender_map.items()
    )
    return f"""CASE
        {case_branches}
        ELSE NULLIF({normalized}, '')
    END"""


# jellyfish.metaphone (Python) and Snowflake's native SOUNDEX are different
# algorithms with different output — using SOUNDEX here would silently
# break parity with the AWS demo's blocking/fuzzy behavior. True parity
# requires a Python UDF running the SAME jellyfish library, registered once
# per session/deployment (see procedures/run_pipeline.py, which registers
# this UDF via Snowpark's session.udf.register using jellyfish directly,
# not the SQL below). This constant documents the UDF name the generated
# SQL calls; it does not itself define the UDF.
METAPHONE_UDF_NAME = "MATCHBOT_METAPHONE"


def metaphone_sql(std_name_column: str) -> str:
    """Call the registered MATCHBOT_METAPHONE UDF on an already-standardized
    name column. NULL in, NULL out (matches metaphone()'s None handling)."""
    return f"{METAPHONE_UDF_NAME}({std_name_column})"


def last_name8_sql(last_name_std_column: str) -> str:
    """First 8 chars of the standardized last name."""
    return f"LEFT({last_name_std_column}, 8)"


def birth_parts_sql(birth_date_column: str) -> dict[str, str]:
    """SQL expressions for birth_year/birth_month/birth_day from a DATE
    column already parsed from the source (see provider_sql.py for the
    %Y-%m-%d-equivalent TO_DATE parsing of the raw provider column)."""
    return {
        "birth_year": f"YEAR({birth_date_column})",
        "birth_month": f"MONTH({birth_date_column})",
        "birth_day": f"DAY({birth_date_column})",
    }


# ---------------------------------------------------------------------------
# Deterministic-matcher hash keys.
#
# THE SINGLE SOURCE OF TRUTH for how a multi-column deterministic matcher's
# hash column is computed — RILDS_STAGE computes it via
# deterministic_hash_sql() below (called from provider_sql.py), and
# whichever separate process populates/refreshes RILDS_REFERENCE MUST
# compute its own hash columns using this exact same formula, or a
# genuinely matching pair of records will hash to different values and
# silently never match (see snowflake/ddl/05_reference_table.sql's
# comment on the hash columns for the formula written out for that
# process to follow). There is no legacy Python predecessor to mirror
# here — this hashing scheme is new to this pipeline.
#
# Formula, precisely, for a matcher with keys [k1, k2, ...] (order is the
# matcher's key order in config/global.yaml — never reorder independently
# on either side):
#   1. Normalize each key exactly as matchers/deterministic.py's _norm_sql
#      does at comparison time: TRIM(UPPER(col::VARCHAR)) for string-typed
#      keys, the raw value (cast to VARCHAR for concatenation) for
#      date/numeric-typed keys (birth_date, birth_year, birth_month,
#      birth_day — see _NON_STRING_HASH_KEYS). This mirrors the join's own
#      normalization exactly, so the hash can never disagree with what a
#      direct column-by-column join would have compared, regardless of
#      whether the stored column happens to already be clean (e.g.
#      city/state are stored upper/trimmed already per most providers'
#      transforms, but this formula does not rely on that assumption —
#      every key is defensively re-normalized here, same as the join does
#      today).
#   2. If ANY key's normalized value is NULL or blank (matching
#      _blank_check_sql's guard), the WHOLE hash is NULL — never hash a
#      partially-missing key. Two rows both missing the same field must
#      never collide into a false hash match (e.g. two people both missing
#      an address must not hash to the same address-based hash value).
#   3. Concatenate the normalized values with a literal '|' delimiter
#      (chosen because none of these fields can naturally contain a pipe
#      character post-normalization) and hash with SHA2(..., 256).
_NON_STRING_HASH_KEYS = frozenset({"birth_date", "birth_year", "birth_month", "birth_day"})


def _hash_norm_sql(column_ref: str, key: str) -> str:
    """Mirror matchers/deterministic.py's _norm_sql exactly — see this
    section's module-level comment for why the hash must use the identical
    normalization the join itself uses."""
    if key in _NON_STRING_HASH_KEYS:
        return f"{column_ref}::VARCHAR"
    return f"TRIM(UPPER({column_ref}::VARCHAR))"


def _hash_blank_check_sql(column_ref: str, key: str) -> str:
    """Mirror matchers/deterministic.py's _blank_check_sql exactly."""
    if key in _NON_STRING_HASH_KEYS:
        return f"{column_ref} IS NOT NULL"
    return f"({column_ref} IS NOT NULL AND TRIM({column_ref}::VARCHAR) != '')"


def deterministic_hash_sql(keys: list[str], column_refs: dict[str, str]) -> str:
    """SQL expression for one matcher's hash column, e.g. name_dob_hash.

    ``keys`` is the matcher's key list in config/global.yaml order (order
    matters — see module comment). ``column_refs`` maps each canonical key
    name to the actual column reference to read it from (e.g.
    {"first_name_std": "first_name_std", "birth_date": "birth_date"} when
    called from provider_sql.py's final SELECT, where those columns are
    already in scope under their own names) — a plain dict rather than
    always assuming ``key`` doubles as the column name, since a caller
    computing this against a table with differently-named/aliased columns
    needs to supply the real reference.

    Only meant for matchers with 2+ keys — see config/global.yaml's
    deterministic_external_id/deterministic_ssn, which stay plain
    single-column comparisons in matchers/deterministic.py (hashing a
    single column collapses nothing; there is no join-cost benefit, and
    deterministic_external_id specifically compares against a
    provider-varying reference column that can't be hashed once — see
    matchers/deterministic.py's external_id_column handling).
    """
    guard = " AND ".join(_hash_blank_check_sql(column_refs[k], k) for k in keys)
    normalized_concat = " || '|' || ".join(_hash_norm_sql(column_refs[k], k) for k in keys)
    return f"IFF({guard}, SHA2({normalized_concat}, 256), NULL)"


# ---------------------------------------------------------------------------
# Fixed key-set -> hash-column-name registry.
#
# Explicit rather than derived from a matcher's NAME (e.g. deriving
# "name_dob_hash" from the string "deterministic_name_dob") — keyed on the
# matcher's KEYS tuple instead, so provider_sql.py (which computes hash
# columns on RILDS_STAGE, keyed by what canonical attributes are actually
# available) and matchers/deterministic.py (which emits the join predicate
# for a MatcherSpec, keyed by config/global.yaml's declared keys) are
# guaranteed to agree on the same column name for the same key-set without
# either one needing to parse or trust the other's matcher NAME string.
#
# Covers the 4 current deterministic matchers with 2+ keys (see
# config/global.yaml) — deterministic_external_id and deterministic_ssn
# (1 key each) are intentionally absent; see deterministic_hash_sql's
# docstring for why those two stay plain column comparisons.
#
# deterministic_fn_addr (first_name_std, address1_std, state, zip5 — no
# last name) was removed as too loose a last-resort tier, reintroduced as
# deterministic_firstname_addr_state_zip, then removed again (along with
# deterministic_lastname_addr_state_zip) per explicit request — both
# carried real household-collision risk (people sharing a last name +
# address, or a first name + address, could false-match) with only
# state+zip as a narrowing anchor. deterministic_name_addr_full and
# deterministic_name_addr (and their hash columns name_addr_full_hash/
# name_addr_hash) were dropped earlier for the same reason. Rule 6 was
# then changed from deterministic_name_state_zip (first_name_std,
# last_name_std, state, zip5 — name_state_zip_hash) to
# deterministic_name_addr_zip (first_name_std, last_name_std,
# address1_std, zip5 — name_addr_zip_hash), swapping state for
# address1_std, per explicit request — not part of the current rule set
# (rules 1-6: external_id, ssn, name_ssn4, name_dob, name_addr_city_state,
# name_addr_zip).
#
# Adding another multi-key deterministic matcher later: add its key tuple
# (order matters — must match that matcher's `keys:` list in
# config/global.yaml exactly) and a new hash column name here, add the
# matching computed column to provider_sql.py's projection and to both
# RILDS_STAGE and RILDS_REFERENCE's DDL — four places, all listed together
# so nothing is easy to half-do.
HASH_COLUMN_BY_KEYS: dict[tuple[str, ...], str] = {
    ("first_name_std", "last_name_std", "ssn4"): "name_ssn4_hash",
    ("first_name_std", "last_name_std", "birth_date"): "name_dob_hash",
    ("first_name_std", "last_name_std", "address1_std", "city", "state"): "name_addr_city_state_hash",
    ("first_name_std", "last_name_std", "address1_std", "zip5"): "name_addr_zip_hash",
}


def hash_column_for_keys(keys: list[str]) -> str | None:
    """The precomputed hash column name for this exact key tuple, or None
    if this key-set has no registered hash column (e.g. a single-key
    matcher, or a multi-key combination not yet added to
    HASH_COLUMN_BY_KEYS) — callers must fall back to plain column-by-column
    comparison in that case, not assume a hash column exists.
    """
    return HASH_COLUMN_BY_KEYS.get(tuple(keys))
