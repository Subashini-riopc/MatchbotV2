"""Unit tests for derive_sql.py's deterministic-matcher hash-key logic.

Covers deterministic_hash_sql() (the shared formula) and
hash_column_for_keys()/HASH_COLUMN_BY_KEYS (the key-set -> column-name
registry both provider_sql.py and matchers/deterministic.py rely on to
agree on the same hash column for the same matcher). See
derive_sql.py's own module comment above these functions for the full
design rationale (why a hash join instead of a multi-column AND join,
why NULL must propagate, why column order matters).
"""

from __future__ import annotations

from matchbot_snowflake.derive_sql import (
    HASH_COLUMN_BY_KEYS,
    deterministic_hash_sql,
    hash_column_for_keys,
)


def test_hash_column_by_keys_covers_all_four_multi_key_matchers() -> None:
    """Every deterministic matcher in the current config/global.yaml chain
    with 2+ keys must have a registered hash column —
    deterministic_external_id/deterministic_ssn (1 key each) are the only
    ones intentionally absent. deterministic_fn_addr (fn_addr_hash) was
    removed as too loose a last-resort tier, reintroduced as
    deterministic_firstname_addr_state_zip, then removed again along with
    deterministic_lastname_addr_state_zip per explicit request — both
    carried real household-collision risk. name_addr_full_hash/
    name_addr_hash were dropped earlier for the same reasoning. Rule 6
    (deterministic_name_state_zip, name_state_zip_hash) was then changed
    to deterministic_name_addr_zip (name_addr_zip_hash) — state swapped
    for address1_std — none are part of the current 6-rule chain."""
    assert len(HASH_COLUMN_BY_KEYS) == 4
    assert set(HASH_COLUMN_BY_KEYS.values()) == {
        "name_ssn4_hash",
        "name_dob_hash",
        "name_addr_city_state_hash",
        "name_addr_zip_hash",
    }


def test_hash_column_for_keys_matches_registered_key_sets() -> None:
    assert hash_column_for_keys(["first_name_std", "last_name_std", "birth_date"]) == "name_dob_hash"
    assert hash_column_for_keys(
        ["first_name_std", "last_name_std", "address1_std", "city", "state"]
    ) == "name_addr_city_state_hash"


def test_hash_column_for_keys_returns_none_for_unregistered_key_sets() -> None:
    """Single-key matchers (deterministic_external_id/deterministic_ssn)
    and any key combination not explicitly registered must return None —
    callers (matchers/deterministic.py) must fall back to plain
    column-by-column comparison rather than assume a hash column exists."""
    assert hash_column_for_keys(["ssn"]) is None
    assert hash_column_for_keys(["rilds_id"]) is None
    assert hash_column_for_keys(["some_other_key"]) is None


def test_hash_column_for_keys_is_order_sensitive() -> None:
    """The key list order IS the concatenation order in the hash formula —
    a differently-ordered key list is a DIFFERENT hash (or no registered
    hash at all), never silently treated as equivalent."""
    assert hash_column_for_keys(["last_name_std", "first_name_std", "birth_date"]) is None


def test_deterministic_hash_sql_normalizes_string_keys() -> None:
    """String-typed keys must be TRIM(UPPER(...))'d before concatenation —
    mirrors matchers/deterministic.py's _norm_sql exactly, so the hash can
    never disagree with what a direct column join would have compared."""
    sql = deterministic_hash_sql(
        ["first_name_std", "last_name_std"],
        {"first_name_std": "first_name_std", "last_name_std": "last_name_std"},
    )
    assert "TRIM(UPPER(first_name_std::VARCHAR))" in sql
    assert "TRIM(UPPER(last_name_std::VARCHAR))" in sql


def test_deterministic_hash_sql_leaves_birth_date_untouched() -> None:
    """birth_date is DATE-typed on both stage and reference — must NOT be
    upper/trimmed (that would be meaningless for a date), only cast to
    VARCHAR for concatenation into the hash input string."""
    sql = deterministic_hash_sql(
        ["first_name_std", "birth_date"],
        {"first_name_std": "first_name_std", "birth_date": "birth_date"},
    )
    assert "birth_date::VARCHAR" in sql
    assert "TRIM(UPPER(birth_date" not in sql


def test_deterministic_hash_sql_concatenates_in_key_order_with_pipe_delimiter() -> None:
    sql = deterministic_hash_sql(
        ["first_name_std", "last_name_std", "birth_date"],
        {"first_name_std": "first_name_std", "last_name_std": "last_name_std", "birth_date": "birth_date"},
    )
    # first_name_std's expression must appear before last_name_std's, which
    # must appear before birth_date's, joined by " || '|' || ".
    first_idx = sql.index("first_name_std")
    last_idx = sql.index("last_name_std")
    birth_idx = sql.index("birth_date")
    assert first_idx < last_idx < birth_idx
    assert " || '|' || " in sql


def test_deterministic_hash_sql_guards_every_key_null_and_blank() -> None:
    """The whole hash must be NULL if ANY key is missing/blank — never hash
    a partially-missing key (two rows both missing the same field must not
    collide into a false match)."""
    sql = deterministic_hash_sql(
        ["first_name_std", "last_name_std", "birth_date"],
        {"first_name_std": "first_name_std", "last_name_std": "last_name_std", "birth_date": "birth_date"},
    )
    assert sql.startswith("IFF(")
    assert "first_name_std IS NOT NULL AND TRIM(first_name_std::VARCHAR) != ''" in sql
    assert "last_name_std IS NOT NULL AND TRIM(last_name_std::VARCHAR) != ''" in sql
    # birth_date is a non-string key: NULL-check only, no blank/trim check
    # (mirrors matchers/deterministic.py's _blank_check_sql for date keys).
    assert "birth_date IS NOT NULL" in sql
    assert "TRIM(birth_date" not in sql
    assert ", NULL)" in sql  # the IFF's else-branch


def test_deterministic_hash_sql_uses_sha2_256() -> None:
    sql = deterministic_hash_sql(
        ["first_name_std", "last_name_std"],
        {"first_name_std": "first_name_std", "last_name_std": "last_name_std"},
    )
    assert ", 256)" in sql
    assert sql.count("SHA2(") == 1


def test_deterministic_hash_sql_respects_column_ref_overrides() -> None:
    """column_refs lets a caller supply a different actual column reference
    than the bare key name (e.g. an alias-qualified reference) — the hash
    formula must read from whatever column_refs says, not assume key ==
    column name."""
    sql = deterministic_hash_sql(
        ["first_name_std"], {"first_name_std": "s.first_name_std"}
    )
    assert "s.first_name_std" in sql
