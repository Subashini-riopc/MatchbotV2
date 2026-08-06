"""Unit tests for matching/derive.py's deterministic-matcher hash keys and
matching/deterministic.py's hash-based comparison / fallback behavior.

The hash formula here MUST stay byte-for-byte identical to the Snowflake
side's (snowflake/python/matchbot_snowflake/derive_sql.py's
deterministic_hash_sql/HASH_COLUMN_BY_KEYS) — see test_cross_platform_hash_parity
below for a literal cross-check against a hand-computed SHA-256 value.
"""

from __future__ import annotations

import hashlib

import polars as pl

from matchbot.config.models import MatcherSpec, StandardizationConfig
from matchbot.domain.enums import MatchDecision
from matchbot.matching.derive import HASH_KEY_SETS, add_derived_columns, add_hash_columns
from matchbot.matching.deterministic import DeterministicMatcher

STD_CONFIG = StandardizationConfig(
    gender_map={"F": "FEMALE", "M": "MALE"},
    name_suffixes=["JR", "SR"],
    name_prefixes=["MR", "DR"],
    address_abbreviations={"STREET": "ST"},
)


def test_hash_key_sets_covers_four_multi_key_matchers() -> None:
    """deterministic_fn_addr (fn_addr_hash) was removed from
    config/global.yaml as too loose a last-resort tier (no last name
    required), reintroduced as deterministic_firstname_addr_state_zip
    (firstname_addr_state_zip_hash), then removed again along with
    deterministic_lastname_addr_state_zip per explicit request — both
    carried real household-collision risk. name_addr_full_hash/
    name_addr_hash were dropped earlier for the same reasoning. Rule 6
    (deterministic_name_state_zip, name_state_zip_hash) was then changed
    to deterministic_name_addr_zip (name_addr_zip_hash) — state swapped
    for address1_std — leaving 4 multi-key matchers in the current
    6-rule chain."""
    assert len(HASH_KEY_SETS) == 4
    assert set(HASH_KEY_SETS.keys()) == {
        "name_ssn4_hash",
        "name_dob_hash",
        "name_addr_city_state_hash",
        "name_addr_zip_hash",
    }


def test_add_derived_columns_computes_hash_columns() -> None:
    df = pl.DataFrame({
        "first_name": ["Lisa"],
        "last_name": ["Aadal Ferrara"],
        "birth_date": ["1962-05-06"],
        "address1": ["170 Beechwood Street"],
        "city": ["CRANSTON"],
        "state": ["RI"],
        "zip": ["02921"],
    })
    out = add_derived_columns(df, STD_CONFIG)
    assert out["name_dob_hash"][0] is not None
    assert len(out["name_dob_hash"][0]) == 64  # SHA-256 hex digest


def test_cross_platform_hash_parity() -> None:
    """Hand-computed against the exact formula documented in both
    derive.py's HASH_KEY_SETS/_row_hash and derive_sql.py's
    deterministic_hash_sql — must match byte-for-byte, since this is what
    lets a Postgres-side hash and a Snowflake-side hash for the same
    logical record compare equal."""
    df = pl.DataFrame({
        "first_name": ["Lisa"],
        "last_name": ["Aadal Ferrara"],
        "birth_date": ["1962-05-06"],
    })
    out = add_derived_columns(df, STD_CONFIG)

    expected = hashlib.sha256("LISA|AADALFERRARA|1962-05-06".encode()).hexdigest()
    assert out["name_dob_hash"][0] == expected


def test_hash_is_null_when_any_key_missing() -> None:
    """No birth_date at all -> name_dob_hash must be None, not a hash of
    a partial key set."""
    df = pl.DataFrame({
        "first_name": ["Lisa"],
        "last_name": ["Aadal Ferrara"],
    })
    out = add_derived_columns(df, STD_CONFIG)
    assert out["name_dob_hash"][0] is None


def test_hash_is_null_when_key_present_but_blank() -> None:
    df = pl.DataFrame({
        "first_name": [""],
        "last_name": ["Aadal Ferrara"],
        "birth_date": ["1962-05-06"],
    })
    out = add_derived_columns(df, STD_CONFIG)
    assert out["name_dob_hash"][0] is None


def test_add_hash_columns_handles_missing_source_columns() -> None:
    """A frame with no address fields at all (e.g. RIDE, which never maps
    address1/city/state/zip) must get None for every address-based hash,
    not raise."""
    df = pl.DataFrame({"first_name": ["Lisa"], "last_name": ["Aadal Ferrara"]})
    out = add_hash_columns(df)
    assert out["name_addr_city_state_hash"][0] is None


def test_deterministic_matcher_uses_hash_when_record_has_it() -> None:
    spec = MatcherSpec(
        name="deterministic_name_dob", type="deterministic",
        keys=["first_name_std", "last_name_std", "birth_date"],
    )
    matcher = DeterministicMatcher(spec, STD_CONFIG)

    record = {
        "first_name_std": "LISA", "last_name_std": "AADALFERRARA",
        "birth_date": "1962-05-06", "name_dob_hash": "abc123",
    }
    candidates = [
        {"idcol_id": "1", "name_dob_hash": "xyz"},
        {"idcol_id": "2", "name_dob_hash": "abc123"},
    ]
    outcome = matcher.match(record, candidates)
    assert outcome.decision is MatchDecision.MATCHED
    assert outcome.idcol_id == "2"


def test_deterministic_matcher_falls_back_when_hash_key_absent() -> None:
    """A record with NO name_dob_hash key at all (e.g. a hand-built test
    fixture, or any caller that skips add_derived_columns) must still
    match correctly via the original per-key comparison — this is the
    regression this test guards: an earlier version of the hash-path logic
    treated a missing hash key the same as a hash value of None, which
    incorrectly returned NO_MATCH instead of falling back."""
    spec = MatcherSpec(
        name="deterministic_name_dob", type="deterministic",
        keys=["first_name_std", "last_name_std", "birth_date"],
    )
    matcher = DeterministicMatcher(spec, STD_CONFIG)

    record = {
        "first_name_std": "LISA", "last_name_std": "AADALFERRARA",
        "birth_date": "1962-05-06",
    }
    candidates = [{
        "idcol_id": "2",
        "first_name_std": "LISA", "last_name_std": "AADALFERRARA",
        "birth_date": "1962-05-06",
    }]
    outcome = matcher.match(record, candidates)
    assert outcome.decision is MatchDecision.MATCHED
    assert outcome.idcol_id == "2"


def test_deterministic_matcher_hash_none_is_real_no_match() -> None:
    """A record that DOES carry the hash key, but with value None (a
    genuinely missing/blank underlying field, per add_hash_columns), must
    be NO_MATCH — this is different from the key being absent entirely."""
    spec = MatcherSpec(
        name="deterministic_name_dob", type="deterministic",
        keys=["first_name_std", "last_name_std", "birth_date"],
    )
    matcher = DeterministicMatcher(spec, STD_CONFIG)

    record = {"first_name_std": "LISA", "last_name_std": "AADALFERRARA", "name_dob_hash": None}
    candidates = [{"idcol_id": "2", "name_dob_hash": None}]
    outcome = matcher.match(record, candidates)
    assert outcome.decision is MatchDecision.UNMATCHED


def test_single_key_matcher_never_uses_hash_path() -> None:
    """deterministic_ssn (1 key) has no registered hash column — must
    always use the per-key path, even if the record happens to carry a
    key named after some hash column by coincidence."""
    spec = MatcherSpec(name="deterministic_ssn", type="deterministic", keys=["ssn"])
    matcher = DeterministicMatcher(spec, STD_CONFIG)
    assert matcher._hash_column is None

    record = {"ssn": "123456789"}
    candidates = [{"idcol_id": "1", "ssn": "123456789"}]
    outcome = matcher.match(record, candidates)
    assert outcome.decision is MatchDecision.MATCHED
