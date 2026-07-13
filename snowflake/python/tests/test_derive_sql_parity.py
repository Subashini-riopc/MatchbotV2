"""Parity fixtures for derive_sql.py against the real Python std_name()/
metaphone()/std_gender() outputs.

This module intentionally does NOT execute the generated SQL against a live
Snowflake connection — it exists as the frozen "expected outputs" the
generated SQL must reproduce once step 3 of the build order (see
docs/snowflake-implementation-plan.md) can be run against a real warehouse.
Run manually against Snowflake with:

    SELECT <std_name_sql('raw_col', std_config)> FROM (SELECT 'Mary Jane Smith Jr.' AS raw_col);

and confirm the result matches the corresponding case below.
"""

from __future__ import annotations

from matchbot.config.models import StandardizationConfig
from matchbot.matching.standardize import metaphone, std_gender, std_name

STD_CONFIG = StandardizationConfig(
    gender_map={
        "F": "FEMALE",
        "FEMALE": "FEMALE",
        "M": "MALE",
        "MALE": "MALE",
        "NB": "NONBINARY",
        "X": "NONBINARY",
    },
    name_suffixes=["JR", "SR", "II", "III", "IV", "V", "VI", "ESQ"],
    name_prefixes=["MR", "MRS", "MS", "DR"],
)

# (input, expected std_name output) — frozen from the real Python function.
STD_NAME_CASES = [
    ("Mary Jane Smith Jr.", "MARYJANESMITH"),
    ("Dr. John O'Neil", "JOHNONEIL"),
    (None, None),
    ("   ", None),
    ("Smith", "SMITH"),
]

METAPHONE_CASES = [
    ("SMITH", "SM0"),
    ("JOHN", "JN"),
    (None, None),
]

STD_GENDER_CASES = [
    ("F", "FEMALE"),
    ("male", "MALE"),
    ("X", "NONBINARY"),
    ("unknown", "UNKNOWN"),  # not in map -> uppercased raw value, per std_gender()
    (None, None),
]


def test_std_name_fixture_matches_python() -> None:
    """Sanity check the fixture itself stays in sync with std_name()."""
    for value, expected in STD_NAME_CASES:
        assert std_name(value, STD_CONFIG) == expected


def test_metaphone_fixture_matches_python() -> None:
    for value, expected in METAPHONE_CASES:
        assert metaphone(value) == expected


def test_std_gender_fixture_matches_python() -> None:
    for value, expected in STD_GENDER_CASES:
        assert std_gender(value, STD_CONFIG) == expected
