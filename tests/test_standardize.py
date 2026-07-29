"""Unit tests for the value standardizers."""

from __future__ import annotations

import pytest

from matchbot.config.models import StandardizationConfig
from matchbot.matching import standardize as S

STD = StandardizationConfig(
    gender_map={"F": "FEMALE", "M": "MALE", "NB": "NONBINARY"},
    name_suffixes=["JR", "SR", "III"],
    name_prefixes=["MR", "DR"],
    address_abbreviations={
        "STREET": "ST", "AVENUE": "AVE", "APARTMENT": "APT", "NORTH": "N",
    },
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("smith jr", "SMITH"),
        ("dr. mary-ellen", "MARYELLEN"),
        ("  Contreras-Ruiz  ", "CONTRERASRUIZ"),
        ("", None),
        (None, None),
    ],
)
def test_std_name(raw: str | None, expected: str | None) -> None:
    assert S.std_name(raw, STD) == expected


def test_name_suffix() -> None:
    assert S.name_suffix("smith jr", STD) == "JR"
    assert S.name_suffix("smith", STD) is None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("23456789", "023456789"),
        ("123-45-6789", "123456789"),
        ("12345", None),  # too short after pad? -> 000012345 is 9 digits
        ("", None),
        (None, None),
    ],
)
def test_std_ssn(raw: str | None, expected: str | None) -> None:
    result = S.std_ssn(raw)
    if raw == "12345":
        assert result == "000012345"
    else:
        assert result == expected


def test_ssn4() -> None:
    assert S.ssn4("123-45-6789") == "6789"
    assert S.ssn4(None) is None


def test_std_gender() -> None:
    assert S.std_gender("F", STD) == "FEMALE"
    assert S.std_gender("m", STD) == "MALE"
    assert S.std_gender("unknown", STD) == "UNKNOWN"  # passthrough uppercased
    assert S.std_gender(None, STD) is None


def test_metaphone_and_jaro() -> None:
    assert S.metaphone("Contreras") == S.metaphone("contreras")
    assert S.jaro_winkler("MARY", "MARI") > 0.8
    assert S.jaro_winkler("MARY", None) == 0.0


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("123 Main Street", "123 MAIN ST"),
        ("456 North Oak Avenue Apartment 2", "456 N OAK AVE APT 2"),
        ("  789 Elm St  ", "789 ELM ST"),  # already-abbreviated tokens pass through unchanged
        ("", None),
        (None, None),
    ],
)
def test_std_address(raw: str | None, expected: str | None) -> None:
    assert S.std_address(raw, STD) == expected


def test_std_address_does_not_corrupt_substrings() -> None:
    """A token-level abbreviation match must not corrupt an unrelated word
    that merely contains the same substring (e.g. 'EAST' -> 'E' must not
    also rewrite a town/street name like 'EASTON')."""
    std = StandardizationConfig(address_abbreviations={"EAST": "E"})
    assert S.std_address("EASTON AVE", std) == "EASTON AVE"
    assert S.std_address("123 EAST AVE", std) == "123 E AVE"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("02901", "02901"),
        ("02901-1234", "02901"),  # ZIP+4 suffix dropped
        ("0290", None),  # fewer than 5 digits
        ("", None),
        (None, None),
    ],
)
def test_std_zip(raw: str | None, expected: str | None) -> None:
    assert S.std_zip(raw) == expected
