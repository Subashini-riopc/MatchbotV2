"""Unit tests for CanonicalStage's combined_columns handling.

Covers CombinedColumnSpec (config/models.py): concatenating multiple raw
source columns into one canonical attribute, e.g. RISOS's STREET_NUMBER +
STREET_NAME into address1 — a real fix for a bare house number otherwise
being the only thing every address-anchored matcher compared (see
provider_risos.yaml's combined_columns comment).
"""

from __future__ import annotations

import polars as pl

from matchbot.config.models import CombinedColumnSpec, ProviderConfig
from matchbot.domain.enums import FileFormat
from matchbot.pipeline.canonical import CanonicalStage


def _risos_like_provider(**overrides) -> ProviderConfig:
    defaults = dict(
        provider_id="risos_voter",
        display_name="RISOS",
        format=FileFormat.CSV,
        file_glob="Voter_*.txt",
        column_mappings={"FIRST_NAME": "first_name", "VOTER_ID": "member_external_id"},
        combined_columns={
            "address1": CombinedColumnSpec(from_columns=["STREET_NUMBER", "STREET_NAME"]),
        },
    )
    defaults.update(overrides)
    return ProviderConfig(**defaults)


class _FakeCtx:
    def __init__(self, provider: ProviderConfig) -> None:
        self.provider = provider


def test_combined_columns_concatenates_with_default_space_separator() -> None:
    provider = _risos_like_provider()
    frame = pl.DataFrame({
        "FIRST_NAME": ["JOHN"],
        "VOTER_ID": ["12345"],
        "STREET_NUMBER": ["30"],
        "STREET_NAME": ["COLLYER ST"],
    })
    result = CanonicalStage().run(_FakeCtx(provider), frame)
    assert result.frame["address1"][0] == "30 COLLYER ST"


def test_combined_columns_respects_custom_separator() -> None:
    provider = _risos_like_provider(
        combined_columns={
            "address1": CombinedColumnSpec(
                from_columns=["STREET_NUMBER", "STREET_NAME"], separator="-"
            ),
        }
    )
    frame = pl.DataFrame({
        "FIRST_NAME": ["JOHN"],
        "VOTER_ID": ["12345"],
        "STREET_NUMBER": ["30"],
        "STREET_NAME": ["COLLYER ST"],
    })
    result = CanonicalStage().run(_FakeCtx(provider), frame)
    assert result.frame["address1"][0] == "30-COLLYER ST"


def test_combined_columns_tolerates_one_missing_piece() -> None:
    """A row missing STREET_NAME entirely (column present, value null) still
    produces the piece it has, rather than a fully-null address1 — mirrors
    this stage's existing tolerant handling of missing raw columns."""
    provider = _risos_like_provider()
    frame = pl.DataFrame({
        "FIRST_NAME": ["JOHN"],
        "VOTER_ID": ["12345"],
        "STREET_NUMBER": ["30"],
        "STREET_NAME": [None],
    })
    result = CanonicalStage().run(_FakeCtx(provider), frame)
    assert result.frame["address1"][0] == "30"


def test_combined_columns_target_survives_the_keep_and_rename_steps() -> None:
    """address1 (built by combine, not column_mappings) must still be
    present in the final frame — the keep-only-canonical-attrs filter and
    the rename step must not accidentally drop or clobber it."""
    provider = _risos_like_provider()
    frame = pl.DataFrame({
        "FIRST_NAME": ["JOHN"],
        "VOTER_ID": ["12345"],
        "STREET_NUMBER": ["30"],
        "STREET_NAME": ["COLLYER ST"],
    })
    result = CanonicalStage().run(_FakeCtx(provider), frame)
    assert "address1" in result.frame.columns
    assert result.frame["first_name"][0] == "JOHN"


def test_address2_stays_null_when_only_address1_is_combined() -> None:
    """RISOS has no genuine second address line — address2 must be null,
    not accidentally populated from STREET_NAME or anything else."""
    provider = _risos_like_provider()
    frame = pl.DataFrame({
        "FIRST_NAME": ["JOHN"],
        "VOTER_ID": ["12345"],
        "STREET_NUMBER": ["30"],
        "STREET_NAME": ["COLLYER ST"],
    })
    result = CanonicalStage().run(_FakeCtx(provider), frame)
    assert result.frame["address2"][0] is None


def test_no_combined_columns_is_a_pure_noop() -> None:
    """A provider with no combined_columns at all (e.g. RIDE) must behave
    exactly as before this feature was added."""
    provider = ProviderConfig(
        provider_id="ride_enrollment",
        display_name="RIDE",
        format=FileFormat.CSV,
        file_glob="*.csv",
        column_mappings={"FIRSTNAME": "first_name"},
    )
    frame = pl.DataFrame({"FIRSTNAME": ["MARY"]})
    result = CanonicalStage().run(_FakeCtx(provider), frame)
    assert result.frame["first_name"][0] == "MARY"
