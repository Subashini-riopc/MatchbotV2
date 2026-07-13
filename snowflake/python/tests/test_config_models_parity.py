"""Parity test: matchbot_snowflake.config_models.load_config() must produce
byte-for-byte identical results to matchbot.config.loader.load_config() —
the real one, imported directly here (this test file, unlike run_pipeline.py,
runs on a normal dev machine with polars available, so importing the real
matchbot package is fine for testing purposes; only the Snowflake stored
procedure itself needs to avoid it).

config_models.py is a deliberate, hand-maintained duplicate (see its module
docstring for why) — this test is the actual guard against the two
silently drifting apart. Run this after any change to either file.
"""

from __future__ import annotations

from pathlib import Path

from matchbot.config.loader import load_config as real_load_config

from matchbot_snowflake.config_models import load_bundled_config
from matchbot_snowflake.config_models import load_config as snowflake_load_config

CONFIG_DIR = Path(__file__).resolve().parents[3] / "config"


def test_snowflake_config_matches_real_matchbot_config() -> None:
    real = real_load_config(CONFIG_DIR)
    mirrored = snowflake_load_config(CONFIG_DIR)

    # Compare as dicts (Pydantic model_dump) rather than object identity —
    # these are two separate classes by design, only their VALUES must match.
    assert real.model_dump() == mirrored.model_dump()


def test_ride_provider_matches_exactly() -> None:
    real = real_load_config(CONFIG_DIR).provider("ride_enrollment")
    mirrored = snowflake_load_config(CONFIG_DIR).provider("ride_enrollment")

    assert real.model_dump() == mirrored.model_dump()


def test_global_matcher_chain_matches_exactly() -> None:
    real = real_load_config(CONFIG_DIR).global_config.matching.matchers
    mirrored = snowflake_load_config(CONFIG_DIR).global_config.matching.matchers

    assert [m.model_dump() for m in real] == [m.model_dump() for m in mirrored]


def test_bundled_config_matches_real_config() -> None:
    """load_bundled_config() reads via importlib.resources from
    matchbot_snowflake/config/ (the physical copy — see
    test_bundled_config_in_sync.py) instead of a filesystem Path. Runs
    fine on a normal dev machine (importlib.resources works identically
    whether the package is on disk or zipped) — this is the local
    equivalent of what the deployed Snowflake procedure actually calls.
    """
    real = real_load_config(CONFIG_DIR)
    bundled = load_bundled_config()

    assert real.model_dump() == bundled.model_dump()
