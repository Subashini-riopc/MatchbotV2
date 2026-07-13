"""Guards against the bundled config copy (snowflake/python/matchbot_snowflake/
config/) silently drifting from the real config/ directory.

matchbot_snowflake/config/ is a physical COPY of config/, not a symlink or
build-time reference — required because Snowflake's multi-IMPORTS mechanism
extracts each declared zip to its own separate sys.path root rather than
merging them, so config/ has to be bundled inside the SAME zip as the
Python code (see snowflake.yml's artifacts comment for the full story).
That means nothing enforces the copy stays current automatically — re-run
the copy command below after any change to config/global.yaml or
config/providers/*.yaml, then re-deploy.

    cp -r config/* snowflake/python/matchbot_snowflake/config/
"""

from __future__ import annotations

from pathlib import Path

REAL_CONFIG_DIR = Path(__file__).resolve().parents[3] / "config"
BUNDLED_CONFIG_DIR = Path(__file__).resolve().parents[1] / "matchbot_snowflake" / "config"


def test_bundled_global_yaml_matches_real() -> None:
    real = (REAL_CONFIG_DIR / "global.yaml").read_text()
    bundled = (BUNDLED_CONFIG_DIR / "global.yaml").read_text()
    assert real == bundled, (
        "snowflake/python/matchbot_snowflake/config/global.yaml is out of sync "
        "with config/global.yaml — re-copy and re-deploy"
    )


def test_bundled_provider_files_match_real() -> None:
    real_providers = sorted((REAL_CONFIG_DIR / "providers").glob("*.yaml"))
    bundled_providers = sorted((BUNDLED_CONFIG_DIR / "providers").glob("*.yaml"))

    real_names = [p.name for p in real_providers]
    bundled_names = [p.name for p in bundled_providers]
    assert real_names == bundled_names, (
        "Bundled config/providers/ has a different file set than the real "
        "config/providers/ — re-copy and re-deploy"
    )

    for real_path, bundled_path in zip(real_providers, bundled_providers):
        assert real_path.read_text() == bundled_path.read_text(), (
            f"{real_path.name} is out of sync between config/providers/ and "
            "the bundled copy — re-copy and re-deploy"
        )
