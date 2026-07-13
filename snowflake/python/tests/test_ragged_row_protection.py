"""Regression test for a real data-corruption bug caught during build-step-3
live validation (see docs/snowflake-implementation-plan.md).

15 of 1000 real RIDE rows have an unescaped comma inside the college name
(e.g. "University of Maine, Farmington"), shifting every column after it.
Without ERROR_ON_COLUMN_COUNT_MISMATCH on the file format, Snowflake's
COPY INTO silently accepted these shifted rows — one such row's SEX column
ended up holding the district name "South Kingstown" instead of 'F'/'M',
which then failed downstream with 'String ... is too long' once that value
reached a narrower column. This is exactly the failure mode
storage/schema.py's rilds_land_rejects comment describes for the AWS/
Postgres pipeline's ParseStage, which pre-validates field counts for the
same reason.

This test only checks the DDL text for the required setting — it doesn't
execute against a live warehouse (no connection available in CI).
"""

from __future__ import annotations

from pathlib import Path

DDL_PATH = (
    Path(__file__).resolve().parents[3]
    / "snowflake"
    / "ddl"
    / "02_file_format_and_stage.sql"
)


def test_file_format_rejects_ragged_rows() -> None:
    sql = DDL_PATH.read_text()
    assert "ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE" in sql, (
        "CSV_PROVIDER_FORMAT must reject rows whose field count doesn't match "
        "the header, or an unescaped comma inside a field (e.g. a college name) "
        "silently shifts every later column instead of being rejected — see "
        "module docstring for the real incident this guards against"
    )
