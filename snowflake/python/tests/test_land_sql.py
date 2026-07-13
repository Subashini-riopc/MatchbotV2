"""Unit tests for land_sql.py — dynamic, header-driven land-table creation.

Verifies the land step is genuinely provider-agnostic: given a new
provider's file header, no code changes are needed, only the header text
differs. This corrects an earlier regression from the AWS pipeline's
build_land_table() behavior, where RIDE_LAND was hand-written with a fixed
36-column shape instead of derived at runtime — caught when asked directly
whether a new provider's file shape would require new code.
"""

from __future__ import annotations

from matchbot_snowflake.land_sql import (
    REJECTS_TABLE,
    land_table_name,
    parse_header_columns,
    render_create_land_table_sql,
    render_load_clean_rows_sql,
    render_reject_ragged_rows_sql,
)

RIDE_HEADER = (
    "RECID,FIRSTNAME,MIDDLENAME,LASTNAME,NAMESUFFIX,SASID,RECORDFOUND,SEARCHDATE,"
    "COLLEGECODE,COLLEGENAME,COLLEGESTATE,COLLEGETYPE,PUBLICPRIVATE,ENROLLBEGIN,"
    "ENROLLEND,ENROLLSTATUS,GRADUATED,GRADUATIONDATE,DEGREETITLE,MAJOR,COLLEGESEQ,"
    "CREDIT,ENROLLFIRST,ENROLLLAST,HSGRADDATE,HSEXITTYPE,HSCODE,HSNAME,DISTCODE,"
    "DISTNAME,SEX,RACE,RACE7,LUNCH,IEP,LEP"
)


def test_land_table_name_is_provider_specific() -> None:
    assert land_table_name("ride") == "RIDE_LAND"
    assert land_table_name("dcyf") == "DCYF_LAND"


def test_rejects_table_is_shared_not_per_provider() -> None:
    """One table for all providers, mirroring Postgres's single shared
    rilds_land_rejects — not <PROVIDER>_LAND_REJECTS."""
    assert REJECTS_TABLE == "RILDS_LAND_REJECTS"


def test_parse_header_columns_matches_real_ride_header() -> None:
    columns = parse_header_columns(RIDE_HEADER)
    assert len(columns) == 36
    assert columns[0] == "RECID"
    assert columns[1] == "FIRSTNAME"
    assert columns[-1] == "LEP"


def test_parse_header_columns_drops_reserved_names() -> None:
    """A raw header token colliding with a provenance column name (e.g. a
    provider file that happens to have a column literally called 'id') must
    not silently overwrite that provenance column."""
    header = "id,FIRSTNAME,pipeline_run_id,LASTNAME"
    columns = parse_header_columns(header)
    assert "ID" not in columns
    assert "PIPELINE_RUN_ID" not in columns
    assert columns == ["FIRSTNAME", "LASTNAME"]


def test_parse_header_columns_deduplicates() -> None:
    header = "NAME,NAME,NAME"
    columns = parse_header_columns(header)
    assert columns == ["NAME", "NAME_2", "NAME_3"]


def test_parse_header_columns_sanitizes_non_identifier_chars() -> None:
    header = "First Name,Last-Name,SSN#"
    columns = parse_header_columns(header)
    assert columns == ["FIRST_NAME", "LAST_NAME", "SSN_"]


def test_create_table_sql_has_one_column_per_header_token() -> None:
    columns = parse_header_columns(RIDE_HEADER)
    sql = render_create_land_table_sql("ride", columns)
    assert "CREATE TABLE IF NOT EXISTS RIDE_LAND" in sql
    for col in columns:
        assert f"{col} VARCHAR" in sql


def test_create_table_sql_works_for_a_totally_different_provider_shape() -> None:
    """The whole point: a provider with a completely different file shape
    needs no new code, only different header text."""
    different_header = "STUDENT_ID,FNAME,LNAME,DOB,ENROLLMENT_DATE"
    columns = parse_header_columns(different_header)
    sql = render_create_land_table_sql("newprovider", columns)
    assert "CREATE TABLE IF NOT EXISTS NEWPROVIDER_LAND" in sql
    assert "STUDENT_ID VARCHAR" in sql
    assert "FNAME VARCHAR" in sql
    assert "DOB VARCHAR" in sql


def test_reject_sql_uses_header_derived_field_count_not_hardcoded() -> None:
    columns = parse_header_columns(RIDE_HEADER)
    sql = render_reject_ragged_rows_sql("ride", 1, "STAGE/file.csv", len(columns))
    assert "expected 36" in sql
    assert "!= 36" in sql
    assert "RILDS_LAND_REJECTS" in sql
    assert "'ride'" in sql


def test_load_clean_rows_sql_has_one_split_part_per_column() -> None:
    columns = parse_header_columns(RIDE_HEADER)
    sql = render_load_clean_rows_sql("ride", 1, "STAGE/file.csv", columns)
    assert sql.count("SPLIT_PART($1, ',',") == len(columns)
    assert "= 36" in sql
    assert "INSERT INTO RIDE_LAND" in sql
    # Column list order must match the SPLIT_PART order positionally.
    assert ", ".join(columns) in sql.replace("\n", " ").replace("    ", " ")


def test_load_and_reject_sql_use_same_field_count_threshold() -> None:
    """A row must be routed to exactly one of RIDE_LAND / RILDS_LAND_REJECTS
    — never both, never neither. This only holds if both queries use the
    identical expected-count boundary."""
    columns = parse_header_columns(RIDE_HEADER)
    reject_sql = render_reject_ragged_rows_sql("ride", 1, "STAGE/file.csv", len(columns))
    load_sql = render_load_clean_rows_sql("ride", 1, "STAGE/file.csv", columns)
    assert "!= 36" in reject_sql
    assert "= 36" in load_sql
