"""Dynamic, per-provider land-table creation and loading.

The Snowflake equivalent of storage/schema.py's build_land_table() /
storage/postgres.py's write_land(): given a provider code and the incoming
file's actual header row, creates (or reuses) a land table shaped to match
that file exactly, and loads it — with zero hardcoded column list, so a
new provider's file needs no new DDL or code, matching the AWS pipeline's
"any provider works with zero bespoke DDL" guarantee (build_land_table()'s
own docstring: "Built dynamically from the file's columns, so any provider
works with zero bespoke DDL").

An earlier iteration of this demo hand-wrote a fixed 36-column RIDE_LAND
table — that was a real regression from parity with the AWS pipeline,
caught when asked directly whether a new provider's file shape would
require new code. This module is the fix.

Also generalizes the ragged-row (field-count-mismatch) detection built
during live validation (see docs/snowflake-implementation-plan.md): the
expected field count is read from the header itself, not hardcoded to
RIDE's specific 36 — so the same quarantine logic works for any provider's
file shape. Rejects go into ONE shared RILDS_LAND_REJECTS table (mirrors
Postgres's single shared rilds_land_rejects), not a per-provider table —
only the land table itself is per-provider.

Ragged-row detection and loading both read the file through
CSV_PROVIDER_FORMAT (quote-aware: FIELD_OPTIONALLY_ENCLOSED_BY = '"'),
using $1..$N positional columns — NOT the raw single-column format with
manual comma-counting an earlier version of this module used. That manual
approach (LENGTH(line) - LENGTH(REPLACE(line, ',', '')) + 1) can't tell a
genuinely shifted row apart from a properly quoted field containing a
comma (e.g. "WEST HILLS COLLEGE, LEMOORE") — it counted the quoted comma
as a real delimiter and flagged every such row as ragged, even though
Snowflake's real CSV parser (confirmed live) correctly keeps it as one
field. Caught when a freshly generated synthetic RIDE file — whose
college-name pool legitimately includes comma-containing names — landed
0 of 1000 rows, all rejected as "ragged," even though every row was
well-formed. A genuinely ragged row (too many real fields) still shows up
here as a non-NULL value in the column immediately past the expected
count ($(N+1)) — confirmed live against a hand-crafted 3-column test file
(too-few -> trailing $N is NULL; correct -> exact fit; too-many -> $(N+1)
is populated) — so that check still works, just computed off the real
parse instead of a naive character count.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session

# Provenance columns every land table carries, same set build_land_table()
# reserves — a source column with one of these names is skipped rather
# than colliding. Uppercase, matching _sanitize_column_name()'s output
# (parse_header_columns compares sanitized candidates against this set).
_RESERVED_COLUMNS = frozenset({"ID", "PIPELINE_RUN_ID", "SOURCE_ROW_ID", "CREATED_AT"})

_NON_IDENTIFIER_CHARS_RE = re.compile(r"[^A-Z0-9_]")

REJECTS_TABLE = "RILDS_LAND_REJECTS"

# Header parsing alone still uses a plain single-column raw-line format
# (FIELD_DELIMITER = NONE): a file's header rarely contains a quoted comma,
# and header_columns must be known before the real per-column SELECT below
# can be built. Reading the header requires seeing it (SKIP_HEADER=0);
# reading data rows requires skipping it (SKIP_HEADER=1) — see
# snowflake/ddl/02_file_format_and_stage.sql for both CREATE FILE FORMAT
# statements.
HEADER_LINE_FORMAT = "RAW_LINE_FORMAT_WITH_HEADER"
DATA_LINE_FORMAT = "RAW_LINE_FORMAT_SKIP_HEADER"

# Real, quote-aware CSV parsing (FIELD_OPTIONALLY_ENCLOSED_BY = '"') for
# ragged-row detection and the actual land-table load — see this module's
# docstring for why the raw single-column format's manual comma-counting
# can't be used for either of those two steps.
DATA_CSV_FORMAT = "MATCHBOT_CSV_PROVIDER_FORMAT"


def land_table_name(provider_code: str) -> str:
    """The per-provider land table name, e.g. 'ride' -> 'RIDE_LAND'."""
    return f"{provider_code.upper()}_LAND"


def _sanitize_column_name(raw: str) -> str:
    """Turn one raw header token into a safe, unquoted Snowflake identifier.

    Mirrors build_land_table()'s ``raw.strip().lower()`` — uppercased
    instead, since this codebase's generated SQL treats land columns as
    unquoted (see provider_sql.py's land.<COLUMN> reference — a
    quoted-lowercase reference doesn't match Snowflake's default
    uppercase-folded unquoted identifiers, a bug caught during live
    validation).
    """
    name = raw.strip().upper()
    name = _NON_IDENTIFIER_CHARS_RE.sub("_", name)
    return name


def parse_header_columns(header_line: str, delimiter: str = ",") -> list[str]:
    """Split a raw header line into sanitized, deduplicated column names.

    Reserved provenance names and empty tokens are dropped, same as
    build_land_table()'s ``if not name or name in reserved: continue``.
    Duplicate header tokens are suffixed (_2, _3, ...) rather than
    silently colliding, since a raw file header is not guaranteed unique
    the way Python dict keys naturally are.
    """
    columns: list[str] = []
    seen: set[str] = set()
    for raw in header_line.split(delimiter):
        name = _sanitize_column_name(raw)
        if not name or name in _RESERVED_COLUMNS:
            continue
        candidate = name
        suffix = 2
        while candidate in seen:
            candidate = f"{name}_{suffix}"
            suffix += 1
        seen.add(candidate)
        columns.append(candidate)
    return columns


def fetch_header_columns(
    session: "Session", stage_file_path: str, raw_line_format: str = HEADER_LINE_FORMAT
) -> list[str]:
    """Read the real header row directly off the staged file and return its
    sanitized column names — the Snowflake-side source of the same
    ``source_columns`` build_land_table() receives from ParseStage.

    HEADER_LINE_FORMAT (RAW_LINE_FORMAT_WITH_HEADER) has SKIP_HEADER=0, so
    the header row itself is readable here as plain text — the opposite of
    CSV_PROVIDER_FORMAT and DATA_LINE_FORMAT, which both skip it.
    """
    result = session.sql(
        f"""
        SELECT $1 AS header_line
        FROM @{stage_file_path}
            (FILE_FORMAT => '{raw_line_format}')
        LIMIT 1
        """
    ).collect()
    if not result:
        raise ValueError(f"Could not read header row from {stage_file_path}")
    return parse_header_columns(result[0]["HEADER_LINE"])


def render_create_land_table_sql(provider_code: str, header_columns: list[str]) -> str:
    """CREATE TABLE IF NOT EXISTS <PROVIDER>_LAND (...), one VARCHAR column
    per header column, in source order — mirrors build_land_table() exactly:
    provenance columns first/last, every source column stored as raw text.
    """
    table_name = land_table_name(provider_code)
    column_lines = ",\n    ".join(f"{col} VARCHAR" for col in header_columns)
    return f"""CREATE TABLE IF NOT EXISTS {table_name} (
    id                NUMBER IDENTITY PRIMARY KEY,
    pipeline_run_id   NUMBER NOT NULL,
    source_row_id     NUMBER NOT NULL,
    {column_lines},
    created_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)"""


def render_reject_ragged_rows_sql(
    provider_code: str,
    pipeline_run_id: int,
    stage_file_path: str,
    expected_field_count: int,
    csv_format: str = DATA_CSV_FORMAT,
) -> str:
    """INSERT INTO RILDS_LAND_REJECTS (shared across all providers), reading
    straight off the stage — no intermediate table. The field-count check
    is generic: driven by expected_field_count computed from the file's own
    header, not hardcoded per provider.

    Reads via CSV_PROVIDER_FORMAT ($1..$(N+1), quote-aware), not the raw
    single-column format — a row is genuinely ragged only if data actually
    spills into the column PAST the expected count ($(N+1) IS NOT NULL);
    a quoted comma inside one field (e.g. a college name) is already merged
    into a single $i by Snowflake's real parser and never reaches here. See
    this module's docstring for the live-confirmed behavior this relies on.
    raw_line is reconstructed by rejoining the parsed fields with commas —
    not guaranteed byte-identical to the original line (e.g. original
    quoting isn't preserved) but sufficient for DQ investigation.
    """
    field_refs = [f"${i}" for i in range(1, expected_field_count + 2)]
    raw_line_expr = f"ARRAY_TO_STRING(ARRAY_CONSTRUCT({', '.join(field_refs)}), ',')"
    overflow_col = f"${expected_field_count + 1}"
    return f"""INSERT INTO {REJECTS_TABLE} (pipeline_run_id, provider_code, raw_line, reason)
SELECT
    {pipeline_run_id},
    '{provider_code}',
    {raw_line_expr},
    'field count mismatch: expected {expected_field_count}, got more'
FROM @{stage_file_path}
    (FILE_FORMAT => '{csv_format}')
WHERE {overflow_col} IS NOT NULL"""


def render_load_clean_rows_sql(
    provider_code: str,
    pipeline_run_id: int,
    stage_file_path: str,
    header_columns: list[str],
    csv_format: str = DATA_CSV_FORMAT,
) -> str:
    """INSERT INTO <PROVIDER>_LAND, reading straight off the stage via
    CSV_PROVIDER_FORMAT's real, quote-aware parsing ($1..$N positional
    columns) — column count and target table both driven entirely by
    header_columns, never hardcoded.

    Excludes rows where data spills past the expected column count (see
    render_reject_ragged_rows_sql) — those already went to
    RILDS_LAND_REJECTS instead.
    """
    land_table = land_table_name(provider_code)
    expected_field_count = len(header_columns)
    column_list = ", ".join(header_columns)
    positional_columns = ",\n    ".join(f"${i}" for i in range(1, expected_field_count + 1))
    overflow_col = f"${expected_field_count + 1}"
    return f"""INSERT INTO {land_table} (
    pipeline_run_id, source_row_id, {column_list}
)
SELECT
    {pipeline_run_id},
    METADATA$FILE_ROW_NUMBER,
    {positional_columns}
FROM @{stage_file_path}
    (FILE_FORMAT => '{csv_format}')
WHERE {overflow_col} IS NULL"""
