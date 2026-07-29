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
#
# Comma remains the default (matches RIDE), but this is NOT the only shape
# in use: RISOS's file is pipe-delimited, so its file format differs (see
# PIPE_PROVIDER_FORMAT in snowflake/ddl/02_file_format_and_stage.sql).
# csv_format_for_delimiter() below picks the right one per provider — the
# caller (run_pipeline.py) passes provider.delimiter through rather than
# relying on this default for every provider.
DATA_CSV_FORMAT = "MATCHBOT_CSV_PROVIDER_FORMAT"

# Maps a provider's configured delimiter (ProviderConfig.delimiter) to the
# Snowflake file format object shaped for it. Add an entry here (and a
# matching CREATE FILE FORMAT in the DDL) when a new provider uses a
# delimiter neither of these covers.
_DELIMITER_TO_CSV_FORMAT = {
    ",": "MATCHBOT_CSV_PROVIDER_FORMAT",
    "|": "MATCHBOT_PIPE_PROVIDER_FORMAT",
}


def csv_format_for_delimiter(delimiter: str) -> str:
    """The Snowflake file format object name for a provider's delimiter.

    Raises on an unconfigured delimiter rather than silently falling back to
    comma — a wrong-but-valid file format would still "succeed" while
    mis-splitting every row (the exact failure mode this function exists to
    prevent for a provider like RISOS).
    """
    try:
        return _DELIMITER_TO_CSV_FORMAT[delimiter]
    except KeyError:
        raise ValueError(
            f"no Snowflake file format configured for delimiter {delimiter!r}; "
            f"add one to _DELIMITER_TO_CSV_FORMAT and snowflake/ddl/02_file_format_and_stage.sql"
        ) from None


# Matches ONE trailing date/period/sequence segment on a file stem, e.g.
# "_032026", "_2026-07-09", "-2026-07-09", "_20260709". Applied repeatedly
# (not just once) since a stem can have more than one such segment
# (ride_enrollment_2026-07-09 has a single hyphenated date segment here,
# but a future file could plausibly have "_v2_2026-07-09" etc.) — see
# file_type_from_filename's loop.
_TRAILING_DATE_OR_NUMBER_RE = re.compile(r"[_-][0-9]{2,}([_-][0-9]{2,})*$")


def file_type_from_filename(filename: str) -> str:
    """The file-type token used in the land table name, e.g.
    'Voter_032026.txt' -> 'VOTER', 'VoterHistory_032026.txt' -> 'VOTERHISTORY',
    'ride_enrollment_2026-07-09.csv' -> 'RIDE_ENROLLMENT'.

    A single provider can ship several structurally different files (RISOS
    sends voter registration and voter history separately, with unrelated
    columns) — one table per PROVIDER alone would mean the second file type
    either collides with the first table's shape or silently no-ops against
    it (CREATE TABLE IF NOT EXISTS does nothing once a table of that name
    already exists, so the differently-shaped file's INSERT then fails on
    missing columns — the exact bug this function fixes). Stripping the
    trailing date/sequence run (e.g. _032026, _2026-07-09) keeps the table
    name stable across a provider's routine periodic drops of the same file
    type, without needing a config entry per file.
    """
    stem = filename.rsplit("/", 1)[-1]
    stem = re.sub(r"\.[A-Za-z0-9]+$", "", stem)  # drop extension
    stem = _TRAILING_DATE_OR_NUMBER_RE.sub("", stem)
    return _sanitize_column_name(stem)


def land_table_name(provider_code: str, file_type: str | None = None) -> str:
    """The land table name for a provider (+ optional file type).

    'ride' -> 'RIDE_LAND' (single file type, back-compat: RIDE_LAND already
    exists in deployed accounts under this name).
    ('risos', 'VOTER') -> 'RISOS_VOTER_LAND'; ('risos', 'VOTERHISTORY') ->
    'RISOS_VOTERHISTORY_LAND' — distinct tables per file shape, all still
    prefixed with the provider code so they're identifiable as RISOS's.
    """
    if file_type:
        return f"{provider_code.upper()}_{file_type.upper()}_LAND"
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
    session: "Session",
    stage_file_path: str,
    raw_line_format: str = HEADER_LINE_FORMAT,
    delimiter: str = ",",
) -> list[str]:
    """Read the real header row directly off the staged file and return its
    sanitized column names — the Snowflake-side source of the same
    ``source_columns`` build_land_table() receives from ParseStage.

    HEADER_LINE_FORMAT (RAW_LINE_FORMAT_WITH_HEADER) has SKIP_HEADER=0, so
    the header row itself is readable here as plain text — the opposite of
    CSV_PROVIDER_FORMAT and DATA_LINE_FORMAT, which both skip it. This raw
    line format's own FIELD_DELIMITER is NONE regardless of the provider
    (see snowflake/ddl/02_file_format_and_stage.sql), so it always returns
    the whole header line undivided — ``delimiter`` (ProviderConfig.delimiter,
    e.g. "|" for RISOS) is only used here, in Python, to split that line.
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
    return parse_header_columns(result[0]["HEADER_LINE"], delimiter=delimiter)


def render_create_land_table_sql(
    provider_code: str, header_columns: list[str], file_type: str | None = None
) -> str:
    """CREATE TABLE IF NOT EXISTS <PROVIDER>[_<FILE_TYPE>]_LAND (...), one
    VARCHAR column per header column, in source order — mirrors
    build_land_table() exactly: provenance columns first/last, every source
    column stored as raw text.

    IF NOT EXISTS only covers a table that has never been created before —
    it does NOT add columns to a same-named table created by an earlier,
    differently-shaped file (that's what caused RISOS's VoterHistory load to
    fail: RISOS_LAND already existed, shaped for the voter-registration
    file, so VoterHistory's INSERT referenced columns like DATE_1 that were
    never in that table). Callers must pair this with
    render_add_missing_columns_sql for a table that might already exist in
    a different shape — see run_pipeline.py.
    """
    table_name = land_table_name(provider_code, file_type)
    column_lines = ",\n    ".join(f"{col} VARCHAR" for col in header_columns)
    return f"""CREATE TABLE IF NOT EXISTS {table_name} (
    id                NUMBER IDENTITY PRIMARY KEY,
    pipeline_run_id   NUMBER NOT NULL,
    source_row_id     NUMBER NOT NULL,
    {column_lines},
    created_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)"""


def render_add_missing_columns_sql(
    provider_code: str,
    header_columns: list[str],
    existing_columns: set[str],
    file_type: str | None = None,
) -> list[str]:
    """ALTER TABLE ... ADD COLUMN IF NOT EXISTS for any header column not
    already on the table — lets a file type's schema evolve (e.g. RISOS
    adds a new field to VoterHistory next year) without losing history:
    old rows simply have NULL in the newly added column. Never drops or
    renames a column, even if a later file omits one the table already
    has — that column just stays NULL for this run's rows, same as any
    other column this file's header didn't map.

    Returns a list of statements (one ALTER per missing column) rather than
    one combined ALTER ... ADD COLUMN (a, b, c) — simpler to reason about
    and matches how render_create_land_table_sql etc. are executed
    one-statement-at-a-time by run_pipeline.py. Empty list (no statements)
    when every header column already exists on the table.
    """
    table_name = land_table_name(provider_code, file_type)
    missing = [col for col in header_columns if col not in existing_columns]
    return [
        f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {col} VARCHAR"
        for col in missing
    ]


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
    file_type: str | None = None,
) -> str:
    """INSERT INTO <PROVIDER>[_<FILE_TYPE>]_LAND, reading straight off the
    stage via CSV_PROVIDER_FORMAT's real, quote-aware parsing ($1..$N
    positional columns) — column count and target table both driven
    entirely by header_columns, never hardcoded.

    Excludes rows where data spills past the expected column count (see
    render_reject_ragged_rows_sql) — those already went to
    RILDS_LAND_REJECTS instead.
    """
    land_table = land_table_name(provider_code, file_type)
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


def render_file_profile_sql(
    provider_code: str,
    pipeline_run_id: int,
    header_columns: list[str],
    file_type: str | None = None,
) -> str:
    """One row per header_columns entry: null/blank count for that column,
    scoped to this run's rows in the land table. The Snowflake-side
    equivalent of matchbot.pipeline.parse::_profile_file's null_counts —
    computed on the LAND table (this run's landed rows) rather than a
    Python DataFrame, since there's no in-memory frame at any point in
    this SQL-generation pipeline. A cell counts as null/blank under the
    same rule the AWS side uses: SQL NULL or an all-whitespace string.

    UNION ALL rather than one row with N columns: header_columns is
    provider-specific and arbitrary-length, so a fixed-shape result (one
    row, one column per header column) would require dynamic pivoting;
    a tall (column_name, null_count) shape needs no such thing and is
    just as easy to render in an email.
    """
    land_table = land_table_name(provider_code, file_type)
    per_column = "\nUNION ALL\n".join(
        f"SELECT '{col}' AS column_name, "
        f"COUNT_IF({col} IS NULL OR TRIM({col}) = '') AS null_count "
        f"FROM {land_table} WHERE pipeline_run_id = {pipeline_run_id}"
        for col in header_columns
    )
    return per_column


def render_duplicate_row_count_sql(
    provider_code: str,
    pipeline_run_id: int,
    header_columns: list[str],
    file_type: str | None = None,
) -> str:
    """Count of rows in this run's landed rows that are exact duplicates of
    another row on that same run — i.e. sum over (group size - 1) for
    every group of 2+ identical rows, matching parse.py::_profile_file's
    duplicate_row_count definition. Compares on header_columns only (not
    the id/pipeline_run_id/source_row_id/created_at provenance columns),
    since two source rows with identical data but different provenance
    are still "the same row as received" for this purpose.

    NOT implemented as COUNT(*) - COUNT(DISTINCT col1, col2, ...): Snowflake's
    multi-column COUNT(DISTINCT ...) silently excludes any row where ANY
    column is NULL from the distinct count entirely (confirmed live with a
    hand-built 4-row test: one row containing a NULL was dropped from
    COUNT(DISTINCT...) rather than counted as its own distinct value,
    corrupting the subtraction — a land table's NULLable text columns make
    this a real, not theoretical, risk). GROUP BY ... HAVING COUNT(*) > 1
    treats a NULL as an ordinary groupable value, matching how the AWS
    side's Polars-based duplicate check treats identical-including-null
    rows as duplicates of each other.
    """
    land_table = land_table_name(provider_code, file_type)
    column_list = ", ".join(header_columns)
    return f"""SELECT COALESCE(SUM(group_size - 1), 0)
FROM (
    SELECT COUNT(*) AS group_size
    FROM {land_table}
    WHERE pipeline_run_id = {pipeline_run_id}
    GROUP BY {column_list}
    HAVING COUNT(*) > 1
)"""
