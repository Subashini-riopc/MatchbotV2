"""Export rilds_reference from Postgres to a CSV for one-time load into
Snowflake's RILDS_REFERENCE table.

The reverse of scripts/seed_rilds_reference.py — reads the SAME table that
script writes, using the SAME Settings/DATABASE_URL/DB_SCHEMA env vars as
every other matchbot entrypoint, so this works against whichever Postgres
(local or the AWS demo's RDS) already has rilds_reference populated:

    DATABASE_URL=postgresql://... DB_SCHEMA=rilds \\
        uv run python -m matchbot_snowflake.export.export_rilds_reference

This is a ONE-TIME export, not a live sync (see
docs/snowflake-implementation-plan.md's "Reference data" scope note) — both
demos are meant to compare against an identical, frozen snapshot of people,
not diverge over time. Re-run manually if you want to refresh the
snapshot.

The output CSV's column order matches snowflake/ddl/05_reference_table.sql
exactly, so the Snowflake-side load can be a plain
``COPY INTO RILDS_REFERENCE FROM @stage/rilds_reference_export.csv``
with no column remapping. Blank values are written as empty fields (not
the literal string "NULL") since Snowflake's NULL_IF file-format option
(see snowflake/ddl/02_file_format_and_stage.sql, which also accepts '')
already treats empty CSV fields as NULL — matching how
scripts/seed_rilds_reference.py's own reader treats "" as NULL on the way
back in.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

from matchbot.config.settings import get_settings
from matchbot.storage.schema import build_metadata, search_path_sql
from sqlalchemy import create_engine, select

# Column order matches snowflake/ddl/05_reference_table.sql exactly.
REFERENCE_COLUMNS = [
    "idcol_id", "person_id", "dataset_id",
    "first_name", "middle_name", "last_name", "birth_date", "gender", "ssn",
    "apprentice_id", "brown_id", "bryant_id", "ccri_id", "college_board_id",
    "dcyf_id", "dlt_ern", "employri_id", "ged_id", "jwu_id", "kidsnet_child_id",
    "laces_id", "laces_staff_id", "laces_student_id", "lasid", "nspid", "ods",
    "ric_id", "ride_cert_id", "ridoh_lead_id", "risd_id", "rjri_id", "rwu_id",
    "salve_id", "sasid", "uri_id", "voter_id", "workforce_id",
    "providencecollege_id", "netech_id",
    "first_name_std", "first_name_metaphone1", "first_name_metaphone2",
    "first_name_transposed", "first_initial", "middle_name_std", "middle_initial",
    "last_name_std", "last_name_metaphone1", "last_name_metaphone2",
    "last_name_transposed", "last_name_suffix", "last_initial", "last_name8",
    "full_name_std", "full_name_metaphone", "full_name_transposed", "full_name_dob",
    "birth_month", "birth_day", "birth_year", "ssn4",
    "address_source", "address1", "address2", "city", "state", "zip",
]


def _normalize_url(url: str) -> str:
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


def export_rilds_reference(output_csv: Path) -> int:
    """Query Postgres's rilds_reference and write it to output_csv.

    Returns the number of rows written.
    """
    settings = get_settings()
    md = build_metadata(settings.db_schema)
    table = next(t for t in md.tables.values() if t.name == "rilds_reference")
    engine = create_engine(_normalize_url(settings.database_url), pool_pre_ping=True, future=True)

    output_csv.parent.mkdir(parents=True, exist_ok=True)

    row_count = 0
    with engine.connect() as conn:
        conn.execute(search_path_sql(settings.db_schema))
        result = conn.execute(select(table))
        with output_csv.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=REFERENCE_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            for row in result.mappings():
                # None -> '' so Snowflake's NULL_IF('', 'NULL') file format
                # (see 02_file_format_and_stage.sql) reads it back as NULL,
                # mirroring how seed_rilds_reference.py treats '' as NULL.
                writer.writerow({k: ("" if v is None else v) for k, v in row.items()})
                row_count += 1

    return row_count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("snowflake/data/rilds_reference_export/rilds_reference.csv"),
        help="Output CSV path (default: snowflake/data/rilds_reference_export/rilds_reference.csv).",
    )
    args = parser.parse_args()

    print(f"Exporting rilds_reference to {args.output} ...")
    count = export_rilds_reference(args.output)
    print(f"Done. Exported {count:,} rows.")
    print(
        "Next: upload this CSV to the Snowflake external stage's input "
        "prefix (or PUT it to an internal stage) and run:\n"
        "  COPY INTO RILDS_REFERENCE FROM @<stage>/rilds_reference.csv "
        "FILE_FORMAT = CSV_PROVIDER_FORMAT;"
    )


if __name__ == "__main__":
    main()
