"""The Snowpark stored procedure entrypoint: RILDS.RUN_MATCH_PIPELINE.

This is orchestration/SQL-generation glue, not row-by-row computation — it
assembles SQL (via provider_sql.py / cascade_builder.py) and executes it
through session.sql(...).collect(). The only per-row Python work in this
entire demo is the MATCHBOT_METAPHONE UDF (registered here once, at
deployment, not called from Python per row — Snowflake invokes it as part
of the generated SQL's SELECT list, same as any other SQL function call).

See docs/snowflake-implementation-plan.md's "Build order" step 5: this
procedure wraps already-independently-tested SQL generators (provider_sql,
matcher_registry, cascade_builder) — a mismatch between this procedure's
output and running those generators' SQL directly is a plumbing bug, not a
matching-logic bug.

Deploy (once, or whenever this file / its dependencies change):

    snow sql -f snowflake/ddl/00_database_and_schema.sql
    # ... run 01-06 DDL files ...
    snow snowpark deploy   # or an equivalent CREATE PROCEDURE ... AS $$ ... $$
                            # packaging this module — see the module-level
                            # HANDLER contract below.

Invoke manually (build step 5's validation):

    CALL RUN_MATCH_PIPELINE('data/input/ride_enrollment/some_file.csv');
"""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session


def register_metaphone_udf(session: "Session") -> None:
    """Register MATCHBOT_METAPHONE as a Python UDF backed by the real
    jellyfish library — the same library matching/standardize.py::metaphone
    uses. Snowflake's native SOUNDEX is a different algorithm; using it
    instead of this UDF would silently break parity with the AWS demo (see
    derive_sql.py's module docstring). Call once per deployment /
    session — CREATE OR REPLACE makes re-registration idempotent.

    return_type/input_types are passed explicitly as StringType() rather
    than relying on Snowpark's type-hint inference: this module has
    `from __future__ import annotations` (module-level, for the
    TYPE_CHECKING-guarded Session import), which turns every annotation —
    including this inner function's `str | None` — into a plain string at
    runtime. Snowpark's register() tries to parse that string as a real
    type and fails with "TypeError: invalid type str | None" — caught via
    live deployment, not by any local test (type-hint-string behavior only
    manifests when Snowpark actually attempts registration).
    """
    import jellyfish
    from snowflake.snowpark.types import StringType

    def _metaphone(value):
        if value is None:
            return None
        text = value.strip().upper()
        if not text:
            return None
        return jellyfish.metaphone(text) or None

    session.udf.register(
        _metaphone,
        name="MATCHBOT_METAPHONE",
        return_type=StringType(),
        input_types=[StringType()],
        packages=["jellyfish"],
        is_permanent=True,
        replace=True,
        stage_location="@~",  # user stage; adjust to a named stage in production use
    )


def _new_run_uid() -> str:
    return f"run-{uuid.uuid4().hex[:12]}"


def run_match_pipeline(session: "Session", file_path: str) -> str:
    """Land, cleanse, stage, match, and audit ONE file already visible on
    INPUT_STAGE. Returns a short human-readable summary string (the
    convention CALL procedures use to report back in a worksheet/Task log).

    ``file_path`` is the relative path within the stage, e.g.
    'data/input/ride_enrollment/ride_enrollment_2026-07-09.csv' — the same
    shape scripts/lambda_function_glue.py's key parsing expects.
    """
    from matchbot_snowflake.cascade_builder import build_cascade_sql, build_writeback_sql
    from matchbot_snowflake.config_models import load_bundled_config
    from matchbot_snowflake.land_sql import (
        fetch_header_columns,
        land_table_name,
        render_create_land_table_sql,
        render_duplicate_row_count_sql,
        render_file_profile_sql,
        render_load_clean_rows_sql,
        render_reject_ragged_rows_sql,
    )
    from matchbot_snowflake.matcher_registry import build_sql_fragments
    from matchbot_snowflake.notify_sql import (
        matched_on_attributes,
        render_failure_email_sql,
        render_success_email_sql,
    )
    from matchbot_snowflake.provider_sql import render_provider_projection_sql

    started_at = time.time()
    run_uid = _new_run_uid()

    # --- resolve provider from the folder segment of file_path -----------
    folder = file_path.split("/")[-2] if "/" in file_path else None
    provider_row = (
        session.sql(
            "SELECT provider_id, provider_code, dataset_name, external_id_column "
            "FROM PROVIDER_FOLDER_MAP WHERE folder_name = ?",
            params=[folder],
        )
        .collect()
    )
    if not provider_row:
        return f"SKIPPED — no provider configured for folder {folder!r} (file: {file_path})"
    provider_id, provider_code, dataset_name, external_id_column = (
        provider_row[0][0],
        provider_row[0][1],
        provider_row[0][2],
        provider_row[0][3],
    )

    # --- audit: begin_run -------------------------------------------------
    session.sql(
        "INSERT INTO RILDS_AUDIT (run_uid, provider_code, dataset_name, "
        "runtime, source_uri, status) VALUES (?, ?, ?, 'snowflake', ?, 'RUNNING')",
        params=[run_uid, provider_code, dataset_name, file_path],
    ).collect()
    pipeline_run_id = session.sql(
        "SELECT id FROM RILDS_AUDIT WHERE run_uid = ?", params=[run_uid]
    ).collect()[0][0]

    try:
        # --- land: dynamic, header-driven (matchbot_snowflake/land_sql.py) ---
        # Reads THIS file's own header to create/reuse a land table shaped
        # to match it, and to compute the expected field count — no
        # hardcoded column list or count, so a new provider's differently
        # shaped file needs no new code here (mirrors AWS's
        # build_land_table(): "any provider works with zero bespoke DDL").
        # An earlier version of this procedure hand-wrote RIDE's 36-column
        # COPY INTO here directly — a real regression from that guarantee,
        # corrected once asked directly whether a new provider would need
        # new code.
        #
        # Ragged rows (e.g. an unescaped comma inside a college name
        # shifting every later column — see storage/schema.py's
        # rilds_land_rejects comment for the same failure mode on the AWS
        # side) are detected by counting delimiters per raw line ourselves:
        # Snowflake's ERROR_ON_COLUMN_COUNT_MISMATCH file-format setting was
        # tried first and found, via live validation, not to catch rows
        # with MORE fields than expected (only fewer) — see
        # snowflake/ddl/02_file_format_and_stage.sql.
        # Stage name matches this account's actual deployed stage — see
        # snowflake/ddl/02_file_format_and_stage.sql (created as
        # MATCHBOT_INPUT_STAGE in this account, not the DDL comment's
        # generic INPUT_STAGE placeholder name).
        stage_file_path = f"MATCHBOT_INPUT_STAGE/{file_path}"
        header_columns = fetch_header_columns(session, stage_file_path)
        expected_field_count = len(header_columns)
        land_table = land_table_name(provider_code)

        session.sql(render_create_land_table_sql(provider_code, header_columns)).collect()

        session.sql(
            render_reject_ragged_rows_sql(
                provider_code, pipeline_run_id, stage_file_path, expected_field_count
            )
        ).collect()
        session.sql(
            render_load_clean_rows_sql(
                provider_code, pipeline_run_id, stage_file_path, header_columns
            )
        ).collect()

        # Query actual counts rather than trust the INSERT result set's
        # column naming (Snowflake's "number of rows inserted" convention
        # isn't verified against a live session here) — a direct COUNT(*)
        # is unambiguous and self-evidently correct either way.
        rows_rejected = session.sql(
            "SELECT COUNT(*) FROM RILDS_LAND_REJECTS WHERE pipeline_run_id = ?",
            params=[pipeline_run_id],
        ).collect()[0][0]
        rows_landed = session.sql(
            f"SELECT COUNT(*) FROM {land_table} WHERE pipeline_run_id = ?",
            params=[pipeline_run_id],
        ).collect()[0][0]

        # --- cleanse + canonicalize: provider_sql.py's generated SELECT ---
        # Loaded via importlib.resources (load_bundled_config), not a
        # Path(__file__)-relative lookup: Snowflake's stored-procedure
        # sandbox runs this package directly out of the uploaded zip via
        # zipimport, without ever extracting it to a real filesystem
        # location — so `import matchbot_snowflake...` works, but plain
        # Path("...").exists()/.read_text() cannot see files bundled
        # inside that same zip (confirmed live: the path computed from
        # __file__ was byte-identical to the file's real location inside
        # the zip per `unzip -l`, yet .exists() on it still returned
        # False). See config_models.py's load_bundled_config docstring.
        app_config = load_bundled_config()
        provider = app_config.provider(provider_id)
        projection_sql = render_provider_projection_sql(
            provider,
            app_config.global_config.standardization,
            land_table=land_table,
            pipeline_run_id=pipeline_run_id,
        )
        stage_insert_sql = f"""
            INSERT INTO RILDS_STAGE (
                pipeline_run_id, provider_code, dataset_name, source_row_id,
                first_name, middle_name, last_name, birth_date, gender,
                first_name_std, last_name_std, first_name_metaphone1,
                last_name_metaphone1, last_name8, birth_year, birth_month,
                birth_day, rilds_id, lasid, ssn, address1, address2, city,
                state, zip
            )
            SELECT {pipeline_run_id}, provider_code, dataset_name, source_row_id,
                first_name, middle_name, last_name, birth_date, gender,
                first_name_std, last_name_std, first_name_metaphone1,
                last_name_metaphone1, last_name8, birth_year, birth_month,
                birth_day, rilds_id, lasid, ssn, address1, address2, city,
                state, zip
            FROM ({projection_sql})
        """
        session.sql(stage_insert_sql).collect()
        rows_staged = session.sql(
            "SELECT COUNT(*) FROM RILDS_STAGE WHERE pipeline_run_id = ?",
            params=[pipeline_run_id],
        ).collect()[0][0]

        # --- match: matcher_registry.py + cascade_builder.py --------------
        # WINNERS is a plain (non-temporary) table, CREATE OR REPLACE'd each
        # run: Snowflake's owner's-rights stored procedure sandbox rejects
        # `CREATE TEMPORARY TABLE` outright ("Unsupported statement type
        # 'temporary TABLE'" — confirmed via a live CALL failure), and a CTE
        # can't be used instead since WINNERS must stay visible across
        # several separate session.sql(...).collect() calls (the 4
        # writeback statements below), not just within one statement.
        fragments = build_sql_fragments(
            app_config.global_config.matching.matchers, external_id_column
        )
        cascade_sql = build_cascade_sql(fragments, run_id_param=str(pipeline_run_id))
        session.sql(f"CREATE OR REPLACE TABLE WINNERS AS {cascade_sql}").collect()

        writeback = build_writeback_sql(run_id_param=str(pipeline_run_id))
        session.sql(writeback["update_stage_matched"]).collect()
        session.sql(writeback["update_stage_unmatched"]).collect()
        session.sql(writeback["insert_matched"]).collect()
        session.sql(writeback["insert_error"]).collect()

        rows_matched = session.sql(
            "SELECT COUNT(*) FROM RILDS_MATCHED WHERE pipeline_run_id = ?",
            params=[pipeline_run_id],
        ).collect()[0][0]
        rows_unmatched = session.sql(
            "SELECT COUNT(*) FROM RILDS_ERROR WHERE pipeline_run_id = ?",
            params=[pipeline_run_id],
        ).collect()[0][0]

        duration_seconds = time.time() - started_at
        match_rate = round(rows_matched / rows_staged, 4) if rows_staged else 0.0

        session.sql(
            "UPDATE RILDS_AUDIT SET status = 'SUCCESS', duration_seconds = ?, "
            "match_rate = ?, rows_received = ?, rows_rejected = ?, rows_landed = ?, "
            "rows_staged = ?, rows_matched = ?, rows_unmatched = ?, "
            "finished_at = CURRENT_TIMESTAMP() WHERE id = ?",
            params=[
                duration_seconds, match_rate, rows_landed + rows_rejected, rows_rejected,
                rows_landed, rows_staged, rows_matched, rows_unmatched, pipeline_run_id,
            ],
        ).collect()

        session.sql(
            "UPDATE INGEST_LOG SET status = 'SUCCESS', pipeline_run_id = ? "
            "WHERE file_path = ?",
            params=[pipeline_run_id, file_path],
        ).collect()

        # Run-summary email — Snowflake-native equivalent of the AWS demo's
        # SESNotifier (see notify_sql.py's module docstring for the
        # one-time account setup this depends on). A failure here (e.g. an
        # unverified recipient) must not fail an otherwise-successful
        # pipeline run — logged via the return string's own visibility in
        # CALL's output / INGEST_LOG rather than re-raised.
        try:
            reference_row_count = session.sql(
                "SELECT COUNT(*) FROM RILDS_REFERENCE"
            ).collect()[0][0]
            matched_on = matched_on_attributes(app_config.global_config.matching.matchers)
            duplicate_row_count = session.sql(
                render_duplicate_row_count_sql(provider_code, pipeline_run_id, header_columns)
            ).collect()[0][0]
            null_counts = [
                (row["COLUMN_NAME"], row["NULL_COUNT"])
                for row in session.sql(
                    render_file_profile_sql(provider_code, pipeline_run_id, header_columns)
                ).collect()
            ]
            session.sql(
                render_success_email_sql(
                    file_path, provider_code, rows_landed, rows_rejected,
                    rows_staged, rows_matched, rows_unmatched, match_rate,
                    duration_seconds, run_uid, matched_on, reference_row_count,
                    expected_field_count, duplicate_row_count, null_counts,
                )
            ).collect()
        except Exception as email_exc:  # noqa: BLE001
            log_note = f" (email notification failed: {email_exc})"
        else:
            log_note = ""

        return (
            f"{file_path}: {rows_matched}/{rows_staged} matched "
            f"({match_rate:.1%}), {rows_unmatched} unmatched, "
            f"{duration_seconds:.2f}s [SUCCESS] (run_uid={run_uid})" + log_note
        )

    except Exception as exc:  # noqa: BLE001 — surfaced to CALL's return + audit row
        session.sql(
            "UPDATE RILDS_AUDIT SET status = 'FAILED', error = ?, "
            "finished_at = CURRENT_TIMESTAMP() WHERE id = ?",
            params=[str(exc), pipeline_run_id],
        ).collect()
        session.sql(
            "UPDATE INGEST_LOG SET status = 'FAILED', error = ? WHERE file_path = ?",
            params=[str(exc), file_path],
        ).collect()
        try:
            session.sql(render_failure_email_sql(file_path, str(exc), run_uid)).collect()
        except Exception:  # noqa: BLE001 — never mask the real failure below
            pass
        raise
