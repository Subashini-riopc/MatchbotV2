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

import fnmatch
import json
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


def register_jaro_winkler_udf(session: "Session") -> None:
    """Register MATCHBOT_JARO_WINKLER as a Python UDF backed by the real
    jellyfish library — the same library matching/standardize.py::jaro_winkler
    uses for the fuzzy matcher tier. Needed because plain SQL has no
    string-similarity function that's guaranteed to match jellyfish's
    algorithm/output exactly (Snowflake's native JAROWINKLER_SIMILARITY is a
    different implementation on a 0-100 scale, not confirmed numerically
    identical — using it would risk silently diverging fuzzy match/review
    outcomes from the AWS side for the same input pair). Call once per
    deployment/session — CREATE OR REPLACE makes re-registration idempotent.

    Mirrors jaro_winkler()'s exact null/empty handling: missing on either
    side scores 0.0, not NULL, so this can be used directly in arithmetic
    (weighted sums) without a separate NULL-coalescing step downstream.

    return_type/input_types passed explicitly as concrete Snowpark types for
    the same reason register_metaphone_udf does — see that function's
    docstring (this module's `from __future__ import annotations` turns
    inline type hints into plain strings at runtime, which Snowpark's
    register() cannot parse).
    """
    import jellyfish
    from snowflake.snowpark.types import DoubleType, StringType

    def _jaro_winkler(a, b):
        if not a or not b:
            return 0.0
        return float(jellyfish.jaro_winkler_similarity(a, b))

    session.udf.register(
        _jaro_winkler,
        name="MATCHBOT_JARO_WINKLER",
        return_type=DoubleType(),
        input_types=[StringType(), StringType()],
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
    from matchbot_snowflake.derive_sql import HASH_COLUMN_BY_KEYS
    from matchbot_snowflake.land_sql import (
        csv_format_for_delimiter,
        fetch_header_columns,
        file_type_from_filename,
        land_table_name,
        render_add_missing_columns_sql,
        render_create_land_table_sql,
        render_duplicate_row_count_sql,
        render_file_profile_sql,
        render_load_clean_rows_sql,
        render_reject_ragged_rows_sql,
    )
    from matchbot_snowflake.matcher_registry import build_sql_fragments
    from matchbot_snowflake.notify_sql import (
        render_failure_email_sql,
        render_success_email_sql,
    )
    from matchbot_snowflake.provider_sql import render_provider_projection_sql
    from matchbot_snowflake.voter_history_sql import (
        STAGE_TABLE as VOTER_HISTORY_STAGE_TABLE,
        render_voter_history_projection_sql,
    )

    started_at = time.time()
    run_uid = _new_run_uid()

    # --- resolve provider from the folder segment of file_path, THEN pick
    # the row whose file_glob actually matches this filename -----------
    # PROVIDER_FOLDER_MAP is keyed on (folder_name, file_glob), not
    # folder_name alone: a multi_file provider (e.g. RISOS) has more than
    # one ProviderConfig sharing one folder (risos_voter/Voter_*.txt and
    # risos_voter/VoterHistory_*.txt) — see snowflake/ddl/03_provider_folder_map.sql.
    # Every row for this folder is fetched, then filtered in Python by
    # fnmatch against the real filename, since Snowflake SQL has no glob
    # operator to push this into the WHERE clause itself.
    folder = file_path.split("/")[-2] if "/" in file_path else None
    filename = file_path.rsplit("/", 1)[-1]
    candidate_rows = (
        session.sql(
            "SELECT provider_id, provider_code, dataset_name, external_id_column, delimiter, "
            "file_glob, multi_file, matches_dataset, matching_identifiers "
            "FROM PROVIDER_FOLDER_MAP WHERE folder_name = ?",
            params=[folder],
        )
        .collect()
    )
    provider_row = next(
        (row for row in candidate_rows if fnmatch.fnmatch(filename, row[5])), None
    )
    if provider_row is None:
        return (
            f"SKIPPED — no provider configured for folder {folder!r} matching "
            f"filename {filename!r} (file: {file_path})"
        )
    (
        provider_id, provider_code, dataset_name, external_id_column, delimiter,
        _file_glob, multi_file, matches_dataset, matching_identifiers,
    ) = (
        provider_row[0], provider_row[1], provider_row[2], provider_row[3],
        provider_row[4], provider_row[5], provider_row[6], provider_row[7],
        provider_row[8],
    )
    csv_format = csv_format_for_delimiter(delimiter)

    # file_type names this file's OWN land table ({PROVIDER}_{FILE_TYPE}_LAND)
    # for a multi_file provider — e.g. RISOS's VOTER vs VOTERHISTORY — so two
    # structurally unrelated file shapes never collide on one table (see
    # land_sql.py's file_type_from_filename/land_table_name).
    file_type = file_type_from_filename(filename) if multi_file else None

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
        header_columns = fetch_header_columns(session, stage_file_path, delimiter=delimiter)
        expected_field_count = len(header_columns)
        land_table = land_table_name(provider_code, file_type)

        session.sql(
            render_create_land_table_sql(provider_code, header_columns, file_type=file_type)
        ).collect()

        # Schema evolution: the table above may already have existed from an
        # earlier run of this SAME file type with fewer/different columns
        # (RISOS's own file adding a new field next year, say) — ADD any
        # header column the table doesn't already have. Never drops/renames
        # a column, so old rows just read NULL for a column their file
        # didn't carry. Not needed the very first time a file type lands
        # (CREATE TABLE above just made it with every current column), but
        # cheap and idempotent (ADD COLUMN IF NOT EXISTS) to always check.
        #
        # Queried via INFORMATION_SCHEMA rather than DESCRIBE TABLE: a plain
        # SELECT's result columns are reliably uppercase (COLUMN_NAME) in
        # Snowpark, whereas DESCRIBE TABLE is one of the few commands whose
        # own result columns come back lowercase ("name") — mixing the two
        # conventions in one query risks a silent case-sensitivity bug this
        # avoids entirely.
        existing_columns = {
            row["COLUMN_NAME"]
            for row in session.sql(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = 'RILDS' AND TABLE_NAME = ?",
                params=[land_table],
            ).collect()
        }
        for alter_sql in render_add_missing_columns_sql(
            provider_code, header_columns, existing_columns, file_type=file_type
        ):
            session.sql(alter_sql).collect()

        session.sql(
            render_reject_ragged_rows_sql(
                provider_code,
                pipeline_run_id,
                stage_file_path,
                expected_field_count,
                csv_format=csv_format,
            )
        ).collect()
        session.sql(
            render_load_clean_rows_sql(
                provider_code,
                pipeline_run_id,
                stage_file_path,
                header_columns,
                csv_format=csv_format,
                file_type=file_type,
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

        if not matches_dataset:
            # This provider_id has matches_dataset=false (e.g.
            # risos_voterhistory — see its YAML's module docstring): it
            # lands and transforms but never runs person-linkage matching,
            # mirroring legacy's PeripheralDataRow (run_linkage=False)
            # datasets, which ride on another dataset's already-established
            # identity linkage rather than matching independently.
            #
            # dataset_name dispatches to the one transform this file type
            # actually has today (VoterHistory's per-election unpivot) —
            # same pattern as matches_dataset=true's column_mappings-driven
            # provider_sql.py dispatch, just not config-generic yet since
            # only one non-matching transform exists so far. A future
            # matches_dataset=false file type with a genuinely different
            # transform shape gets its own dataset_name branch here, same
            # as provider_sql.py would gain a second projection function
            # rather than trying to force every shape through one generic
            # column_mappings schema.
            if dataset_name == "voterhistory":
                transform_sql = render_voter_history_projection_sql(land_table, pipeline_run_id)
                session.sql(
                    f"""INSERT INTO {VOTER_HISTORY_STAGE_TABLE} (
                        pipeline_run_id, source_row_id, voter_id, election_date,
                        election_name, vote_type, precinct, party, did_not_vote
                    )
                    SELECT {pipeline_run_id}, source_row_id, voter_id, election_date,
                        election_name, vote_type, precinct, party, did_not_vote
                    FROM ({transform_sql})"""
                ).collect()
                rows_transformed = session.sql(
                    f"SELECT COUNT(*) FROM {VOTER_HISTORY_STAGE_TABLE} WHERE pipeline_run_id = ?",
                    params=[pipeline_run_id],
                ).collect()[0][0]
                transform_note = f", {rows_transformed} rows into {VOTER_HISTORY_STAGE_TABLE}"
            else:
                # A matches_dataset=false file type with no transform
                # branch above yet — land succeeded, but there's nothing
                # else to run for it. Same "not built yet" outcome as the
                # old LANDED_ONLY status, just reached via a different
                # routing condition.
                transform_note = " (no transform configured for this file type yet)"

            duration_seconds = time.time() - started_at
            session.sql(
                "UPDATE RILDS_AUDIT SET status = 'SUCCESS', duration_seconds = ?, "
                "rows_received = ?, rows_rejected = ?, rows_landed = ?, "
                "finished_at = CURRENT_TIMESTAMP() WHERE id = ?",
                params=[
                    duration_seconds, rows_landed + rows_rejected, rows_rejected,
                    rows_landed, pipeline_run_id,
                ],
            ).collect()
            session.sql(
                "UPDATE INGEST_LOG SET status = 'SUCCESS', pipeline_run_id = ? "
                "WHERE file_path = ?",
                params=[pipeline_run_id, file_path],
            ).collect()
            return (
                f"{file_path}: landed {rows_landed} rows into {land_table} "
                f"({rows_rejected} rejected){transform_note}, {duration_seconds:.2f}s "
                f"[TRANSFORMED — no person-linkage matching for this file type] "
                f"(run_uid={run_uid})"
            )

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
        # Hash columns (name_dob_hash, etc.) are appended after zip5 — see
        # derive_sql.py's HASH_COLUMN_BY_KEYS, the single source of truth
        # for their names, imported rather than hardcoded here so this list
        # can never silently drift from what provider_sql.py actually
        # computes.
        hash_column_names = list(HASH_COLUMN_BY_KEYS.values())
        hash_columns_list = ", ".join(hash_column_names)
        stage_insert_sql = f"""
            INSERT INTO RILDS_STAGE (
                pipeline_run_id, provider_code, dataset_name, source_row_id,
                first_name, middle_name, last_name, birth_date, gender,
                first_name_std, last_name_std, first_name_metaphone1,
                last_name_metaphone1, last_name8, birth_year, birth_month,
                birth_day, rilds_id, lasid, ssn, ssn4, address1, address1_std,
                address2, city, state, zip, zip5, {hash_columns_list}
            )
            SELECT {pipeline_run_id}, provider_code, dataset_name, source_row_id,
                first_name, middle_name, last_name, birth_date, gender,
                first_name_std, last_name_std, first_name_metaphone1,
                last_name_metaphone1, last_name8, birth_year, birth_month,
                birth_day, rilds_id, lasid, ssn, ssn4, address1, address1_std,
                address2, city, state, zip, zip5, {hash_columns_list}
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
        session.sql(writeback["update_stage_low_confidence"]).collect()
        session.sql(writeback["update_stage_unmatched"]).collect()
        session.sql(writeback["insert_matched"]).collect()
        session.sql(writeback["insert_low_confidence"]).collect()
        session.sql(writeback["insert_error"]).collect()

        rows_matched = session.sql(
            "SELECT COUNT(*) FROM RILDS_MATCHED WHERE pipeline_run_id = ?",
            params=[pipeline_run_id],
        ).collect()[0][0]
        # RILDS_ERROR now holds two distinct decisions (see
        # cascade_builder.build_writeback_sql's docstring): NO_MATCH (no
        # candidate cleared any matcher's review_threshold) and
        # LOW_CONFIDENCE (a fuzzy candidate cleared review_threshold but not
        # accept_threshold — flagged for manual review, not a hard failure).
        # rows_unmatched is reported as their combined count, same meaning
        # as before this matcher expansion (every RILDS_ERROR row); the two
        # are also queryable separately via RILDS_ERROR.decision.
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
            # PROVIDER_FOLDER_MAP.matching_identifiers (config_bridge.py) is
            # this provider's own resolved-chain "matched on" list — already
            # filtered down to only the matchers this provider's
            # column_mappings actually support (config_bridge.py's
            # matching_identifiers_for_provider(), same two-step computation
            # as the AWS orchestrator's filter_chain_by_provider_attributes +
            # matched_on_attributes). Reading it here instead of recomputing
            # from the full, UNFILTERED global chain fixes a real gap: this
            # email used to list every matcher's attributes regardless of
            # whether this provider's data could ever satisfy them (e.g.
            # showing SSN/Birth Date/Address for RIDE, which maps none of
            # those) — confirmed live before this fix. Snowpark returns an
            # ARRAY column as a JSON string via collect(), not a Python list.
            matched_on = json.loads(matching_identifiers) if matching_identifiers else []
            duplicate_row_count = session.sql(
                render_duplicate_row_count_sql(
                    provider_code, pipeline_run_id, header_columns, file_type=file_type
                )
            ).collect()[0][0]
            null_counts = [
                (row["COLUMN_NAME"], row["NULL_COUNT"])
                for row in session.sql(
                    render_file_profile_sql(
                        provider_code, pipeline_run_id, header_columns, file_type=file_type
                    )
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
