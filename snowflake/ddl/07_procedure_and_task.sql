-- Snowflake demo — stored procedure registration + polling Task.
--
-- The RUN_MATCH_PIPELINE procedure body is the Python in
-- python/matchbot_snowflake/procedures/run_pipeline.py — deploy it with
-- the Snowflake CLI's `snow snowpark deploy` (or an equivalent CREATE
-- PROCEDURE ... AS $$ ... $$ packaging step) rather than hand-copying
-- Python into this file; keeping the SQL registration and the Python body
-- in separate files means the Python stays testable with plain pytest
-- (see snowflake/python/tests/), not only inside a live warehouse.
--
-- This file defines: the poll/dispatch procedure (pure SQL — finds new
-- files, resolves their provider, calls RUN_MATCH_PIPELINE per file) and
-- the Task that runs it on a schedule. Poll/dispatch is deliberately
-- separate from RUN_MATCH_PIPELINE itself so matching can be invoked and
-- tested directly (CALL RUN_MATCH_PIPELINE('...')) without waiting on the
-- schedule — see docs/snowflake-implementation-plan.md build step 6.

USE DATABASE MATCHBOT;
USE SCHEMA RILDS;

CREATE OR REPLACE PROCEDURE POLL_INPUT_STAGE()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    files_found INTEGER DEFAULT 0;
    files_dispatched INTEGER DEFAULT 0;
BEGIN
    -- Refresh the directory table's view of the stage, then find files
    -- not yet in INGEST_LOG whose folder resolves to a configured provider.
    ALTER STAGE INPUT_STAGE REFRESH;

    INSERT INTO INGEST_LOG (file_path, provider_id, status)
    SELECT
        d.relative_path,
        m.provider_id,
        'PENDING'
    FROM DIRECTORY(@INPUT_STAGE) d
    JOIN PROVIDER_FOLDER_MAP m
        ON m.folder_name = SPLIT_PART(d.relative_path, '/', -2)
    WHERE NOT EXISTS (
        SELECT 1 FROM INGEST_LOG i WHERE i.file_path = d.relative_path
    );

    files_found := SQLROWCOUNT;

    -- Dispatch every still-PENDING file (covers both newly-inserted rows
    -- above and any prior run that failed before updating its own status).
    FOR rec IN (
        SELECT file_path FROM INGEST_LOG WHERE status = 'PENDING'
    ) DO
        CALL RUN_MATCH_PIPELINE(:rec.file_path);
        files_dispatched := files_dispatched + 1;
    END FOR;

    RETURN 'Found ' || files_found || ' new file(s), dispatched ' || files_dispatched || '.';
END;
$$;

-- Scheduled polling Task — the only trigger mechanism in this demo (no
-- Lambda/EventBridge). 5-minute cadence is a starting point for the demo;
-- tune via ALTER TASK ... SET SCHEDULE = '...' once real timing
-- expectations are known.
CREATE OR REPLACE TASK POLL_INPUT_STAGE_TASK
    WAREHOUSE = MATCHBOT_DEMO_WH
    SCHEDULE = '5 MINUTE'
    COMMENT = 'Polls INPUT_STAGE for new provider files and dispatches RUN_MATCH_PIPELINE per file'
AS
    CALL POLL_INPUT_STAGE();

-- Tasks are created SUSPENDED by default — must be explicitly resumed.
-- Leave suspended until build step 6's manual EXECUTE TASK validation
-- passes (see docs/snowflake-implementation-plan.md):
--   ALTER TASK POLL_INPUT_STAGE_TASK RESUME;
--
-- Manual validation before relying on the schedule:
--   EXECUTE TASK POLL_INPUT_STAGE_TASK;
--   SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
--     WHERE NAME = 'POLL_INPUT_STAGE_TASK' ORDER BY SCHEDULED_TIME DESC LIMIT 5;
