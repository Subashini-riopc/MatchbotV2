-- Snowflake account cost tracking — DAILY_COSTS table + refresh procedure.
--
-- Entirely independent of the RILDS_* pipeline tables/procedures in this
-- schema — reads only from Snowflake's own SNOWFLAKE.ACCOUNT_USAGE system
-- views (never touches any pipeline data) and writes only to DAILY_COSTS.
-- Exists because ACCOUNT_USAGE views have real ingestion latency (documented
-- up to ~3 hours, sometimes more) and no long-term retention guarantee of
-- their own — DAILY_COSTS is a durable, fast-to-query daily snapshot that
-- survives independently of whatever ACCOUNT_USAGE itself retains.
--
-- One row per (usage_date, service_type, warehouse_name): service_type rows
-- (WAREHOUSE_METERING, PIPE, AUTO_CLUSTERING, TRUST_CENTER, etc., sourced
-- from METERING_DAILY_HISTORY) get warehouse_name = '(ALL)', a sentinel
-- meaning "not broken out by warehouse" — Snowflake forces every PRIMARY
-- KEY column NOT NULL (confirmed live: a plain nullable column becomes
-- NOT NULL the moment it's added to a PK), so a real NULL can't be used
-- here as originally designed; '(ALL)' plays that role instead, chosen to
-- never collide with a real warehouse name. WAREHOUSE_METERING
-- additionally gets one row per (date, real warehouse_name) from
-- WAREHOUSE_METERING_HISTORY (rolled up from its native per-hour grain to
-- daily), so both "cost by service" and "cost by warehouse" are
-- answerable from the same table without a second one.
--
-- Populated by calling REFRESH_DAILY_COSTS() — manually, whenever you want
-- fresh numbers (CALL REFRESH_DAILY_COSTS();); no Task/schedule is created
-- here, per explicit request. Re-running it is always safe: the MERGE
-- updates existing rows rather than duplicating them, so a re-pull of the
-- same date range after ACCOUNT_USAGE settles late-arriving data just
-- corrects the numbers in place.

USE DATABASE MATCHBOT;
USE SCHEMA RILDS;

CREATE TABLE IF NOT EXISTS DAILY_COSTS (
    usage_date                   DATE         NOT NULL,
    service_type                 VARCHAR(100) NOT NULL,  -- e.g. WAREHOUSE_METERING, PIPE, AUTO_CLUSTERING, TRUST_CENTER
    warehouse_name                VARCHAR(200) NOT NULL,  -- '(ALL)' sentinel unless this row is a per-warehouse WAREHOUSE_METERING breakout
    credits_used_compute         NUMBER(38, 9),
    credits_used_cloud_services  NUMBER(38, 9),
    credits_used_total           NUMBER(38, 9),
    credits_billed                NUMBER(38, 10),          -- NULL for warehouse-level rows (WAREHOUSE_METERING_HISTORY has no billed figure of its own)
    loaded_at                     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (usage_date, service_type, warehouse_name)
);

-- Quick daily rollup — total credits + a per-service breakdown as one JSON
-- object per day. A plain VIEW (not materialized): always reflects
-- whatever is currently in DAILY_COSTS at query time, no separate refresh
-- step of its own — only the base table needs REFRESH_DAILY_COSTS().
CREATE OR REPLACE VIEW DAILY_COSTS_SUMMARY AS
SELECT
    usage_date,
    SUM(credits_used_total)                              AS total_credits_used,
    SUM(credits_billed)                                  AS total_credits_billed,
    OBJECT_AGG(service_type, TO_VARIANT(credits_billed)) AS cost_by_service
FROM DAILY_COSTS
WHERE warehouse_name = '(ALL)'  -- service_type rows only; avoids double-counting WAREHOUSE_METERING's per-warehouse rows against its own service_type total
GROUP BY usage_date;

CREATE OR REPLACE PROCEDURE REFRESH_DAILY_COSTS(LOOKBACK_DAYS INTEGER DEFAULT 7)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    service_rows INTEGER DEFAULT 0;
    warehouse_rows INTEGER DEFAULT 0;
BEGIN
    -- Service-type rows (one per usage_date/service_type, warehouse_name
    -- = '(ALL)' sentinel) — re-pulls the last LOOKBACK_DAYS days every
    -- call (not just "yesterday"), since ACCOUNT_USAGE can revise recent
    -- days' figures after they first appear; MERGE makes re-pulling the
    -- same range safe.
    MERGE INTO DAILY_COSTS AS target
    USING (
        SELECT
            usage_date,
            service_type,
            '(ALL)' AS warehouse_name,
            credits_used_compute,
            credits_used_cloud_services,
            credits_used AS credits_used_total,
            credits_billed
        FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
        WHERE usage_date >= DATEADD(day, -:LOOKBACK_DAYS, CURRENT_DATE())
    ) AS source
    ON target.usage_date = source.usage_date
       AND target.service_type = source.service_type
       AND target.warehouse_name = source.warehouse_name
    WHEN MATCHED THEN UPDATE SET
        credits_used_compute = source.credits_used_compute,
        credits_used_cloud_services = source.credits_used_cloud_services,
        credits_used_total = source.credits_used_total,
        credits_billed = source.credits_billed,
        loaded_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        usage_date, service_type, warehouse_name,
        credits_used_compute, credits_used_cloud_services, credits_used_total, credits_billed
    ) VALUES (
        source.usage_date, source.service_type, source.warehouse_name,
        source.credits_used_compute, source.credits_used_cloud_services,
        source.credits_used_total, source.credits_billed
    );
    service_rows := SQLROWCOUNT;

    -- Per-warehouse rows (one per usage_date/warehouse_name, service_type
    -- fixed to 'WAREHOUSE_METERING') — rolled up from WAREHOUSE_METERING_
    -- HISTORY's native per-hour grain via DATE(start_time)/SUM(...).
    -- credits_billed is left NULL here: WAREHOUSE_METERING_HISTORY has no
    -- billed-credits figure of its own (only METERING_DAILY_HISTORY does,
    -- at the service_type grain), so this stays an unbilled usage figure.
    MERGE INTO DAILY_COSTS AS target
    USING (
        SELECT
            DATE(start_time) AS usage_date,
            'WAREHOUSE_METERING' AS service_type,
            warehouse_name,
            SUM(credits_used_compute) AS credits_used_compute,
            SUM(credits_used_cloud_services) AS credits_used_cloud_services,
            SUM(credits_used) AS credits_used_total,
            NULL AS credits_billed
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE start_time >= DATEADD(day, -:LOOKBACK_DAYS, CURRENT_DATE())
        GROUP BY DATE(start_time), warehouse_name
    ) AS source
    ON target.usage_date = source.usage_date
       AND target.service_type = source.service_type
       AND target.warehouse_name = source.warehouse_name
    WHEN MATCHED THEN UPDATE SET
        credits_used_compute = source.credits_used_compute,
        credits_used_cloud_services = source.credits_used_cloud_services,
        credits_used_total = source.credits_used_total,
        loaded_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        usage_date, service_type, warehouse_name,
        credits_used_compute, credits_used_cloud_services, credits_used_total, credits_billed
    ) VALUES (
        source.usage_date, source.service_type, source.warehouse_name,
        source.credits_used_compute, source.credits_used_cloud_services,
        source.credits_used_total, source.credits_billed
    );
    warehouse_rows := SQLROWCOUNT;

    RETURN 'Refreshed ' || service_rows || ' service-type row(s) and ' ||
           warehouse_rows || ' warehouse row(s) for the last ' || LOOKBACK_DAYS || ' day(s).';
END;
$$;

-- Manual invocation (no Task/schedule — call whenever you want fresh
-- numbers; safe to re-run anytime):
--   CALL REFRESH_DAILY_COSTS();          -- default 7-day lookback
--   CALL REFRESH_DAILY_COSTS(30);        -- wider lookback, e.g. first run
--   SELECT * FROM DAILY_COSTS_SUMMARY ORDER BY usage_date DESC;
