-- Snowflake demo — database, schema, warehouse.
--
-- Uses the existing MATCHBOT database / RILDS schema (already created on
-- the account) rather than provisioning a separate demo database. Schema
-- name RILDS mirrors the Postgres DB_SCHEMA=rilds convention used by the
-- AWS demo, purely so table names read the same across both write-ups.
-- CREATE ... IF NOT EXISTS below is a safe no-op against the existing
-- database/schema.

CREATE DATABASE IF NOT EXISTS MATCHBOT;

CREATE SCHEMA IF NOT EXISTS MATCHBOT.RILDS;

USE DATABASE MATCHBOT;
USE SCHEMA RILDS;

-- Small, auto-suspending warehouse — this is a demo, not a production
-- workload; no need to size beyond XSMALL. AUTO_SUSPEND in seconds.
CREATE WAREHOUSE IF NOT EXISTS MATCHBOT_DEMO_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'MatchBot Snowflake demo — land/stage/match/audit pipeline';

USE WAREHOUSE MATCHBOT_DEMO_WH;
