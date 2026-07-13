-- Snowflake demo — provider folder mapping.
--
-- The Snowflake-native replacement for the hardcoded FOLDER_TO_PROVIDER /
-- PROVIDER_GLOB dicts in scripts/lambda_function_glue.py. Rows here are
-- generated from config/providers/*.yaml by
-- python/matchbot_snowflake/config_bridge.py (MERGE INTO, idempotent) —
-- never hand-edited, so this table can't drift from the YAML the way the
-- two Lambda dicts can from each other.

USE DATABASE MATCHBOT;
USE SCHEMA RILDS;

CREATE TABLE IF NOT EXISTS PROVIDER_FOLDER_MAP (
    folder_name         VARCHAR(100) NOT NULL PRIMARY KEY,  -- S3 key's provider-folder segment, e.g. 'ride_enrollment'
    provider_id         VARCHAR(100) NOT NULL,              -- ProviderConfig.provider_id
    provider_code       VARCHAR(20)  NOT NULL,               -- ProviderConfig.provider_code
    dataset_name        VARCHAR(100) NOT NULL,               -- ProviderConfig.dataset_name
    file_glob           VARCHAR(200) NOT NULL,               -- ProviderConfig.file_glob
    external_id_column  VARCHAR(50),                         -- ProviderConfig.external_id_column, e.g. 'sasid'
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Tracks which staged files have already been processed, so the polling
-- Task never reprocesses a file it already ran.
CREATE TABLE IF NOT EXISTS INGEST_LOG (
    file_path        VARCHAR(1000) NOT NULL PRIMARY KEY,  -- relative path within the stage, e.g. data/input/ride_enrollment/foo.csv
    provider_id      VARCHAR(100) NOT NULL,
    first_seen_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    status           VARCHAR(20)  NOT NULL DEFAULT 'PENDING',  -- PENDING | SUCCESS | FAILED
    pipeline_run_id  NUMBER,
    error            VARCHAR(4000)
);
