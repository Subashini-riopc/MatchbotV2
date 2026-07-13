-- Snowflake demo — matched, error, and audit tables.
--
-- RILDS_MATCHED / RILDS_ERROR denormalize the same identity columns as
-- RILDS_STAGE (mirrors storage/schema.py's *_identity_columns() reuse
-- across rilds_stage/rilds_matched/rilds_error) so a reviewer can see the
-- attributes a row matched or failed on without joining back to stage.
--
-- RILDS_AUDIT mirrors the Postgres rilds_audit run-log shape, so the same
-- counts/timings/match-rate reporting works identically across both demos.

USE DATABASE MATCHBOT;
USE SCHEMA RILDS;

CREATE TABLE IF NOT EXISTS RILDS_MATCHED (
    id                     NUMBER IDENTITY PRIMARY KEY,
    pipeline_run_id        NUMBER NOT NULL,
    stage_id               NUMBER NOT NULL,          -- FK -> RILDS_STAGE.id
    idcol_id               NUMBER NOT NULL,          -- FK -> RILDS_REFERENCE.idcol_id
    match_score            NUMBER(5, 4) NOT NULL,
    match_method           VARCHAR(20) NOT NULL,     -- EXACT_SASID / EXACT / ...

    first_name             VARCHAR(52),
    middle_name            VARCHAR(50),
    last_name              VARCHAR(52),
    birth_date             DATE,
    gender                 VARCHAR(10),
    first_name_std         VARCHAR(52),
    last_name_std          VARCHAR(52),
    first_name_metaphone1  VARCHAR(50),
    last_name_metaphone1   VARCHAR(50),
    last_name8             VARCHAR(8),
    birth_year             SMALLINT,
    birth_month            SMALLINT,
    birth_day              SMALLINT,
    rilds_id               VARCHAR(50),
    lasid                  VARCHAR(50),
    ssn                    VARCHAR(11),
    address1               VARCHAR(200),
    address2               VARCHAR(200),
    city                   VARCHAR(100),
    state                  VARCHAR(20),
    zip                    VARCHAR(20),

    matched_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    matched_by             VARCHAR(100) DEFAULT 'system'
);

CREATE TABLE IF NOT EXISTS RILDS_ERROR (
    id                     NUMBER IDENTITY PRIMARY KEY,
    pipeline_run_id        NUMBER NOT NULL,
    stage_id               NUMBER NOT NULL,          -- FK -> RILDS_STAGE.id
    decision               VARCHAR(20) NOT NULL,     -- NO_MATCH / LOW_CONFIDENCE
    match_score            NUMBER(5, 4),
    reason                 VARCHAR(4000),

    first_name             VARCHAR(52),
    middle_name            VARCHAR(50),
    last_name              VARCHAR(52),
    birth_date             DATE,
    gender                 VARCHAR(10),
    first_name_std         VARCHAR(52),
    last_name_std          VARCHAR(52),
    first_name_metaphone1  VARCHAR(50),
    last_name_metaphone1   VARCHAR(50),
    last_name8             VARCHAR(8),
    birth_year             SMALLINT,
    birth_month            SMALLINT,
    birth_day              SMALLINT,
    rilds_id               VARCHAR(50),
    lasid                  VARCHAR(50),
    ssn                    VARCHAR(11),
    address1               VARCHAR(200),
    address2               VARCHAR(200),
    city                   VARCHAR(100),
    state                  VARCHAR(20),
    zip                    VARCHAR(20),

    created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RILDS_AUDIT (
    id                 NUMBER IDENTITY PRIMARY KEY,
    run_uid            VARCHAR(64) NOT NULL UNIQUE,
    provider_code      VARCHAR(20) NOT NULL,
    dataset_name       VARCHAR(100),
    runtime            VARCHAR(32) NOT NULL DEFAULT 'snowflake',
    source_uri         VARCHAR(1000),
    status             VARCHAR(16) NOT NULL,
    duration_seconds   FLOAT,
    match_rate         FLOAT,
    rows_received      NUMBER,
    rows_rejected      NUMBER,
    rows_cleansed      NUMBER,
    rows_landed        NUMBER,
    rows_staged        NUMBER,
    rows_matched       NUMBER,
    rows_unmatched     NUMBER,
    rows_skipped       NUMBER,
    stage_timings      VARIANT,
    dq_metrics         VARIANT,
    error              VARCHAR(4000),
    started_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    finished_at        TIMESTAMP_NTZ
);
