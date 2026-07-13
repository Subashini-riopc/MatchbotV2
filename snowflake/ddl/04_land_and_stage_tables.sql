-- Snowflake demo — land rejects + stage tables.
--
-- <PROVIDER>_LAND tables (e.g. RIDE_LAND) are intentionally NOT defined
-- here — they're created dynamically at runtime by
-- matchbot_snowflake/land_sql.py, shaped from each incoming file's own
-- header row, mirroring storage/schema.py's build_land_table() ("Built
-- dynamically from the file's columns, so any provider works with zero
-- bespoke DDL"). An earlier version of this file hand-wrote a fixed
-- 36-column RIDE_LAND table — that was a real regression from parity with
-- the AWS pipeline (a new provider's differently-shaped file would have
-- needed new DDL), corrected once asked directly whether onboarding a new
-- provider required new code.
--
-- RILDS_LAND_REJECTS is the one land-related table still defined
-- statically here, since its shape never varies by provider (mirrors
-- storage/schema.py's single shared rilds_land_rejects) — a rejected row
-- is always stored as one verbatim raw line plus a reason, never split
-- into provider-specific columns.
--
-- RILDS_STAGE mirrors storage/schema.py's rilds_stage exactly, including the
-- current (post sasid->rilds_id rename) column set — see
-- docs/snowflake-implementation-plan.md for why rilds_id replaces sasid.

USE DATABASE MATCHBOT;
USE SCHEMA RILDS;

CREATE TABLE IF NOT EXISTS RILDS_LAND_REJECTS (
    id                NUMBER IDENTITY PRIMARY KEY,
    pipeline_run_id   NUMBER NOT NULL,
    provider_code     VARCHAR(20) NOT NULL,
    raw_line          VARCHAR(16777216) NOT NULL,
    reason            VARCHAR(4000) NOT NULL,
    created_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RILDS_STAGE (
    id                     NUMBER IDENTITY PRIMARY KEY,
    pipeline_run_id        NUMBER NOT NULL,
    provider_code          VARCHAR(20) NOT NULL,
    dataset_name           VARCHAR(100) NOT NULL,
    source_row_id          NUMBER NOT NULL,          -- FK -> <PROVIDER>_LAND.id (e.g. RIDE_LAND.id)

    -- identity columns (mirrors storage.schema._identity_columns())
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
    rilds_id               VARCHAR(50),              -- generic provider-issued strong id (was sasid)
    lasid                  VARCHAR(50),               -- kept, unused by any current provider/matcher
    ssn                    VARCHAR(11),
    address1               VARCHAR(200),
    address2               VARCHAR(200),
    city                   VARCHAR(100),
    state                  VARCHAR(20),
    zip                    VARCHAR(20),

    -- match-output columns
    idcol_id               NUMBER,                    -- FK -> RILDS_REFERENCE.idcol_id, NULL until matched
    match_score            NUMBER(5, 4),
    match_status           VARCHAR(20) DEFAULT 'PENDING',
    loaded_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Clustering keys mirroring the Postgres composite blocking indexes
-- (idx_stage_block_last8_dob / idx_stage_block_meta_year / etc. — see
-- _BLOCKING_INDEXES in storage/schema.py). Not required for correctness at
-- demo volumes; added for a fair perf comparison in the write-up.
ALTER TABLE RILDS_STAGE CLUSTER BY (last_name8, birth_date);
