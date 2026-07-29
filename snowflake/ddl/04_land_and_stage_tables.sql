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
    ssn4                   VARCHAR(4),                -- last 4 digits of ssn, derived (see derive_sql.ssn4_sql)
    address1               VARCHAR(200),
    address1_std           VARCHAR(200),              -- standardized address1, derived (see derive_sql.std_address_sql)
    address2               VARCHAR(200),
    city                   VARCHAR(100),
    state                  VARCHAR(20),
    zip                    VARCHAR(20),
    zip5                   VARCHAR(5),                -- zip truncated to 5 digits, derived (see derive_sql.std_zip_sql)

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

-- ssn4/address1_std/zip5 (name+address/name+ssn4 matcher tiers) added after
-- initial deployment — CREATE TABLE IF NOT EXISTS above is a no-op against
-- an already-deployed RILDS_STAGE, so these ADD COLUMN IF NOT EXISTS
-- statements are what actually bring an existing table up to date. Adding a
-- column is a metadata-only operation in Snowflake: existing rows are not
-- rewritten and get NULL for these columns (never derived retroactively —
-- re-run the affected files through RUN_MATCH_PIPELINE if backfilled
-- matching is needed for historical data).
ALTER TABLE RILDS_STAGE ADD COLUMN IF NOT EXISTS ssn4 VARCHAR(4);
ALTER TABLE RILDS_STAGE ADD COLUMN IF NOT EXISTS address1_std VARCHAR(200);
ALTER TABLE RILDS_STAGE ADD COLUMN IF NOT EXISTS zip5 VARCHAR(5);

-- RISOS_VOTERHISTORY_STAGE — NOT a <PROVIDER>_STAGE generalization of
-- RILDS_STAGE, and deliberately not shared with it. VoterHistory
-- (config/providers/provider_risos_voterhistory.yaml, matches_dataset:
-- false) never runs person-linkage matching, so it has no use for
-- RILDS_STAGE's match-output columns (idcol_id/match_score/match_status)
-- — those would sit permanently NULL for every row, and mixing a
-- never-matched dataset into the one table every matched provider's rows
-- live in would make "is this row matched, unmatched, or simply not a
-- matching candidate at all" ambiguous from the schema alone. One row per
-- (voter, election) pair the voter actually participated in — see
-- matchbot_snowflake/voter_history_sql.py for the unpivot that produces
-- this shape from RISOS_VOTERHISTORY_LAND's 8 wide election-slot columns.
CREATE TABLE IF NOT EXISTS RISOS_VOTERHISTORY_STAGE (
    id                NUMBER IDENTITY PRIMARY KEY,
    pipeline_run_id   NUMBER NOT NULL,
    source_row_id     NUMBER NOT NULL,          -- FK -> RISOS_VOTERHISTORY_LAND.id
    voter_id          VARCHAR(11),              -- zero-padded to 11, matches RISOS_VOTER_LAND's rilds_id width
    election_date     DATE,
    election_name     VARCHAR(200),
    vote_type         VARCHAR(10),
    precinct          VARCHAR(4),               -- zero-padded to 4, mirrors legacy's per-slot ImporterField
    party             VARCHAR(10),
    -- TRUE for exactly one row per voter with all 8 election slots empty
    -- (mirrors legacy's rilds_did_not_vote bulk-update) — election_date/
    -- election_name/vote_type/precinct/party are all NULL on that row.
    did_not_vote      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
