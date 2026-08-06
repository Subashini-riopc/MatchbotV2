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

    -- Precomputed hash columns for the deterministic matchers with 2+ keys
    -- (see config/global.yaml's matcher chain) — collapse a multi-column
    -- exact-equality join (e.g. name_dob's 3-column AND) into a single
    -- VARCHAR(64) SHA2-256 hex digest comparison. NULL whenever any input
    -- key is missing/blank (never hash a partially-missing key — see
    -- derive_sql.py's deterministic_hash_sql, THE single source of truth
    -- for the exact formula). RILDS_REFERENCE must compute these same 4
    -- columns identically (see that table's DDL) or matching rows will
    -- hash differently and silently never match via these matchers.
    -- (fn_addr_hash — first_name_std, address1_std, state, zip5 — was
    -- removed along with deterministic_fn_addr, dropped as too loose a
    -- last-resort tier; reintroduced as firstname_addr_state_zip_hash,
    -- then dropped again along with lastname_addr_state_zip_hash — see
    -- the DROP COLUMN migration further below; neither is part of the
    -- current 6-rule chain. name_addr_full_hash/name_addr_street_zip_hash/
    -- name_addr_hash were dropped for the same household-collision
    -- reasoning even earlier. name_state_zip_hash (rule 6: first_name_std,
    -- last_name_std, state, zip5) was renamed to name_addr_zip_hash when
    -- rule 6 changed to first_name_std, last_name_std, address1_std,
    -- zip5 (state swapped for address1_std) — see the RENAME COLUMN
    -- migration further below.)
    name_ssn4_hash            VARCHAR(64),             -- keys: first_name_std, last_name_std, ssn4
    name_dob_hash             VARCHAR(64),             -- keys: first_name_std, last_name_std, birth_date
    name_addr_city_state_hash VARCHAR(64),             -- keys: first_name_std, last_name_std, address1_std, city, state
    name_addr_zip_hash             VARCHAR(64),        -- keys: first_name_std, last_name_std, address1_std, zip5

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

-- Hash columns (see the column-level comments above) added after initial
-- deployment — same reasoning as ssn4/address1_std/zip5 above.
ALTER TABLE RILDS_STAGE ADD COLUMN IF NOT EXISTS name_ssn4_hash VARCHAR(64);
ALTER TABLE RILDS_STAGE ADD COLUMN IF NOT EXISTS name_dob_hash VARCHAR(64);
ALTER TABLE RILDS_STAGE ADD COLUMN IF NOT EXISTS name_addr_city_state_hash VARCHAR(64);

-- deterministic_fn_addr (first_name + address only, no last name) was
-- removed from config/global.yaml as too loose a last-resort tier — its
-- backing column is dropped rather than left as unused dead weight.
ALTER TABLE RILDS_STAGE DROP COLUMN IF EXISTS fn_addr_hash;

-- name_state_zip tier added per explicit request — same reasoning as the
-- ssn4/hash columns above. (Column later renamed to name_addr_zip_hash;
-- see the RENAME COLUMN migration below.)
ALTER TABLE RILDS_STAGE ADD COLUMN IF NOT EXISTS name_state_zip_hash VARCHAR(64);

-- deterministic_name_addr_full, deterministic_name_addr_street_zip, and
-- deterministic_name_addr were dropped from config/global.yaml — not
-- part of the current 6-rule chain. Their backing hash columns are
-- dropped rather than left as unused dead weight.
ALTER TABLE RILDS_STAGE DROP COLUMN IF EXISTS name_addr_full_hash;
ALTER TABLE RILDS_STAGE DROP COLUMN IF EXISTS name_addr_street_zip_hash;
ALTER TABLE RILDS_STAGE DROP COLUMN IF EXISTS name_addr_hash;

-- Rule 6 (deterministic_name_state_zip: first_name_std, last_name_std,
-- state, zip5) was changed to deterministic_name_addr_zip (first_name_std,
-- last_name_std, address1_std, zip5 — state swapped for address1_std) per
-- explicit request. RENAME (not drop+add): preserves any already-
-- populated RILDS_REFERENCE values under the new name rather than losing
-- them — though note the VALUES THEMSELVES are now stale (computed from
-- the old key set) until the reference-population process recomputes
-- them with the new formula; this migration only renames the column.
ALTER TABLE RILDS_STAGE RENAME COLUMN name_state_zip_hash TO name_addr_zip_hash;

-- deterministic_lastname_addr_state_zip and
-- deterministic_firstname_addr_state_zip (each dropping one of the two
-- name fields, anchored only by state+zip) were removed from
-- config/global.yaml per explicit request — both carried real
-- household-collision risk (people sharing a last name + address, or a
-- first name + address, could false-match). Their backing hash columns
-- are dropped rather than left as unused dead weight.
ALTER TABLE RILDS_STAGE DROP COLUMN IF EXISTS lastname_addr_state_zip_hash;
ALTER TABLE RILDS_STAGE DROP COLUMN IF EXISTS firstname_addr_state_zip_hash;

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
