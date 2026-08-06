-- Snowflake demo — reference table.
--
-- Mirrors storage/schema.py's rilds_reference exactly (67 columns) so the
-- one-time export from Postgres (python/matchbot_snowflake/export/
-- export_rilds_reference.py) loads without any column remapping, and so
-- parity diffs against the AWS demo compare like-for-like. idcol_id is a
-- natural key (proddb's identifiers_idcollection.id) — NOT auto-generated
-- here either, same as Postgres.
--
-- This table is populated ONCE via the export script and never live-synced
-- — see docs/snowflake-implementation-plan.md's "Reference data" scope note.

USE DATABASE MATCHBOT;
USE SCHEMA RILDS;

CREATE TABLE IF NOT EXISTS RILDS_REFERENCE (
    idcol_id                NUMBER PRIMARY KEY,
    person_id               NUMBER,
    dataset_id              NUMBER,

    -- CoreIdentifiers
    first_name              VARCHAR(52),
    middle_name             VARCHAR(50),
    last_name               VARCHAR(52),
    birth_date               DATE,
    gender                   VARCHAR(10),
    ssn                      VARCHAR(11),

    -- ModelIdentifiers (provider-issued ids, all strings)
    apprentice_id            VARCHAR(50),
    brown_id                 VARCHAR(50),
    bryant_id                VARCHAR(50),
    ccri_id                  VARCHAR(50),
    college_board_id         VARCHAR(50),
    dcyf_id                  VARCHAR(50),
    dlt_ern                  VARCHAR(50),
    employri_id              VARCHAR(50),
    ged_id                   VARCHAR(50),
    jwu_id                   VARCHAR(50),
    kidsnet_child_id         VARCHAR(50),
    laces_id                 VARCHAR(50),
    laces_staff_id           VARCHAR(50),
    laces_student_id         VARCHAR(50),
    lasid                    VARCHAR(50),
    nspid                    VARCHAR(50),
    ods                      VARCHAR(50),
    ric_id                   VARCHAR(50),
    ride_cert_id             VARCHAR(50),
    ridoh_lead_id            VARCHAR(50),
    risd_id                  VARCHAR(50),
    rjri_id                  VARCHAR(50),
    rwu_id                   VARCHAR(50),
    salve_id                 VARCHAR(50),
    sasid                    VARCHAR(10),
    uri_id                   VARCHAR(50),
    voter_id                 VARCHAR(50),
    workforce_id             VARCHAR(50),
    providencecollege_id     VARCHAR(50),
    netech_id                VARCHAR(50),

    -- DerivedIdentifiers (computed in proddb, stored verbatim)
    first_name_std           VARCHAR(52),
    first_name_metaphone1    VARCHAR(50),
    first_name_metaphone2    VARCHAR(50),
    first_name_transposed    VARCHAR(52),
    first_initial            VARCHAR(1),
    middle_name_std          VARCHAR(50),
    middle_initial           VARCHAR(1),
    last_name_std            VARCHAR(52),
    last_name_metaphone1     VARCHAR(50),
    last_name_metaphone2     VARCHAR(50),
    last_name_transposed     VARCHAR(52),
    last_name_suffix         VARCHAR(10),
    last_initial             VARCHAR(1),
    last_name8               VARCHAR(8),
    full_name_std            VARCHAR(150),
    full_name_metaphone      VARCHAR(100),
    full_name_transposed     VARCHAR(150),
    full_name_dob            VARCHAR(160),
    birth_month              SMALLINT,
    birth_day                SMALLINT,
    birth_year               SMALLINT,
    ssn4                     VARCHAR(4),

    -- Address (one row per idcol_id)
    address_source           VARCHAR(100),
    address1                 VARCHAR(200),
    address1_std              VARCHAR(200),            -- standardized address1; assumed precomputed
                                                          -- upstream, same contract as first_name_std
                                                          -- above, for the name+address matcher tiers
    address2                 VARCHAR(200),
    city                     VARCHAR(100),
    state                    VARCHAR(20),
    zip                      VARCHAR(20),
    zip5                      VARCHAR(5),               -- zip truncated to 5 digits; same contract as
                                                          -- address1_std above

    -- Precomputed hash columns for the deterministic matchers with 2+ keys
    -- (config/global.yaml's matcher chain) — RILDS_STAGE computes these
    -- same 4 columns per row via provider_sql.py/derive_sql.py's
    -- deterministic_hash_sql(); matchers/deterministic.py then joins on
    -- s.<hash_col> = r.<hash_col> instead of a multi-column AND chain.
    --
    -- CONTRACT for whoever populates/refreshes this table (a separate
    -- process, out of scope here): each hash MUST be computed with the
    -- EXACT formula below, or a genuinely matching pair of records will
    -- hash to different values and silently never match via these
    -- matchers. This is not "assumed precomputed the same way" as a
    -- convention (like address1_std/zip5's contract below) — it is a
    -- hard requirement, since a hash is only useful if both sides compute
    -- it identically.
    --
    -- Formula (see derive_sql.py's deterministic_hash_sql/HASH_COLUMN_BY_KEYS
    -- for the canonical Snowflake SQL implementation of this exact logic):
    --   For a hash's key list [k1, k2, ...] (order matters, see below):
    --   1. Normalize each key: for a string-typed key, TRIM(UPPER(value));
    --      for birth_date specifically, use the raw DATE value cast to
    --      text (no case/trim transform applies to a date).
    --   2. If ANY key's normalized value is NULL or empty/blank, the
    --      resulting hash is NULL (never hash a partially-missing key —
    --      two rows both missing the same field must not collide into a
    --      false match).
    --   3. Concatenate the normalized values with a literal '|' delimiter
    --      and hash with SHA2(concatenated_string, 256) (i.e. the
    --      64-character hex SHA-256 digest).
    --   Column -> exact key list, in order (do not reorder):
    --     name_ssn4_hash:            [first_name_std, last_name_std, ssn4]
    --     name_dob_hash:             [first_name_std, last_name_std, birth_date]
    --     name_addr_city_state_hash: [first_name_std, last_name_std, address1_std, city, state]
    --     name_addr_zip_hash:             [first_name_std, last_name_std, address1_std, zip5]
    -- Example (name_dob_hash): SHA2(TRIM(UPPER(first_name_std)) || '|' ||
    --   TRIM(UPPER(last_name_std)) || '|' || birth_date::VARCHAR, 256)
    --
    -- (fn_addr_hash — first_name_std, address1_std, state, zip5 — was
    -- removed along with deterministic_fn_addr, dropped as too loose a
    -- last-resort tier; reintroduced as firstname_addr_state_zip_hash,
    -- then dropped again along with lastname_addr_state_zip_hash per
    -- explicit request — see the DROP COLUMN migration below.
    -- name_addr_full_hash/name_addr_street_zip_hash/name_addr_hash were
    -- dropped earlier for the same household-collision reasoning — see
    -- the DROP COLUMN migration further below. name_state_zip_hash (rule
    -- 6: first_name_std, last_name_std, state, zip5) was renamed to
    -- name_addr_zip_hash when rule 6 changed to first_name_std,
    -- last_name_std, address1_std, zip5 — see the RENAME COLUMN
    -- migration further below.)
    name_ssn4_hash            VARCHAR(64),
    name_dob_hash             VARCHAR(64),
    name_addr_city_state_hash VARCHAR(64),
    name_addr_zip_hash             VARCHAR(64)
);

-- ADD COLUMN IF NOT EXISTS for already-deployed accounts (CREATE TABLE IF
-- NOT EXISTS above is a no-op against an existing RILDS_REFERENCE). Adding
-- these columns does not populate them for existing rows — address1_std/
-- zip5/the hash columns are assumed precomputed upstream (same one-time-
-- export contract as first_name_std/last_name_std, and for the hash
-- columns specifically, the exact formula documented above); existing
-- rows read NULL here until whatever process populates this table
-- provides them.
ALTER TABLE RILDS_REFERENCE ADD COLUMN IF NOT EXISTS address1_std VARCHAR(200);
ALTER TABLE RILDS_REFERENCE ADD COLUMN IF NOT EXISTS zip5 VARCHAR(5);
ALTER TABLE RILDS_REFERENCE ADD COLUMN IF NOT EXISTS name_ssn4_hash VARCHAR(64);
ALTER TABLE RILDS_REFERENCE ADD COLUMN IF NOT EXISTS name_dob_hash VARCHAR(64);
ALTER TABLE RILDS_REFERENCE ADD COLUMN IF NOT EXISTS name_addr_city_state_hash VARCHAR(64);

-- deterministic_fn_addr (first_name + address only, no last name) was
-- removed from config/global.yaml as too loose a last-resort tier — its
-- backing column is dropped rather than left as unused dead weight.
ALTER TABLE RILDS_REFERENCE DROP COLUMN IF EXISTS fn_addr_hash;

-- name_state_zip tier added per explicit request — same reasoning as the
-- hash columns above; existing rows read NULL here until the
-- reference-population process backfills them. (Column later renamed to
-- name_addr_zip_hash; see the RENAME COLUMN migration below.)
ALTER TABLE RILDS_REFERENCE ADD COLUMN IF NOT EXISTS name_state_zip_hash VARCHAR(64);

-- deterministic_name_addr_full, deterministic_name_addr_street_zip, and
-- deterministic_name_addr were dropped from config/global.yaml — not
-- part of the current 6-rule chain. Their backing hash columns are
-- dropped rather than left as unused dead weight.
ALTER TABLE RILDS_REFERENCE DROP COLUMN IF EXISTS name_addr_full_hash;
ALTER TABLE RILDS_REFERENCE DROP COLUMN IF EXISTS name_addr_street_zip_hash;
ALTER TABLE RILDS_REFERENCE DROP COLUMN IF EXISTS name_addr_hash;

-- Rule 6 (deterministic_name_state_zip: first_name_std, last_name_std,
-- state, zip5) was changed to deterministic_name_addr_zip (first_name_std,
-- last_name_std, address1_std, zip5 — state swapped for address1_std)
-- per explicit request. RENAME (not drop+add): preserves any
-- already-populated values under the new name — though note the VALUES
-- THEMSELVES are now stale (computed from the old key set) until the
-- reference-population process recomputes them with the new formula;
-- this migration only renames the column.
ALTER TABLE RILDS_REFERENCE RENAME COLUMN name_state_zip_hash TO name_addr_zip_hash;

-- deterministic_lastname_addr_state_zip and
-- deterministic_firstname_addr_state_zip (each dropping one of the two
-- name fields, anchored only by state+zip) were removed from
-- config/global.yaml per explicit request — both carried real
-- household-collision risk. Their backing hash columns are dropped
-- rather than left as unused dead weight.
ALTER TABLE RILDS_REFERENCE DROP COLUMN IF EXISTS lastname_addr_state_zip_hash;
ALTER TABLE RILDS_REFERENCE DROP COLUMN IF EXISTS firstname_addr_state_zip_hash;

-- Clustering keys mirroring the Postgres composite blocking indexes.
ALTER TABLE RILDS_REFERENCE CLUSTER BY (last_name8, birth_date);
