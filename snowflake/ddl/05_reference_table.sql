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
    address2                 VARCHAR(200),
    city                     VARCHAR(100),
    state                    VARCHAR(20),
    zip                      VARCHAR(20)
);

-- Clustering keys mirroring the Postgres composite blocking indexes.
ALTER TABLE RILDS_REFERENCE CLUSTER BY (last_name8, birth_date);
