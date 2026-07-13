-- Snowflake demo — file format + external stage.
--
-- File format matches RIDE's CSV shape today (comma-delimited, header
-- present) — see config/providers/provider_ride_enrollment.yaml
-- (delimiter: ",", has_header: true). A second provider's file format
-- would be a second CREATE FILE FORMAT, not a change to this one.

USE DATABASE MATCHBOT;
USE SCHEMA RILDS;

-- NOTE on ragged rows (unescaped commas inside unquoted fields, e.g. a
-- college name "University of Maine, Farmington", shifting every later
-- column — same failure mode storage/schema.py's rilds_land_rejects
-- comment describes for the AWS pipeline's ParseStage): live validation
-- showed ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE does NOT catch rows with
-- MORE fields than expected (only fewer) — COPY_HISTORY and
-- VALIDATION_MODE = 'RETURN_ALL_ERRORS' both reported zero errors on a
-- file with 15 such rows. Ragged-row detection is instead done ourselves,
-- generically, in matchbot_snowflake/land_sql.py — counting delimiters per
-- raw line before trusting it, with the expected count read from each
-- file's own header (not hardcoded). ERROR_ON_COLUMN_COUNT_MISMATCH is
-- left TRUE anyway as defense-in-depth for the *fewer-fields* case it does
-- catch, but is not relied on as the primary protection.
CREATE FILE FORMAT IF NOT EXISTS CSV_PROVIDER_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL')
    EMPTY_FIELD_AS_NULL = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
    COMMENT = 'Matches provider CSV shape (comma-delimited, header row present)';

-- Two single-column "raw line" formats used by land_sql.py's dynamic land
-- step: FIELD_DELIMITER = NONE means each row is read as one undivided
-- text value ($1), which Python then splits and counts itself — this is
-- what makes header-driven table creation and generic ragged-row
-- detection possible (see land_sql.py's module docstring). Differ only in
-- SKIP_HEADER: reading the header itself requires seeing it; reading data
-- rows requires skipping it.
CREATE FILE FORMAT IF NOT EXISTS RAW_LINE_FORMAT_WITH_HEADER
    TYPE = 'CSV'
    FIELD_DELIMITER = NONE
    SKIP_HEADER = 0
    RECORD_DELIMITER = '\n'
    COMMENT = 'One raw text column per line, header row included — used to read a file''s header for dynamic land-table creation';

CREATE FILE FORMAT IF NOT EXISTS RAW_LINE_FORMAT_SKIP_HEADER
    TYPE = 'CSV'
    FIELD_DELIMITER = NONE
    SKIP_HEADER = 1
    RECORD_DELIMITER = '\n'
    COMMENT = 'One raw text column per line, header row skipped — used for generic per-line field-count validation and splitting';

-- Directory-table-enabled external stage: DIRECTORY(@INPUT_STAGE) lists
-- files cheaply without a full S3 LIST call each poll. Points at the same
-- prefix the storage integration is scoped to.
CREATE STAGE IF NOT EXISTS INPUT_STAGE
    STORAGE_INTEGRATION = MATCHBOT_S3_INT
    URL = 's3://<REPLACE_WITH_BUCKET>/data/input/'
    FILE_FORMAT = CSV_PROVIDER_FORMAT
    DIRECTORY = (ENABLE = TRUE, AUTO_REFRESH = TRUE)
    COMMENT = 'Read-only view of the MatchBot S3 input dropzone';

-- Validation for build step 1 (see docs/snowflake-implementation-plan.md):
-- after uploading a RIDE CSV to s3://<bucket>/data/input/ride_enrollment/,
-- confirm it's visible via:
--   SELECT * FROM DIRECTORY(@INPUT_STAGE);
