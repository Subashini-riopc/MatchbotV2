-- Snowflake demo — S3 storage integration.
--
-- This is the ONLY AWS touchpoint in the entire Snowflake demo: a scoped,
-- keyless integration granting Snowflake read access to the same S3
-- dropzone prefix the AWS demo already uses for incoming provider files.
-- No Lambda, no EventBridge, no Glue, no ECS — S3 is purely a file
-- dropzone here.
--
-- Replace the placeholders below with your actual bucket/prefix. This demo
-- reuses an EXISTING IAM role already trusted by another Snowflake storage
-- integration (a separate, dedicated integration object — MATCHBOT_S3_INT —
-- for this demo's bucket, rather than widening the other integration's
-- STORAGE_ALLOWED_LOCATIONS). The role's S3 permissions need this demo's
-- bucket/prefix added (see snowflake/README.md), and its trust policy needs
-- this integration's STORAGE_AWS_IAM_USER_ARN / STORAGE_AWS_EXTERNAL_ID
-- added as an additional trusted principal/external-id condition alongside
-- whatever the other integration already added there — Snowflake generates
-- both only after this CREATE STORAGE INTEGRATION statement first runs.

CREATE STORAGE INTEGRATION IF NOT EXISTS MATCHBOT_S3_INT
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = '<REPLACE_WITH_IAM_ROLE_ARN>'
    STORAGE_ALLOWED_LOCATIONS = ('s3://<REPLACE_WITH_BUCKET>/data/input/')
    COMMENT = 'Read-only access to the MatchBot S3 dropzone (input files only)';

-- After running the CREATE STORAGE INTEGRATION above, run:
--   DESC INTEGRATION MATCHBOT_S3_INT;
-- and use the returned STORAGE_AWS_IAM_USER_ARN / STORAGE_AWS_EXTERNAL_ID
-- to finish trusting this integration in the IAM role's trust policy
-- before creating the external stage in 02_file_formats_and_stage.sql.
