"""Lambda function — S3 upload -> Snowflake RUN_MATCH_PIPELINE trigger.

Triggered by EventBridge when any file is uploaded to:
  s3://rilds/data/input/<provider_folder>/<filename>

The function:
  1. Extracts the S3 key from the event
  2. Derives the relative path Snowflake's stage expects
     (data/input/<folder>/<filename> -> <folder>/<filename>, matching
     run_pipeline.py's own docstring example and how MATCHBOT_INPUT_STAGE
     is mounted: stage_file_path = f"MATCHBOT_INPUT_STAGE/{file_path}")
  3. Calls CALL RUN_MATCH_PIPELINE('<folder>/<filename>') via the Snowflake
     Python connector

No folder->provider / glob mapping lives here (unlike
scripts/lambda_function_glue.py) — RUN_MATCH_PIPELINE resolves the
provider itself from PROVIDER_FOLDER_MAP (keyed on (folder_name,
file_glob), see snowflake/ddl/03_provider_folder_map.sql) and returns a
"SKIPPED — no provider configured ..." result for anything unconfigured,
so there is only one place (Snowflake config) that needs updating when
onboarding a new provider or file type, not two hardcoded Python dicts.

Independent of scripts/lambda_function.py (the ECS trigger) and
scripts/lambda_function_glue.py (the Glue trigger) — neither of those is
modified by or affected by this file; all three can be deployed side by
side against the same S3 bucket/EventBridge rule if desired.

Environment variables (set in Lambda configuration):
  SNOWFLAKE_ACCOUNT       Account identifier, e.g. 'SQLXBJT-NVB56269'
  SNOWFLAKE_USER          Service account username (not a personal login)
  SNOWFLAKE_PRIVATE_KEY_SECRET_ARN
                          Secrets Manager ARN holding the PEM-encoded RSA
                          private key for key-pair auth (no password, no
                          MFA prompt — required since Lambda can't complete
                          an interactive/browser OAuth flow). The secret
                          value is the raw PEM text; if the key itself is
                          passphrase-protected, also set
                          SNOWFLAKE_PRIVATE_KEY_PASSPHRASE_SECRET_ARN.
  SNOWFLAKE_WAREHOUSE     Warehouse to run the CALL under, e.g. 'MATCHBOT_DEMO_WH'
  SNOWFLAKE_DATABASE      'MATCHBOT'
  SNOWFLAKE_SCHEMA        'RILDS'
  SNOWFLAKE_ROLE          Optional; role to use for the session

One-time setup this Lambda depends on (run by a Snowflake admin, not by
this code):
  1. Generate an RSA key pair:
       openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
       openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
  2. Create or alter a SERVICE (non-human) user and attach the public key:
       CREATE USER IF NOT EXISTS MATCHBOT_LAMBDA_SVC;
       ALTER USER MATCHBOT_LAMBDA_SVC SET RSA_PUBLIC_KEY='<contents of rsa_key.pub, header/footer stripped>';
       GRANT ROLE <appropriate role> TO USER MATCHBOT_LAMBDA_SVC;
  3. Store rsa_key.p8's contents (the PRIVATE key) in AWS Secrets Manager,
     and grant this Lambda's execution role secretsmanager:GetSecretValue
     on that secret's ARN.
  4. Grant this Lambda's execution role s3:GetObject on the input bucket
     (only needed if this function is ever extended to read file content
     directly — today it only reads the S3 event, not the file itself).
"""

from __future__ import annotations

import json
import logging
import os

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_secrets_client = None


def _get_secrets_client():
    global _secrets_client
    if _secrets_client is None:
        _secrets_client = boto3.client("secretsmanager")
    return _secrets_client


def _load_secret(secret_arn: str) -> str:
    response = _get_secrets_client().get_secret_value(SecretId=secret_arn)
    return response["SecretString"]


def _connect_to_snowflake():
    """Open a Snowflake connection using key-pair auth (no MFA/browser
    prompt possible from Lambda — see module docstring's setup steps).
    """
    import snowflake.connector
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    private_key_pem = _load_secret(os.environ["SNOWFLAKE_PRIVATE_KEY_SECRET_ARN"])
    passphrase_secret_arn = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE_SECRET_ARN")
    passphrase = _load_secret(passphrase_secret_arn).encode() if passphrase_secret_arn else None

    private_key = serialization.load_pem_private_key(
        private_key_pem.encode(), password=passphrase, backend=default_backend()
    )
    private_key_der = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=private_key_der,
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )


def _relative_file_path(key: str) -> str | None:
    """'data/input/risos_voter/Voter_032026.txt' -> 'risos_voter/Voter_032026.txt'.

    Matches run_pipeline.py's own docstring example and how
    MATCHBOT_INPUT_STAGE is mounted (stage_file_path =
    f"MATCHBOT_INPUT_STAGE/{file_path}") — the same relative-path shape
    used throughout manual CALL RUN_MATCH_PIPELINE(...) invocations.
    Returns None if the key doesn't have the expected data/input/ prefix.
    """
    marker = "data/input/"
    idx = key.find(marker)
    if idx == -1:
        return None
    return key[idx + len(marker) :]


def lambda_handler(event: dict, context: object) -> dict:
    logger.info("Event received: %s", json.dumps(event))

    # --- Extract S3 details from EventBridge event --------------------------
    detail = event.get("detail", {})
    bucket = detail.get("bucket", {}).get("name", "")
    key = detail.get("object", {}).get("key", "")

    if not bucket or not key:
        logger.error("Missing bucket or key in event: %s", event)
        return {"statusCode": 400, "body": "Missing bucket or key"}

    file_path = _relative_file_path(key)
    if file_path is None:
        logger.info("Skipping — key does not match expected structure: %s", key)
        return {"statusCode": 200, "body": "Skipped — unexpected key structure"}

    logger.info("Calling RUN_MATCH_PIPELINE for file_path=%r (bucket=%s)", file_path, bucket)

    try:
        conn = _connect_to_snowflake()
        try:
            cursor = conn.cursor()
            cursor.execute("CALL RUN_MATCH_PIPELINE(%s)", (file_path,))
            result = cursor.fetchone()
            summary = result[0] if result else "(no result returned)"
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 - surface any connector/SQL error as a 500
        logger.error("RUN_MATCH_PIPELINE call failed: %s", exc)
        return {"statusCode": 500, "body": f"RUN_MATCH_PIPELINE call failed: {exc}"}

    logger.info("RUN_MATCH_PIPELINE result: %s", summary)

    return {
        "statusCode": 200,
        "body": json.dumps({"file_path": file_path, "result": summary}),
    }
