"""Generate PROVIDER_FOLDER_MAP rows from the existing matchbot config.

Replaces the hardcoded FOLDER_TO_PROVIDER / PROVIDER_GLOB dicts in
scripts/lambda_function_glue.py with a table generated straight from
config/providers/*.yaml, so onboarding a new provider never requires
hand-editing a second, driftable mapping.

The "folder name" a provider's files land under is derived the same way the
existing Lambda triggers assume: the S3 key shape is
``data/input/<folder>/<filename>``, and today the folder name is identical
to ``provider_id`` for every configured provider (see
config/providers/provider_ride_enrollment.yaml: provider_id=ride_enrollment,
files land under data/input/ride_enrollment/). If a future provider ever
needs a folder name that differs from its provider_id, add an explicit
``s3_folder`` field to ProviderConfig rather than guessing here.
"""

from __future__ import annotations

from matchbot_snowflake.config_models import AppConfig, ProviderConfig, load_config


def provider_folder_name(provider: ProviderConfig) -> str:
    """The S3 folder segment this provider's files land under.

    Today this is always provider_id — see module docstring.
    """
    return provider.provider_id


def build_provider_folder_map_rows(app_config: AppConfig) -> list[dict[str, str | None]]:
    """One row per configured provider, shaped for PROVIDER_FOLDER_MAP."""
    rows: list[dict[str, str | None]] = []
    for provider in app_config.providers.values():
        rows.append(
            {
                "folder_name": provider_folder_name(provider),
                "provider_id": provider.provider_id,
                "provider_code": provider.provider_code,
                "dataset_name": provider.dataset_name,
                "file_glob": provider.file_glob,
                "external_id_column": provider.external_id_column,
            }
        )
    return rows


def render_merge_sql(rows: list[dict[str, str | None]]) -> str:
    """Render one idempotent MERGE INTO statement for all provider rows.

    A MERGE (not a plain INSERT) so re-running this after a provider's YAML
    changes updates the existing row rather than erroring on the
    folder_name primary key, or leaving a stale row behind.
    """
    if not rows:
        return "-- no providers configured; nothing to merge"

    def _sql_literal(value: str | None) -> str:
        if value is None:
            return "NULL"
        return "'" + value.replace("'", "''") + "'"

    values_rows = []
    for row in rows:
        values_rows.append(
            "("
            + ", ".join(
                _sql_literal(row[col])
                for col in (
                    "folder_name",
                    "provider_id",
                    "provider_code",
                    "dataset_name",
                    "file_glob",
                    "external_id_column",
                )
            )
            + ")"
        )

    values_clause = ",\n    ".join(values_rows)

    return f"""MERGE INTO PROVIDER_FOLDER_MAP AS target
USING (
    SELECT * FROM VALUES
    {values_clause}
    AS src(folder_name, provider_id, provider_code, dataset_name, file_glob, external_id_column)
) AS source
ON target.folder_name = source.folder_name
WHEN MATCHED THEN UPDATE SET
    provider_id = source.provider_id,
    provider_code = source.provider_code,
    dataset_name = source.dataset_name,
    file_glob = source.file_glob,
    external_id_column = source.external_id_column,
    updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    folder_name, provider_id, provider_code, dataset_name, file_glob, external_id_column
) VALUES (
    source.folder_name, source.provider_id, source.provider_code,
    source.dataset_name, source.file_glob, source.external_id_column
);"""


def build_provider_folder_map_sql(config_dir: str) -> str:
    """Load config/providers/*.yaml and render the MERGE INTO SQL for it.

    Intended usage: run this once at deploy time (and again whenever
    config/providers/*.yaml changes), execute the returned SQL against
    Snowflake via the connector or a worksheet.
    """
    app_config = load_config(config_dir)
    rows = build_provider_folder_map_rows(app_config)
    return render_merge_sql(rows)
