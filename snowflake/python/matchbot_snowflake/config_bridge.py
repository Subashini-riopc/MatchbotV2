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

import json

from matchbot_snowflake.config_models import AppConfig, MatcherSpec, ProviderConfig, load_config

# Derived attribute -> the canonical (provider-mappable) attribute it's
# computed from — ported verbatim from matchbot.pipeline.match's module of
# the same name (config_models.py is already a deliberate, hand-synced copy
# of the real matchbot package; see that module's docstring). Used only to
# resolve whether a matcher's key is actually satisfiable for a given
# provider — e.g. "first_name_std" isn't itself a column_mappings target,
# but it's derived from "first_name", so a rule keyed on first_name_std IS
# usable whenever the provider maps first_name.
_DERIVED_ATTRIBUTE_SOURCE: dict[str, str] = {
    "first_name_std": "first_name",
    "first_name_metaphone1": "first_name",
    "last_name_std": "last_name",
    "last_name_metaphone1": "last_name",
    "last_name8": "last_name",
    "birth_year": "birth_date",
    "birth_month": "birth_date",
    "birth_day": "birth_date",
    # address1_std/zip5/ssn4 were missing here — a real bug: every
    # address-anchored or ssn4-anchored deterministic rule
    # (deterministic_name_addr_city_state, deterministic_name_addr_zip,
    # deterministic_name_ssn4) was silently dropped from every provider's
    # resolved chain (and therefore its matching_identifiers/"Matched on"
    # email line), even for a provider like RISOS that genuinely maps
    # address1/zip/ssn. Caught via a live RISOS email showing only
    # voter_id/First Name/Last Name/Birth Date despite RISOS mapping
    # address1/city/state/zip. Fixed identically in the real AWS source
    # (matchbot.pipeline.match._DERIVED_ATTRIBUTE_SOURCE) this is ported
    # from.
    "address1_std": "address1",
    "zip5": "zip",
    "ssn4": "ssn",
    "rilds_id": "member_external_id",
}


def _source_attribute(attribute: str) -> str:
    return _DERIVED_ATTRIBUTE_SOURCE.get(attribute, attribute)


def filter_chain_by_provider_attributes(
    matcher_chain: list[MatcherSpec], mapped_attributes: set[str]
) -> list[MatcherSpec]:
    """Drop matchers whose required attribute(s) this provider never maps —
    ported verbatim from matchbot.pipeline.match.filter_chain_by_provider_attributes.
    A rule survives only if every one of its keys/comparison-attributes
    resolves (via _source_attribute) to something in mapped_attributes."""
    kept: list[MatcherSpec] = []
    for spec in matcher_chain:
        required = [_source_attribute(k) for k in spec.keys]
        required += [_source_attribute(c.attribute) for c in spec.comparisons]
        if required and all(attr in mapped_attributes for attr in required):
            kept.append(spec)
    return kept


# Canonical attribute -> display name, for reporting only — ported verbatim
# from matchbot.pipeline.match._ATTRIBUTE_DISPLAY_NAMES (also duplicated in
# notify_sql.py's matched_on_attributes for the same reason: this is
# reporting-only logic, same category as config_models.py's hand-synced
# copy). Falls back to a title-cased, underscore-stripped version of the raw
# attribute for anything not listed here.
_ATTRIBUTE_DISPLAY_NAMES: dict[str, str] = {
    "member_external_id": "External ID",
    "rilds_id": "External ID",
    "first_name": "First Name",
    "first_name_std": "First Name",
    "middle_name": "Middle Name",
    "last_name": "Last Name",
    "last_name_std": "Last Name",
    "birth_date": "Birth Date",
    "ssn": "SSN",
    "ssn4": "SSN",
    "gender": "Gender",
    "address1": "Address",
    "address1_std": "Address",
    "city": "City",
    "state": "State",
    "zip": "Zip",
    "zip5": "Zip",
}


def _display_name(attribute: str) -> str:
    return _ATTRIBUTE_DISPLAY_NAMES.get(attribute, attribute.replace("_", " ").title())


def matched_on_attributes(matcher_chain: list[MatcherSpec]) -> list[str]:
    """Human-readable, deduplicated attribute names the chain compares on —
    ported verbatim from matchbot.pipeline.match.matched_on_attributes."""
    seen: dict[str, None] = {}
    for spec in matcher_chain:
        for attr in spec.keys:
            seen.setdefault(_display_name(attr), None)
        for comparison in spec.comparisons:
            seen.setdefault(_display_name(comparison.attribute), None)
    return list(seen)


def matching_identifiers_for_provider(
    provider: ProviderConfig, app_config: AppConfig
) -> list[str]:
    """The PROVIDER_FOLDER_MAP.matching_identifiers value for one file type:
    which attributes the RESOLVED matcher chain actually compares on for
    this provider, given only the columns it maps — i.e.
    filter_chain_by_provider_attributes() + matched_on_attributes() applied
    together, the same two-step computation the AWS orchestrator
    (pipeline/orchestrator.py) already runs live for every pipeline run.
    Land-only file types (matches_dataset=False, e.g. RISOS VoterHistory)
    never run matching at all — empty list, not an attempt to compute one.

    The generic "External ID" label is replaced with this provider's own
    external_id_column verbatim (e.g. "sasid" for RIDE, "voter_id" for
    RISOS) when that rule is present — "External ID" alone doesn't tell a
    report reader which actual identifier RIDE vs. RISOS matched on;
    external_id_column is already sitting on this same PROVIDER_FOLDER_MAP
    row, so this just surfaces it instead of the coarse display label.
    """
    if not provider.matches_dataset:
        return []
    # combined_columns targets (e.g. RISOS's address1, built from
    # STREET_NUMBER + STREET_NAME) count as mapped too — a provider that
    # builds a canonical attribute via combine rather than a plain 1:1
    # column_mappings entry still genuinely has that attribute available
    # for matching. Missing this caused address-anchored rules to be
    # dropped again immediately after RISOS's address1 moved from
    # column_mappings to combined_columns — same class of bug as the
    # earlier missing address1_std/zip5/ssn4 entries in
    # _DERIVED_ATTRIBUTE_SOURCE, caught by this module's own regression
    # test rather than a live email this time.
    mapped_attributes = set(provider.column_mappings.values()) | set(
        provider.combined_columns.keys()
    )
    chain = filter_chain_by_provider_attributes(
        app_config.global_config.matching.matchers, mapped_attributes
    )
    identifiers = matched_on_attributes(chain)
    if provider.external_id_column:
        identifiers = [
            provider.external_id_column if label == "External ID" else label
            for label in identifiers
        ]
    return identifiers


def provider_folder_name(provider: ProviderConfig) -> str:
    """The S3 folder segment this file type's files land under.

    provider.s3_folder if explicitly set (required for a multi_file
    provider's second+ file type, whose own provider_id is NOT the shared
    folder — e.g. risos_voterhistory's files land under risos_voter, not
    risos_voterhistory), else provider_id (true for every single-file-type
    provider so far, and for a multi_file provider's first/primary file
    type).
    """
    return provider.s3_folder or provider.provider_id


def build_provider_folder_map_rows(
    app_config: AppConfig,
) -> list[dict[str, str | bool | list[str] | dict[str, str] | None]]:
    """One row per configured provider, shaped for PROVIDER_FOLDER_MAP."""
    rows: list[dict[str, str | bool | list[str] | dict[str, str] | None]] = []
    for provider in app_config.providers.values():
        rows.append(
            {
                "folder_name": provider_folder_name(provider),
                "provider_id": provider.provider_id,
                "provider_code": provider.provider_code,
                "dataset_name": provider.dataset_name,
                "file_glob": provider.file_glob,
                "external_id_column": provider.external_id_column,
                "delimiter": provider.delimiter,
                "multi_file": provider.multi_file,
                "matches_dataset": provider.matches_dataset,
                "column_mappings": dict(provider.column_mappings),
                "matching_identifiers": matching_identifiers_for_provider(provider, app_config),
            }
        )
    return rows


def render_merge_sql(rows: list[dict[str, str | bool | list[str] | dict[str, str] | None]]) -> str:
    """Render one idempotent MERGE INTO statement for all provider rows.

    A MERGE (not a plain INSERT) so re-running this after a provider's YAML
    changes updates the existing row rather than erroring on the
    (folder_name, file_glob) primary key, or leaving a stale row behind.

    Keyed on (folder_name, file_glob) rather than folder_name alone: a
    multi_file provider (e.g. RISOS) has more than one ProviderConfig
    sharing one folder_name, each distinguished by its own file_glob —
    run_pipeline.py resolves which row applies to an incoming file by
    matching its filename against file_glob, not folder_name alone.

    column_mappings/matching_identifiers are VARIANT/ARRAY-typed, which a
    plain `VALUES (...) AS src(...)` row constructor can't hold directly —
    each row is instead built as its own SELECT (with PARSE_JSON applied to
    a JSON text literal), UNION ALL'd together, so the two structured
    columns parse correctly while every other column stays a plain literal.
    """
    if not rows:
        return "-- no providers configured; nothing to merge"

    def _sql_string_literal(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def _sql_literal(value: str | bool | None) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        return _sql_string_literal(value)

    select_rows = []
    for row in rows:
        column_mappings_json = _sql_string_literal(json.dumps(row["column_mappings"]))
        matching_identifiers_json = _sql_string_literal(json.dumps(row["matching_identifiers"]))
        select_rows.append(
            "SELECT "
            + ", ".join(
                [
                    _sql_literal(row["folder_name"]),
                    _sql_literal(row["provider_id"]),
                    _sql_literal(row["provider_code"]),
                    _sql_literal(row["dataset_name"]),
                    _sql_literal(row["file_glob"]),
                    _sql_literal(row["external_id_column"]),
                    _sql_literal(row["delimiter"]),
                    _sql_literal(row["multi_file"]),
                    _sql_literal(row["matches_dataset"]),
                    f"PARSE_JSON({column_mappings_json})",
                    f"PARSE_JSON({matching_identifiers_json})",
                ]
            )
        )

    values_clause = "\n    UNION ALL\n    ".join(select_rows)

    return f"""MERGE INTO PROVIDER_FOLDER_MAP AS target
USING (
    {values_clause}
) AS source(folder_name, provider_id, provider_code, dataset_name, file_glob, external_id_column, delimiter, multi_file, matches_dataset, column_mappings, matching_identifiers)
ON target.folder_name = source.folder_name AND target.file_glob = source.file_glob
WHEN MATCHED THEN UPDATE SET
    provider_id = source.provider_id,
    provider_code = source.provider_code,
    dataset_name = source.dataset_name,
    external_id_column = source.external_id_column,
    delimiter = source.delimiter,
    multi_file = source.multi_file,
    matches_dataset = source.matches_dataset,
    column_mappings = source.column_mappings,
    matching_identifiers = source.matching_identifiers,
    updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    folder_name, provider_id, provider_code, dataset_name, file_glob, external_id_column, delimiter, multi_file, matches_dataset, column_mappings, matching_identifiers
) VALUES (
    source.folder_name, source.provider_id, source.provider_code,
    source.dataset_name, source.file_glob, source.external_id_column, source.delimiter, source.multi_file, source.matches_dataset,
    source.column_mappings, source.matching_identifiers
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
