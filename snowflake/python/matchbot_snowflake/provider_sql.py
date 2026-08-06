"""Per-provider column_mappings/transforms -> SQL projection.

Mirrors CleanseStage._apply_string_transforms / CanonicalStage's job: turn
a provider's raw land-table columns into canonical, transformed columns,
as one SELECT per provider — generated from ProviderConfig (see
config_models.py — a deliberate, hand-synced copy of matchbot's own
ProviderConfig, not an import of it; see that module's docstring for why),
not hand-authored per provider.

Only RIDE is built out today (single provider in scope for this demo — see
docs/snowflake-implementation-plan.md). render_provider_projection_sql is
written as one function over one ProviderConfig so a second provider is a
loop addition later, not a rewrite.
"""

from __future__ import annotations

from matchbot_snowflake.config_models import ProviderConfig, StandardizationConfig, TransformSpec

from matchbot_snowflake.derive_sql import (
    HASH_COLUMN_BY_KEYS,
    birth_parts_sql,
    deterministic_hash_sql,
    last_name8_sql,
    metaphone_sql,
    ssn4_sql,
    std_address_sql,
    std_gender_sql,
    std_name_sql,
    std_zip_sql,
)


def _apply_transform_sql(raw_column_ref: str, spec: TransformSpec) -> str:
    """Mirror CleanseStage._apply_string_transforms for one column.

    Order matches the Python implementation exactly: cast to text, trim,
    strip configured substrings, uppercase, zero-pad, then NULL-if-empty.
    Date-typed transforms are handled by the caller (see
    render_provider_projection_sql), not here.
    """
    expr = raw_column_ref
    if spec.trim:
        expr = f"TRIM({expr})"
    for substr in spec.strip:
        escaped = substr.replace("'", "''")
        expr = f"REPLACE({expr}, '{escaped}', '')"
    if spec.upper:
        expr = f"UPPER({expr})"
    if spec.zero_pad:
        expr = f"LPAD({expr}, {spec.zero_pad}, '0')"
    return f"NULLIF({expr}, '')"


def _default_trim_sql(raw_column_ref: str) -> str:
    """Cleanse's default for any string attribute lacking an explicit
    transform: trim + NULL-if-empty (see cleanse.py's untransformed-column
    loop)."""
    return f"NULLIF(TRIM({raw_column_ref}), '')"


def render_provider_projection_sql(
    provider: ProviderConfig,
    std_config: StandardizationConfig,
    *,
    land_table: str,
    pipeline_run_id: int,
) -> str:
    """Render the SELECT that projects ``land_table`` into canonical +
    derived columns for ``provider``, ready to INSERT INTO RILDS_STAGE.

    ``pipeline_run_id`` filters the land table down to only the rows this
    run itself landed. The land table is never truncated between runs
    (CREATE TABLE IF NOT EXISTS in land_sql.py — rows from every past run
    of the same provider stay in it), so without this filter the
    projection re-processes and re-stages every historical run's rows on
    every subsequent call. Caught live: RILDS_STAGE ended up with 6895
    rows (7 accumulated runs x 985) after landing exactly 985 rows for
    the 8th run — rows_landed was correctly scoped by land_sql.py's own
    pipeline_run_id filtering, but this projection had none at all.

    Built as two staged CTEs rather than one flat SELECT: ``canonical``
    computes each mapped/transformed attribute once, ``derived`` then
    references those columns *by name* to compute first_name_std/
    last_name_std/metaphone/last_name8/birth parts. Without this staging,
    every derived expression would need to re-embed the full upstream
    expression tree it depends on (e.g. metaphone needs std_name needs the
    transform needs the raw column) — correct, but unreadably duplicated
    and needlessly expensive to plan. Column order in the final SELECT
    matches RILDS_STAGE's DDL (snowflake/ddl/04_land_and_stage_tables.sql)
    so the caller can ``INSERT INTO RILDS_STAGE (...) SELECT ...``
    positionally without a column list mismatch.
    """
    # Deliberately unquoted: RIDE_LAND (snowflake/ddl/04_land_and_stage_tables.sql)
    # was created with unquoted column names, which Snowflake folds to
    # UPPERCASE. A quoted reference like land."firstname" is a
    # case-sensitive literal that does NOT match the real column FIRSTNAME
    # and fails to compile — caught during build-step-3 parity validation
    # against real RIDE data. Leaving the identifier unquoted here lets
    # Snowflake apply its normal case-insensitive resolution regardless of
    # the raw column's casing in config/providers/*.yaml.
    raw_by_canonical: dict[str, str] = {
        canonical: f"land.{raw_col}"
        for raw_col, canonical in provider.column_mappings.items()
    }

    # combined_columns (see CombinedColumnSpec, config_models.py):
    # concatenates 2+ raw columns into one canonical attribute — e.g. RISOS's
    # STREET_NUMBER + STREET_NAME into address1, since a bare house number
    # is a near-useless identity anchor on its own (every address-anchored
    # matcher keys on address1_std expecting a full street address). NULL-
    # safe: CONCAT would return NULL if ANY argument is NULL, so each raw
    # ref is COALESCE'd to '' first, mirroring canonical.py's Polars
    # ignore_nulls=True (a row missing one piece still gets the pieces it
    # has, not a fully-NULL combined value).
    combined_sql: dict[str, str] = {
        canonical_attr: (
            f" || '{spec.separator}' || "
        ).join(f"COALESCE(land.{raw_col}, '')" for raw_col in spec.from_columns)
        for canonical_attr, spec in provider.combined_columns.items()
    }

    def canonical_sql(canonical_attr: str) -> str:
        # combined_columns and column_mappings are mutually exclusive per
        # canonical attribute (loader.py's cross-reference validation
        # forbids declaring both for the same attribute), so at most one of
        # these is set — a combined expression is just parenthesized and
        # treated exactly like any other raw column reference from here on,
        # so it still goes through this provider's normal transforms entry
        # (e.g. RISOS's address1: {upper: true, trim: true}) rather than
        # bypassing it. Caught as a real bug before this fix: an earlier
        # version special-cased combined_ref straight to
        # _default_trim_sql(), silently skipping the configured upper/trim
        # transform for any combined attribute.
        raw_ref = raw_by_canonical.get(canonical_attr)
        combined_ref = combined_sql.get(canonical_attr)
        if combined_ref is not None:
            raw_ref = f"({combined_ref})"
        if raw_ref is None:
            # A bare, untyped NULL compiles fine as a VARCHAR-ish column
            # value, but birth_date is DATE-typed downstream (RILDS_STAGE's
            # birth_date column, and YEAR()/MONTH()/DAY() in the final
            # SELECT) — Snowflake's EXTRACT-family functions reject an
            # untyped NULL argument outright ("does not support NULL
            # argument type"). Caught during build-step-3 validation
            # against RIDE, which maps no birth_date column at all.
            if canonical_attr == "birth_date":
                return "NULL::DATE"
            return "NULL"
        spec = provider.transforms.get(canonical_attr)
        if canonical_attr == "birth_date":
            fmt = spec.format if spec and spec.format else "%Y-%m-%d"
            # Snowflake TO_DATE format tokens differ from Python strptime;
            # only the %Y-%m-%d default (matching cleanse.py's
            # _parse_date_series default) is mapped here. A provider using
            # a different date format needs its format string translated
            # when it's actually onboarded.
            snowflake_fmt = {"%Y-%m-%d": "YYYY-MM-DD", "%m/%d/%Y": "MM/DD/YYYY"}.get(
                fmt, "YYYY-MM-DD"
            )
            return f"TRY_TO_DATE({raw_ref}, '{snowflake_fmt}')"
        if canonical_attr == "gender":
            return std_gender_sql(raw_ref, std_config)
        if spec is not None:
            return _apply_transform_sql(raw_ref, spec)
        return _default_trim_sql(raw_ref)

    canonical_columns = [
        (canonical_sql("first_name"), "first_name"),
        (canonical_sql("middle_name"), "middle_name"),
        (canonical_sql("last_name"), "last_name"),
        (canonical_sql("birth_date"), "birth_date"),
        (canonical_sql("gender"), "gender"),
        (canonical_sql("member_external_id"), "rilds_id"),
        (canonical_sql("ssn"), "ssn"),
        (canonical_sql("address1"), "address1"),
        (canonical_sql("address2"), "address2"),
        (canonical_sql("city"), "city"),
        (canonical_sql("state"), "state"),
        (canonical_sql("zip"), "zip"),
    ]
    canonical_select = ",\n        ".join(f"{expr} AS {alias}" for expr, alias in canonical_columns)

    birth_parts = birth_parts_sql("birth_date")
    derived_select = ",\n        ".join(
        [
            "source_row_id",
            "provider_code",
            "dataset_name",
            "first_name",
            "middle_name",
            "last_name",
            "birth_date",
            "gender",
            f"{std_name_sql('first_name', std_config)} AS first_name_std",
            f"{std_name_sql('last_name', std_config)} AS last_name_std",
            "rilds_id",
            "NULL AS lasid",
            "ssn",
            "address1",
            "address2",
            "city",
            "state",
            "zip",
        ]
    )

    # NOTE (verify in build step 2): the skip_if_null conditions reference
    # SELECT-list aliases from the same query block. Snowflake supports
    # this (aliases are resolved eagerly, unlike strict ANSI evaluation
    # order) — confirmed against a real warehouse. If it ever fails, fall
    # back to repeating the canonical_sql(...) expression directly in the
    # WHERE clause instead of the alias.
    where_conditions = [f"land.pipeline_run_id = {pipeline_run_id}"]
    if provider.skip_if_null:
        mapped_skip_cols = [c for c in provider.skip_if_null if c in raw_by_canonical]
        if mapped_skip_cols:
            alias_by_canonical = {"member_external_id": "rilds_id"}
            where_conditions.extend(
                f"{alias_by_canonical.get(c, c)} IS NOT NULL" for c in mapped_skip_cols
            )
    skip_filter = "\n    WHERE " + " AND ".join(where_conditions)

    # Hash columns for the deterministic matchers that compare 2+ keys —
    # see derive_sql.py's HASH_COLUMN_BY_KEYS module comment for why the
    # key-set -> column-name mapping is centralized there rather than
    # hardcoded independently here and in matchers/deterministic.py. Column
    # refs point at "standardized" (the CTE below, not "derived") since
    # every hash input (ssn4, address1_std, city, state, zip5) only exists
    # as a real column from that point on — ssn4/address1_std/zip5 are
    # computed IN "standardized" itself, not available any earlier.
    standardized_column_refs = {
        "first_name_std": "first_name_std",
        "last_name_std": "last_name_std",
        "birth_date": "birth_date",
        "ssn4": "ssn4",
        "address1_std": "address1_std",
        "city": "city",
        "state": "state",
        "zip5": "zip5",
    }
    hash_columns_sql = ",\n        ".join(
        f"{deterministic_hash_sql(list(keys), standardized_column_refs)} AS {hash_col}"
        for keys, hash_col in HASH_COLUMN_BY_KEYS.items()
    )
    hash_column_names = list(HASH_COLUMN_BY_KEYS.values())
    hash_columns_select = ",\n    ".join(hash_column_names)

    return f"""WITH canonical AS (
    SELECT
        land.id AS source_row_id,
        '{provider.provider_code}' AS provider_code,
        '{provider.dataset_name}' AS dataset_name,
        {canonical_select}
    FROM {land_table} AS land{skip_filter}
),
derived AS (
    SELECT
        {derived_select}
    FROM canonical
),
standardized AS (
    SELECT
        source_row_id,
        provider_code,
        dataset_name,
        first_name,
        middle_name,
        last_name,
        birth_date,
        gender,
        first_name_std,
        last_name_std,
        {metaphone_sql('first_name_std')} AS first_name_metaphone1,
        {metaphone_sql('last_name_std')} AS last_name_metaphone1,
        {last_name8_sql('last_name_std')} AS last_name8,
        {birth_parts['birth_year']} AS birth_year,
        {birth_parts['birth_month']} AS birth_month,
        {birth_parts['birth_day']} AS birth_day,
        rilds_id,
        lasid,
        ssn,
        {ssn4_sql('ssn')} AS ssn4,
        address1,
        {std_address_sql('address1', std_config)} AS address1_std,
        address2,
        city,
        state,
        zip,
        {std_zip_sql('zip')} AS zip5
    FROM derived
)
SELECT
    source_row_id,
    provider_code,
    dataset_name,
    first_name,
    middle_name,
    last_name,
    birth_date,
    gender,
    first_name_std,
    last_name_std,
    first_name_metaphone1,
    last_name_metaphone1,
    last_name8,
    birth_year,
    birth_month,
    birth_day,
    rilds_id,
    lasid,
    ssn,
    ssn4,
    address1,
    address1_std,
    address2,
    city,
    state,
    zip,
    zip5,
    {hash_columns_sql}
FROM standardized"""
