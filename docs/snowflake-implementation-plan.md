# Snowflake-native MatchBot demo — implementation plan

## Context

MatchBot's matching pipeline already runs on AWS two ways (Glue, ECS/Fargate), both against Postgres, both driven by the same Python core (`Orchestrator` → `ParseStage` → `CanonicalStage` → `CleanseStage` → `MatchStage`). The client wants to evaluate a second option before committing: a fully Snowflake-native version of the same demo, so they can compare the two and decide which platform to go with.

Agreed scope for this first demo (confirmed in conversation):
- Storage **and** compute live entirely in Snowflake. S3 is used only as the file dropzone (external stage) — no other AWS service (no Lambda, no EventBridge, no Glue, no ECS).
- Trigger: a Snowflake **Task** on a schedule, polling the external stage for new files.
- Matching scope: **exact-match parity** with today's deterministic chain only (external id → SSN → name+DOB → name+address). Fuzzy/Levenshtein/nearest-neighbor are explicitly deferred — not built now, but the matcher-to-SQL mechanism must be structured so they can be added later without reworking the cascade.
- Reference data: **one-time export** from the existing Postgres `rilds_reference`, so both demos match against identical people — not a live sync.
- Matching executes as **generated, set-based SQL** (one cascade query using `ROW_NUMBER() OVER (PARTITION BY stage_id ORDER BY priority)` to implement "first matcher to accept wins"), wrapped in a **Snowpark stored procedure** whose Python code assembles/executes SQL — it does not loop over rows itself.
- `config/global.yaml` and `config/providers/*.yaml` remain the single source of truth for both demos — nothing about matcher chains or provider mappings is hand-duplicated into a second, driftable copy.

Explicitly **out of scope** for this first pass (flagging so it's not a surprise later): fuzzy/phonetic/nearest-neighbor matching, live reference-data sync, more than one provider (RIDE only), and any production cutover/dual-running story — this is a comparison demo, not a migration.

## Where the code lives

New top-level directory `snowflake/` (sibling of `src/`, `scripts/`, `config/`), **not** inside `src/matchbot/runtime/`. Reasoning: the existing `Runtime` abstraction models "same Polars/Python core, different I/O adapter" — this demo's execution model is the opposite (a SQL cascade run by Snowflake's own compute, no `Orchestrator`, no `MatchStage` Python loop). Forcing it through `SnowflakeRuntime` would either be a misleading facade or drag Snowpark-only concerns into the cloud-SDK-free core. `src/matchbot/runtime/snowflake.py` stays the stub it is today; this plan is a documented third option in that file's docstring, not an implementation of that class.

```
snowflake/
  ddl/                          # numbered SQL DDL scripts (integration, stage, tables, task, procedure)
  python/matchbot_snowflake/
    config_bridge.py            # loads AppConfig via matchbot.config.loader, generates PROVIDER_FOLDER_MAP rows
    matcher_registry.py         # @register_sql_matcher pattern, mirrors matching/base.py
    matchers/deterministic.py   # the one generator needed for all 4 current matchers
    cascade_builder.py          # assembles the ROW_NUMBER() cascade query from ordered fragments
    derive_sql.py               # SQL port of standardize.py / derive.py (std_name, metaphone, last_name8, etc.)
    provider_sql.py             # per-provider column_mappings/transforms -> SQL projection (RIDE view)
    procedures/run_pipeline.py  # the Snowpark stored procedure entrypoint
    export/export_rilds_reference.py
    tests/                      # unit tests for cascade_builder/matcher_registry against fixtures
  docs/demo-comparison.md
```

`snowflake/python/` depends on the installed `matchbot` package (one-way dependency) specifically to reuse:
- `matchbot.config.loader.load_config` / `ConfigError`
- `matchbot.config.models.{AppConfig, ProviderConfig, MatcherSpec, GlobalConfig}`
- `matchbot.pipeline.match.resolve_matcher_chain`, `filter_chain_by_provider_attributes`
- The logic (ported, not imported — must run in-warehouse) of `matching/standardize.py` and `matching/derive.py`

## Snowflake objects

Uses the existing `MATCHBOT` database, schema `RILDS` (mirrors `DB_SCHEMA=rilds`).

1. Warehouse `MATCHBOT_DEMO_WH` — small, auto-suspend.
2. Storage Integration `MATCHBOT_S3_INT` — scoped to the existing S3 dropzone prefix; the only AWS touchpoint.
3. External Stage `RILDS.INPUT_STAGE` — directory-table-enabled, so `DIRECTORY(@INPUT_STAGE)` lists new files cheaply.
4. `RILDS.PROVIDER_FOLDER_MAP` — `(folder_name, provider_id, provider_code, dataset_name, file_glob, external_id_column)`, generated from `config/providers/*.yaml` by `config_bridge.py` — the Snowflake-native replacement for the hardcoded `FOLDER_TO_PROVIDER` dict in `scripts/lambda_function_glue.py`.
5. `RILDS.INGEST_LOG` — tracks already-processed files so polling doesn't reprocess.
6. `RILDS.RIDE_LAND`, `RILDS.RILDS_STAGE`, `RILDS.RILDS_REFERENCE` (67 columns, natural-key `idcol_id`, loaded once via export), `RILDS.RILDS_MATCHED`, `RILDS.RILDS_ERROR`, `RILDS.RILDS_AUDIT` — same shapes as `storage/schema.py`, so parity diffs are direct.
7. Stored procedure `RILDS.RUN_MATCH_PIPELINE(FILE_PATH STRING)` — Snowpark Python, does land → derive/standardize → stage → generate+run cascade → write matched/error/audit, entirely via `session.sql(...).collect()` calls.
8. Task `RILDS.POLL_INPUT_STAGE_TASK` — scheduled, calls a poll/dispatch procedure (joins `DIRECTORY(@INPUT_STAGE)` against `INGEST_LOG`/`PROVIDER_FOLDER_MAP`) which then calls `RUN_MATCH_PIPELINE` per new file.

## Matcher-to-SQL registry

Mirrors `matching/base.py`'s `@register_matcher` pattern, emitting SQL instead of instantiating a Python class:

```python
_SQL_REGISTRY: dict[str, Callable[[MatcherSpec], MatcherSqlFragment]] = {}
def register_sql_matcher(type_name: str) -> decorator
def build_sql_fragments(specs: list[MatcherSpec]) -> list[MatcherSqlFragment]
```

`MatcherSqlFragment` is a small dataclass (`name`, `priority`, `join_predicate_sql`, `guard_predicate_sql`, `method_label`), not a raw string, so the cascade builder can reason about ordering structurally.

`matchers/deterministic.py` is the only generator needed for all 4 in-scope matchers: for each key in `spec.keys`, emit `s.<key> = r.<key>` ANDed together, plus a guard clause reproducing `DeterministicMatcher.match()`'s "any missing/blank key → no match" behavior. Adding fuzzy later means writing `matchers/fuzzy.py` and registering it — zero change to `matcher_registry.py` or `cascade_builder.py`.

## Cascade query shape

```sql
WITH candidate_matches AS (
    SELECT s.id AS stage_id, r.idcol_id, 1 AS priority, 'EXACT_SASID' AS method
    FROM RILDS_STAGE s JOIN RILDS_REFERENCE r ON <fragment[0].join_predicate_sql>
    WHERE <fragment[0].guard_predicate_sql> AND s.pipeline_run_id = :run_id
    UNION ALL
    SELECT s.id, r.idcol_id, 2, 'EXACT' FROM RILDS_STAGE s JOIN RILDS_REFERENCE r
    ON <fragment[1].join_predicate_sql> WHERE <fragment[1].guard_predicate_sql> AND s.pipeline_run_id = :run_id
    UNION ALL
    -- fragment[2] (name+dob), fragment[3] (name+addr)
),
ranked AS (
    SELECT stage_id, idcol_id, priority, method,
           ROW_NUMBER() OVER (PARTITION BY stage_id ORDER BY priority ASC, idcol_id ASC) AS rn
    FROM candidate_matches
)
SELECT stage_id, idcol_id, method FROM ranked WHERE rn = 1
```

Followed by set-based writes: update `RILDS_STAGE` match columns from `winners`, insert `RILDS_MATCHED` for winners, insert `RILDS_ERROR` for stage rows with no winner. `priority` comes directly from `config/global.yaml`'s matcher order — never hand-numbered. The `idcol_id ASC` tie-break guarantees exactly one winner per stage row even if a priority level has multiple candidates.

Note (worth being upfront about): blocking is implicit in the equi-joins here — no separate blocking-index step like the Python path's `blocking.py`. Fine for exact-match-only at demo scale; call out in `docs/demo-comparison.md` as exactly where fuzzy matching would need real blocking logic reintroduced.

## Provider resolution

Both the folder→provider mapping and the RIDE column-mapping/transform SQL are **generated from the existing YAML at deploy time**, not hand-authored a second time:
- `config_bridge.py` loads `AppConfig` via the reused config loader and emits one `MERGE INTO PROVIDER_FOLDER_MAP` row per provider.
- `provider_sql.py`'s `render_provider_projection_sql(provider: ProviderConfig)` walks `column_mappings`/`transforms` the same way `CleanseStage` does, emitting `UPPER(TRIM(FIRSTNAME)) AS first_name`-style SQL plus a `skip_if_null` filter. Written as one function over one `ProviderConfig` so a second provider is a loop addition later, not a rewrite — but only RIDE is built now.

## Build order (each step has a cheap validation gate before the next)

1. **Stage plumbing only** — warehouse, storage integration, external stage. Validate: upload a RIDE CSV to S3, confirm it's visible via `DIRECTORY(@INPUT_STAGE)`.
2. **Land + stage + reference tables, loaded by hand** — run the one-time reference export, manual `COPY INTO`. Validate: row counts and spot-checked values match Postgres.
3. **Provider view + derived/standardized columns, no matching yet** — port `derive_sql.py`. Validate: diff derived columns (`first_name_std`, metaphone, `last_name8`, `rilds_id`) against the Python pipeline's output for the same input file. **Highest-risk parity point — validate carefully here.**
4. **Matcher registry + cascade builder against a fixed fixture** — unit tests with known winners/fall-throughs; run generated SQL directly in a worksheet against real tables from step 2–3. Validate: every stage row lands in exactly one outcome, no duplicates/omissions.
5. **Wrap in the stored procedure** — thin orchestration over already-tested SQL generators; invoke manually via `CALL`. Validate: identical results to step 4 (any mismatch here is plumbing, not matching logic).
6. **Mapping table + Task** — deploy, test via manual `EXECUTE TASK` before trusting the schedule.
7. **Full parity run + comparison write-up** — same input file through both demos, row-level diff, `docs/demo-comparison.md`.

## Validating parity with the AWS/Postgres demo

- Same input file, same static exported `rilds_reference` snapshot on both sides.
- Diff keyed on `source_row_id` (stable across both pipelines — their autoincrement `id` values will never match numerically).
- Aggregate check first (match rate, per-`match_method` counts) as a cheap gate before the full row-level diff.
- Freeze a small golden fixture (50–200 rows) with known expected outcomes, for regression-testing future changes without a live DB connection.

## Reuse vs. new

**Reused as-is:** config loader/models, `resolve_matcher_chain`/`filter_chain_by_provider_attributes`, `CANONICAL_NAMES`/`MATCH_ATTRIBUTE_COLUMNS`, existing sample data, the *pattern* of `scripts/seed_rilds_reference.py` for the export.

**Ported (same logic, new SQL implementation):** `standardize.py` (`std_name`, `std_gender`, `metaphone`, `squash_ws`), `derive.py`'s column set/order, `cleanse.py`'s transform application, `DeterministicMatcher.match()`'s guard/join semantics.

**Entirely new:** all Snowflake DDL, the `ROW_NUMBER()` cascade mechanism itself (no Python-side analog exists), `PROVIDER_FOLDER_MAP` generation, the stored procedure and Task, the reference-export script.

## Critical files to reference during implementation

- `src/matchbot/config/loader.py`, `src/matchbot/config/models.py`
- `src/matchbot/matching/standardize.py`, `src/matchbot/matching/derive.py`
- `src/matchbot/pipeline/match.py` (matcher-chain resolution/filtering)
- `src/matchbot/storage/schema.py` (table shapes to mirror)
- `config/global.yaml`, `config/providers/provider_ride_enrollment.yaml`
- `scripts/lambda_function_glue.py` (folder→provider pattern to replace)
- `scripts/seed_rilds_reference.py` (export pattern to reverse)
