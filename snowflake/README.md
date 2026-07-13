# MatchBot — Snowflake demo

A standalone, Snowflake-native reproduction of the RIDE/SASID exact-match
pipeline, built to sit alongside the AWS demo (Glue + ECS, `../scripts/`)
so a client can compare the two before deciding which to go with.

Full design rationale: [`../docs/snowflake-implementation-plan.md`](../docs/snowflake-implementation-plan.md).

## Scope (this first pass)

- Storage and compute: entirely Snowflake. S3 is used only as the file
  dropzone (external stage) — no Lambda, no EventBridge, no Glue, no ECS.
- Trigger: a Snowflake Task polling the external stage on a schedule.
- Matching: exact-match parity with the current deterministic chain only
  (external id → SSN → name+DOB → name+address, from `config/global.yaml`).
  Fuzzy/phonetic/nearest-neighbor matching is deliberately deferred — the
  matcher-to-SQL registry is structured so those slot in later without
  reworking the cascade (see `python/matchbot_snowflake/matchers/`).
- Reference data: one-time export from the existing Postgres
  `rilds_reference`, not a live sync — both demos compare against an
  identical, frozen snapshot of people.
- One provider (RIDE) is wired end-to-end; the code is structured so a
  second provider is additive, not a rewrite.

## Layout

```
snowflake/
  ddl/                 numbered SQL DDL — run in order (00 through 07)
  python/
    matchbot_snowflake/ SQL-generation package — depends on the matchbot
                         package (config loader, matcher-chain resolution)
                         so config/global.yaml and config/providers/*.yaml
                         stay the single source of truth for both demos
    tests/              unit tests, no live Snowflake connection required
  docs/                 demo-comparison write-up (produced at the end)
```

`python/matchbot_snowflake` is orchestration and SQL-generation glue — it
assembles SQL text and executes it via `session.sql(...).collect()`. It
never loops over staged records row by row in Python; the one exception is
the `MATCHBOT_METAPHONE` UDF (registered once, called by generated SQL, not
by this package's own code), needed because Snowflake's native `SOUNDEX` is
a different algorithm than the `jellyfish.metaphone` the AWS demo uses —
see `python/matchbot_snowflake/derive_sql.py`'s module docstring.

## Setup

**1. Install the Python package** (depends on the root `matchbot` package
via a local path — see `python/pyproject.toml`):

```bash
cd snowflake/python
uv sync --extra dev
uv run pytest tests/   # all unit tests run with no live Snowflake connection
```

**2. Deploy the DDL**, in order, against a Snowflake account with
`ACCOUNTADMIN` or equivalent privileges (storage integrations require
elevated privileges to create):

```bash
snow sql -f ddl/00_database_and_schema.sql
snow sql -f ddl/01_storage_integration.sql   # fill in the IAM role ARN + bucket first
# ... finish the storage-integration trust-policy step described in that file ...
snow sql -f ddl/02_file_format_and_stage.sql  # fill in the bucket URL
snow sql -f ddl/03_provider_folder_map.sql
snow sql -f ddl/04_land_and_stage_tables.sql
snow sql -f ddl/05_reference_table.sql
snow sql -f ddl/06_matched_error_audit_tables.sql
```

**3. Generate and run the provider-folder mapping SQL** (from the same
`config/providers/*.yaml` the AWS demo uses):

```bash
uv run python -c "
from matchbot_snowflake.config_bridge import build_provider_folder_map_sql
print(build_provider_folder_map_sql('../../config'))
" > /tmp/provider_folder_map.sql
snow sql -f /tmp/provider_folder_map.sql
```

**4. Export and load the reference data** (one-time, from whichever
Postgres already has `rilds_reference` populated):

```bash
DATABASE_URL=postgresql://... DB_SCHEMA=rilds \
    uv run python -m matchbot_snowflake.export.export_rilds_reference

# upload the resulting CSV to an internal/external stage, then:
snow sql -q "COPY INTO RILDS_REFERENCE FROM @<stage>/rilds_reference.csv FILE_FORMAT = CSV_PROVIDER_FORMAT"
```

**5. Deploy the stored procedure + Task** — package
`python/matchbot_snowflake/procedures/run_pipeline.py` (via `snow snowpark
deploy` or an equivalent `CREATE PROCEDURE ... AS $$ ... $$`), then:

```bash
snow sql -f ddl/07_procedure_and_task.sql
```

## Build/validation order

Follow `../docs/snowflake-implementation-plan.md`'s "Build order" section —
each step has its own cheap validation gate before moving to the next
(stage visibility → land/reference data → derived-column parity → matcher
SQL against a fixture → the stored procedure manually invoked → the Task).
Do not enable the Task (`ALTER TASK POLL_INPUT_STAGE_TASK RESUME;`) until
`CALL RUN_MATCH_PIPELINE(...)` has been validated manually.

## Known limitations (by design, for this first pass)

- Blocking is implicit in the matcher equi-joins — no separate
  blocking-index step like the Python path's `matching/blocking.py`. Fine
  at exact-match-only demo scale; fuzzy matching would need real blocking
  logic reintroduced.
- Only RIDE is onboarded. A second provider needs its own land table +
  `provider_sql.py` projection — the pattern is established, not yet
  generalized into a fully dynamic per-provider SQL interpreter.
- Reference data is a frozen snapshot, not live-synced with Postgres.
