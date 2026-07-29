# MatchBot — Snowflake demo

A standalone, Snowflake-native reproduction of the RIDE/SASID exact-match
pipeline, built to sit alongside the AWS demo (Glue + ECS, `../scripts/`)
so a client can compare the two before deciding which to go with.

Full design rationale: [`../docs/snowflake-implementation-plan.md`](../docs/snowflake-implementation-plan.md).

## Scope (this first pass)

- Storage and compute: entirely Snowflake. S3 is used only as the file
  dropzone (external stage) — no Lambda, no EventBridge, no Glue, no ECS.
- Trigger: a Snowflake Task polling the external stage on a schedule.
- Matching: a 12-rule strict-to-loose cascade from `config/global.yaml`,
  the same single source of truth the AWS demo reads —
  external id → SSN → name+SSN4 → name+DOB → five name+address exact
  tiers (full address down to first-name-only+address) → three fuzzy
  tiers (name-fuzzy/address-exact, name-exact/address-fuzzy, fully
  weighted-combined). First matcher to reach a terminal decision wins,
  same as the AWS/Python path (`matching/base.py`'s `Matcher` protocol).
  Fuzzy scoring mirrors `matching/fuzzy.py::FuzzyMatcher` exactly (weighted
  fraction of comparisons clearing their own threshold), computed via
  generated SQL calling a Python UDF for Jaro-Winkler similarity — see
  `python/matchbot_snowflake/matchers/fuzzy.py`.
- A fuzzy match scoring below its own `accept_threshold` but at or above
  `review_threshold` is routed to `RILDS_ERROR` with
  `decision = 'LOW_CONFIDENCE'` (not auto-matched, not silently dropped —
  flagged for optional manual review), mirroring the AWS demo's
  `MatchDecision.AMBIGUOUS` handling in `pipeline/match.py`.
- Reference data: one-time export from the existing Postgres
  `rilds_reference`, not a live sync — both demos compare against an
  identical, frozen snapshot of people. `address1_std`/`zip5` on
  `RILDS_REFERENCE` are assumed precomputed upstream (same contract as
  `first_name_std`/`last_name_std`) — this repo does not derive them for
  reference data, only for incoming provider files.
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
never loops over staged records row by row in Python; the exceptions are
two Python UDFs, each registered once and called by generated SQL, not by
this package's own code:

- `MATCHBOT_METAPHONE` — Snowflake's native `SOUNDEX` is a different
  algorithm than the `jellyfish.metaphone` the AWS demo uses, so true
  parity needs the same library running as a UDF (see `derive_sql.py`'s
  module docstring).
- `MATCHBOT_JARO_WINKLER` — plain SQL has no string-similarity function
  guaranteed to match `jellyfish.jaro_winkler_similarity`'s exact output,
  which the fuzzy matcher tiers' scoring depends on for AWS/Snowflake
  parity (see `procedures/run_pipeline.py::register_jaro_winkler_udf`).

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

**5. Deploy the Python package and register both UDFs** — package
`python/matchbot_snowflake` (via `snow snowpark build && snow snowpark
deploy`, per `snowflake.yml`, or an equivalent manual `CREATE PROCEDURE ...
AS $$ ... $$`), then register the two Python UDFs the generated SQL calls
(idempotent — safe to re-run after any redeploy):

```bash
cd python
snow snowpark build
snow snowpark deploy
snow sql -q "CALL register_metaphone_udf()"
snow sql -q "CALL register_jaro_winkler_udf()"
```

Skipping the UDF registration step means the fuzzy matcher tiers fail at
runtime with an "unknown function MATCHBOT_JARO_WINKLER" error the first
time a record falls through to them.

**6. Deploy the Task**:

```bash
cd ..
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

- Blocking is approximated per matcher rather than via a separate
  blocking-index step like the Python path's `matching/blocking.py`:
  deterministic matchers' equi-joins are inherently selective; fuzzy
  matchers narrow candidates via their first `exact`/`threshold=1.0`
  comparison (e.g. an address or zip field), falling back to a
  `last_name_metaphone1` phonetic filter only if a fuzzy matcher has no
  exact comparison at all (no matcher in the current chain hits that
  fallback — see `matchers/fuzzy.py`'s module docstring). Fine at demo
  scale; a real blocking-index layer would be worth adding before this
  runs against full production volumes.
- Only RIDE is onboarded. A second provider needs its own land table +
  `provider_sql.py` projection — the pattern is established, not yet
  generalized into a fully dynamic per-provider SQL interpreter.
- Reference data is a frozen snapshot, not live-synced with Postgres.
  `address1_std`/`zip5` specifically are not derived by this repo for
  reference data — assumed precomputed upstream (see Scope above); the
  address-tier and fuzzy matchers only find candidates for reference rows
  where those columns are actually populated.
- Fuzzy-tier thresholds/weights (`config/global.yaml`) are a first-pass
  design, not yet validated against real match/non-match outcomes for this
  population — treat as tunable, not final, once real data is available.
