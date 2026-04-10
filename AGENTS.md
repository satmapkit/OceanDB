# AGENTS.md

## Purpose

OceanDB is a Python package for ingesting and querying ocean satellite datasets in PostgreSQL/PostGIS. The codebase combines:

- database initialization and schema management,
- ETL pipelines for along-track and eddy datasets,
- packaged SQL query assets,
- typed schema and dataset helpers,
- a Click CLI for initialization and ingest workflows.

This file gives coding agents the project-specific context needed to make safe changes.

## Repository Map

- `src/OceanDB/cli.py`: CLI entrypoints such as `oceandb init`, `oceandb ingest-along-track`, and `oceandb ingest-eddy`.
- `src/OceanDB/OceanDB.py`: shared database/resource-loading base class.
- `src/OceanDB/OceanDB_Initializer.py`: database/table/index/partition creation logic.
- `src/OceanDB/etl/`: ingest and download flows.
- `src/OceanDB/data_access/`: query-facing APIs.
- `src/OceanDB/schemas/`: schema contracts for returned data.
- `src/OceanDB/ocean_data/`: typed dataset/domain abstractions.
- `src/OceanDB/sql/`: packaged SQL for tables, indices, drops, and queries.
- `src/OceanDB/data/`: packaged NetCDF and CSV assets used by the library.
- `tests/`: pytest suite, including integration tests that create/drop a real database.
- `docker-compose.postgres.yml` and `docker-compose.postgres.test.yml`: local Postgres/PostGIS environments.

## Environment And Setup

Prefer working from the repository root.

Typical local setup:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
pip install -r requirements.txt
```

Runtime configuration is expected from a `.env` file at the repo root. Important variables called out in `README.md`:

- `POSTGRES_HOST`
- `POSTGRES_USERNAME`
- `POSTGRES_PASSWORD`
- `POSTGRES_PORT`
- `POSTGRES_DATABASE`
- `ALONG_TRACK_DATA_DIRECTORY`
- `EDDY_DATA_DIRECTORY`
- `COPERNICUS_USERNAME`
- `COPERNICUS_PASSWORD`

Tests also use `tests/.env.test` through `Config(_env_file="tests/.env.test")`.

## Common Commands

Install package in editable mode:

```bash
pip install -e .
```

Run formatting and lint targets that already exist in the repo:

```bash
make format
make lint
make check
```

Start local databases:

```bash
make run_postgres
make run_postgres_test
```

Run tests:

```bash
pytest
pytest tests/database/test_create.py
pytest tests/database/test_ingest.py
pytest tests/along_track/test_spatiotemporal_queries.py
```

Run tests inside the agent sandbox:

```bash
make build_agent_sandbox
make up_agent_sandbox
make run_agent_sandbox
make test_agent_sandbox
make down_agent_sandbox
```

## Project Conventions

### SQL and schema changes

- Keep SQL in `src/OceanDB/sql/...` rather than embedding large SQL strings in Python when the query is reusable.
- If a query result shape changes, update the corresponding schema contract in `src/OceanDB/schemas/` and any dataset wrappers that depend on it.
- Preserve packaged-resource loading behavior. SQL, NetCDF, and CSV assets are imported via `importlib.resources`, so filenames and package paths matter.

### Database tests

- Many tests are integration tests, not pure unit tests.
- `tests/database/fixtures.py` creates tables, indices, and partitions, then drops the test database during teardown.
- Avoid changing database lifecycle behavior casually; small schema changes can ripple through ingest and query tests.
- Containerized integration runs should use `tests/.env.docker`, which targets the `postgres_test` service rather than `localhost`.

### ETL and data workflows

- Along-track and eddy ingest code assumes specific file naming/layout conventions.
- Before changing ingest logic, inspect the matching fixtures in `tests/data/along_track/` and `tests/data/eddy/`.
- Be careful with date-range and mission filtering logic in the CLI and ETL layers; those paths control which files are discovered and ingested.

### Architecture notes

The repo’s `architecture.md` emphasizes a schema-contract-driven approach:

- define fields once,
- reuse them across SQL/querying and Python dataset construction,
- keep query projections explicit and safe,
- preserve domain-specific dataset semantics instead of returning unstructured rows.

Changes should reinforce that direction rather than bypass it.

## Guardrails For Agents

- Prefer small, targeted changes over broad refactors.
- Do not rename packaged SQL/data files without updating all resource-loading call sites.
- Do not assume tests are isolated from PostgreSQL; verify whether a change requires a running test database.
- For containerized agent work, prefer `docker-compose.agent.yml` over the older externally-networked client container setup.
- If touching CLI or ETL behavior, update `README.md` when the user-facing workflow changes.
- If touching schemas, queries, or typed dataset objects, run the most relevant query/integration tests before finishing when possible.

## Current Observations

- The worktree already has an unrelated modification in `src/OceanDB/utils/date_time_conversion.py`.
- Leave unrelated user changes intact.
