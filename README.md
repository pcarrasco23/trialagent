# TrialRT

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- Python 3.10+
- [OpenAI API key](https://platform.openai.com/account/api-keys)

## Setup

### 1. Set environment variables

```bash
export OPENAI_API_KEY="sk-proj-..."
export SYNTHEA_FHIR_DB_URL="postgresql://postgres:password@localhost:5432/synthea_fhir"
export ADMIN_DB_URL="postgresql://postgres:password@localhost:5433/trial_agent_admin"
export QDRANT_URL="http://localhost:6333"
export TRIAL_AGENT_API_URL="http://localhost:8000"
```

| Variable | Description |
|---|---|
| `OPENAI_API_KEY` | [OpenAI API key](https://platform.openai.com/account/api-keys) for the LLM agents |
| `SYNTHEA_FHIR_DB_URL` | PostgreSQL URL for the patient/FHIR database |
| `ADMIN_DB_URL` | PostgreSQL URL for the admin database (trial info, audits, prompts) |
| `QDRANT_URL` | Qdrant vector database URL |
| `TRIAL_AGENT_API_URL` | Trial-RT API URL for the CDC consumer (default: `http://localhost:8000`) |

### 2. Start infrastructure

```bash
docker compose up -d
```

This starts two PostgreSQL instances (`postgres-synthea` on port 5432 for the FHIR database, `postgres-trial-agent` on port 5433 for the admin database), and Qdrant. Both databases are auto-created on first start by the container's `POSTGRES_DB` setting.

The `db-setup` service runs automatically on first start — it waits for both Postgres instances, then creates all schemas (`scripts/admin_db_setup.py`), seeds the prompts (`scripts/seed_prompts.py`), imports FHIR data (`clients/synthea/synthea_fhir_postgres_import.py`), and imports CTG clinical trial data. Prompts and FHIR data are only seeded/imported on a fresh database. The API and worker services wait for `db-setup` to complete before starting.

### 3. Seed prompts (fresh database only)

On a fresh database, prompts are seeded automatically by `db-setup`. To re-seed manually (this will fail if prompts already exist):

```bash
docker exec -it trial-agent-api python scripts/seed_prompts.py
```

Prompt data is stored in `data/seed_prompts.json`.

### 4. Index trial data into Qdrant

Indexing uses two vector types per collection: BM25 (sparse) and MedCPT (dense). Run BM25 first (sets the payload), then MedCPT.

**Clinical Trials Gov collection**:

```bash
docker exec -it trial-agent-worker python scripts/clinical_trials_gov_bm25.py
docker exec -it trial-agent-worker python scripts/clinical_trials_gov_medcpt.py
```

**TREC 2021 corpus collection** (from `clients/trec/data/corpus.jsonl`):

```bash
docker exec -it trial-agent-worker python scripts/trec_corpus_bm25.py
docker exec -it trial-agent-worker python scripts/trec_corpus_medcpt.py
```

To index a subset for testing, use the `--limit` flag:

```bash
docker exec -it trial-agent-worker python scripts/clinical_trials_gov_bm25.py --limit 1000
docker exec -it trial-agent-worker python scripts/clinical_trials_gov_medcpt.py --limit 1000
```

To re-index BM25 vectors (e.g. after changing the hashing approach), use `--force`:

```bash
docker exec -it trial-agent-worker python scripts/clinical_trials_gov_bm25.py --force
docker exec -it trial-agent-worker python scripts/trec_corpus_bm25.py --force
```

Scripts can also be run outside of Docker:

```bash
export ADMIN_DB_URL="postgresql://postgres:password@localhost:5433/trial_agent_admin"
export QDRANT_URL="http://localhost:6333"
python scripts/clinical_trials_gov_bm25.py
python scripts/clinical_trials_gov_medcpt.py
```

**Download new/updated trials from ClinicalTrials.gov:**

```bash
docker exec -it trial-agent-worker python scripts/ctg_download.py
```

### 5. Build and start the application

```bash
docker compose build
docker compose up -d
```

This builds and starts the following services:

| Service | Port | Description |
|---|---|---|
| `trial-agent-api` | 8000 | REST API — accepts workflow requests (`POST /trial_agent_workflow`) |
| `trial-agent-worker` | — | Polls for pending workflows and runs the agent pipeline |
| `trial-agent-dashboard` | 8002 | Workflow dashboard UI — view workflows, audits, and prompts |
| `synthea` | 8001 | Synthea patient viewer UI (local Postgres) |
| `synthea-bigquery` | 8003 | Synthea patient viewer UI (Google BigQuery) |
| `trec` | 8004 | TREC 2021 clinical trials topic viewer UI |
| `db-setup` | — | Runs on startup to initialize databases and import data (skips if already done) |

Multiple workers can run concurrently — each claims work atomically using Postgres `SKIP LOCKED`.

To rebuild after code changes:

```bash
docker compose build && docker compose up -d
```

