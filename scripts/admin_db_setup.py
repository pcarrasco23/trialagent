"""
Set up the trial_agent_admin database: create tables, seed prompts, and import trial data.

Usage:
    python scripts/admin_db_setup.py

Requires ADMIN_DB_URL environment variable.
"""

import json
import os
from pathlib import Path

import psycopg2

ADMIN_DB_URL = os.environ.get("ADMIN_DB_URL", "")
CTG_STUDIES_PATH = Path(__file__).parent.parent / "data" / "ctg-studies.json"

BATCH_SIZE = 5000

SCHEMA_SQL = """
-- Workflow: one record per CDC event processed
CREATE TABLE IF NOT EXISTS workflow (
    id              UUID PRIMARY KEY,
    patient_id      TEXT NOT NULL,
    content         TEXT NOT NULL,
    observations    TEXT,
    top_k           INTEGER NOT NULL DEFAULT 20,
    trial_corpus    TEXT NOT NULL DEFAULT 'clinical_trials_gov',
    model           TEXT NOT NULL DEFAULT 'gpt-4o',
    qrels           JSONB,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    failure_message TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_workflow_patient ON workflow (patient_id);
CREATE INDEX IF NOT EXISTS idx_workflow_status ON workflow (status);

-- Audits: trace every agent event
CREATE TABLE IF NOT EXISTS audits (
    id              SERIAL PRIMARY KEY,
    workflow_id     UUID NOT NULL REFERENCES workflow(id),
    agent_name      TEXT NOT NULL,
    message_type    TEXT NOT NULL,
    audit_type      TEXT NOT NULL,
    payload         JSONB,
    total_tokens    INTEGER,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audits_workflow ON audits (workflow_id);
CREATE INDEX IF NOT EXISTS idx_audits_agent ON audits (agent_name);

-- Prompts: system and user prompt templates for each agent
CREATE TABLE IF NOT EXISTS prompts (
    id              SERIAL PRIMARY KEY,
    agent_name      TEXT NOT NULL,
    prompt_key      TEXT NOT NULL,
    prompt_type     TEXT NOT NULL CHECK (prompt_type IN ('system', 'user')),
    prompt_text     TEXT NOT NULL,
    description     TEXT,
    version_number  INTEGER NOT NULL DEFAULT 1,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE (agent_name, prompt_key, prompt_type)
);

CREATE INDEX IF NOT EXISTS idx_prompts_agent ON prompts (agent_name);
CREATE INDEX IF NOT EXISTS idx_prompts_type ON prompts (prompt_type);

CREATE TABLE IF NOT EXISTS prompts_history (
    id              INTEGER NOT NULL,
    version_number  INTEGER NOT NULL,
    agent_name      TEXT NOT NULL,
    prompt_key      TEXT NOT NULL,
    prompt_type     TEXT NOT NULL CHECK (prompt_type IN ('system', 'user')),
    prompt_text     TEXT NOT NULL,
    description     TEXT,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (id, version_number)
);

CREATE INDEX IF NOT EXISTS idx_prompts_history_agent ON prompts_history (agent_name);

CREATE TABLE IF NOT EXISTS workflow_prompt_version (
    workflow_id         UUID NOT NULL REFERENCES workflow(id),
    prompt_id           INTEGER NOT NULL,
    prompt_version_number INTEGER NOT NULL,
    PRIMARY KEY (workflow_id, prompt_id)
);

-- Ranking results: final ranked trials per workflow
CREATE TABLE IF NOT EXISTS ranking_results (
    id                  SERIAL PRIMARY KEY,
    workflow_id         UUID NOT NULL REFERENCES workflow(id),
    nct_id              TEXT NOT NULL,
    rank                INTEGER NOT NULL,
    combined_score      NUMERIC,
    matching_score      NUMERIC,
    aggregation_score   NUMERIC,
    relevance_score     NUMERIC,
    eligibility_score   NUMERIC,
    brief_title         TEXT,
    created_at          TIMESTAMP DEFAULT NOW(),
    UNIQUE (workflow_id, nct_id)
);

CREATE INDEX IF NOT EXISTS idx_ranking_workflow ON ranking_results (workflow_id);
CREATE INDEX IF NOT EXISTS idx_ranking_nct ON ranking_results (nct_id);

-- Trial eligibility criteria parsed per trial per workflow
CREATE TABLE IF NOT EXISTS trial_eligibility (
    id                  SERIAL PRIMARY KEY,
    nct_id              TEXT NOT NULL,
    eligibility_type    TEXT NOT NULL CHECK (eligibility_type IN ('inclusion', 'exclusion')),
    criteria            TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trial_eligibility_nct ON trial_eligibility (nct_id);

CREATE TABLE IF NOT EXISTS workflow_trial_eligibility (
    id                  SERIAL PRIMARY KEY,
    workflow_id         UUID NOT NULL REFERENCES workflow(id),
    nct_id              TEXT NOT NULL,
    eligibility_type    TEXT NOT NULL CHECK (eligibility_type IN ('inclusion', 'exclusion')),
    criterion_number    INTEGER NOT NULL,
    reasoning           TEXT,
    eligibility_label   TEXT NOT NULL CHECK (eligibility_label IN (
        'not applicable', 'not enough information',
        'included', 'not included',
        'excluded', 'not excluded'
    ))
);

CREATE INDEX IF NOT EXISTS idx_wte_workflow ON workflow_trial_eligibility (workflow_id);
CREATE INDEX IF NOT EXISTS idx_wte_nct ON workflow_trial_eligibility (workflow_id, nct_id);

-- QRELs evaluation results per workflow
CREATE TABLE IF NOT EXISTS workflow_qrels_results (
    id              SERIAL PRIMARY KEY,
    workflow_id     UUID NOT NULL REFERENCES workflow(id),
    metric_name     TEXT NOT NULL,
    metric_value    DOUBLE PRECISION NOT NULL,
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE (workflow_id, metric_name)
);

CREATE INDEX IF NOT EXISTS idx_wqr_workflow ON workflow_qrels_results (workflow_id);
"""


def main():
    if not ADMIN_DB_URL:
        print("Error: ADMIN_DB_URL environment variable is required")
        return

    conn = psycopg2.connect(ADMIN_DB_URL)
    conn.autocommit = True
    cur = conn.cursor()

    # Create all tables
    cur.execute(SCHEMA_SQL)
    print("Tables created.")

    cur.close()
    conn.close()
    print("Admin database setup complete.")


if __name__ == "__main__":
    main()
