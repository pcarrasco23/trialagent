import json
import os
import uuid

import psycopg2
from psycopg2.extras import Json

ADMIN_DB_URL = os.environ.get("ADMIN_DB_URL", "")

AGENT_MESSAGES = {
    "keyword_extraction_agent": "Extracting medical keywords from patient data...",
    "trial_search_agent": "Searching for matching clinical trials...",
    "eligibility_agent": "Evaluating patient eligibility for each trial...",
    "aggregation_agent": "Calculating relevance and eligibility scores for each trial...",
    "ranking_agent": "Ranking trials by best match...",
    "qrels_agent": "Evaluating ranking against relevance judgments...",
}


def create_workflow(
    patient_id: str,
    content: str,
    top_k: int = 20,
    observations: str | None = None,
    trial_corpus: str = "clinical_trials_gov",
    model: str = "gpt-4o",
    qrels: dict | None = None,
    status: str = "pending",
    db_url: str = ADMIN_DB_URL,
) -> str:
    workflow_id = str(uuid.uuid4())
    if not db_url:
        return workflow_id
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO workflow (id, patient_id, content, observations, top_k,
               trial_corpus, model, qrels, status)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                workflow_id,
                patient_id,
                content,
                observations,
                top_k,
                trial_corpus,
                model,
                Json(qrels) if qrels else None,
                status,
            ),
        )
        cur.execute(
            """INSERT INTO workflow_prompt_version (workflow_id, prompt_id, prompt_version_number)
               SELECT %s, id, version_number FROM prompts""",
            (workflow_id,),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"  [workflow] Failed to create: {e}")
    return workflow_id


def update_workflow_status(
    workflow_id: str,
    status: str,
    failure_message: str | None = None,
    db_url: str = ADMIN_DB_URL,
):
    if not db_url:
        return
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute(
            "UPDATE workflow SET status = %s, failure_message = %s WHERE id = %s",
            (status, failure_message, workflow_id),
        )
        cur.execute(
            "SELECT pg_notify('workflow_updates', %s)",
            (
                json.dumps(
                    {
                        "workflow_id": workflow_id,
                        "status": status,
                        "type": "workflow_status_change",
                    }
                ),
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"  [workflow] Failed to update status: {e}")


def get_workflow_observations(
    workflow_id: str, db_url: str = ADMIN_DB_URL
) -> str | None:
    if not db_url:
        return None
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute("SELECT observations FROM workflow WHERE id = %s", (workflow_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row[0] if row else None
    except Exception as e:
        print(f"  [workflow] Failed to get observations: {e}")
        return None


def get_workflow_qrels(workflow_id: str, db_url: str = ADMIN_DB_URL) -> dict | None:
    if not db_url:
        return None
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute("SELECT qrels FROM workflow WHERE id = %s", (workflow_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row[0] if row and row[0] else None
    except Exception as e:
        print(f"  [workflow] Failed to get qrels: {e}")
        return None


def get_workflow_status(workflow_id: str, db_url: str = ADMIN_DB_URL) -> dict | None:
    if not db_url:
        return None
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute(
            "SELECT id, patient_id, status, failure_message, created_at FROM workflow WHERE id = %s",
            (workflow_id,),
        )
        row = cur.fetchone()
        if not row:
            cur.close()
            conn.close()
            return None
        result = dict(
            zip(["id", "patient_id", "status", "failure_message", "created_at"], row)
        )

        # Get a readable message about what the current agent is doing
        cur.execute(
            "SELECT agent_name FROM audits WHERE workflow_id = %s ORDER BY created_at DESC LIMIT 1",
            (workflow_id,),
        )
        audit_row = cur.fetchone()
        agent_name = audit_row[0] if audit_row else None
        result["agent_message"] = AGENT_MESSAGES.get(agent_name) if agent_name else None

        cur.close()
        conn.close()
        return result
    except Exception as e:
        print(f"  [workflow] Failed to get status: {e}")
        return None


def get_qrels_results(workflow_id: str, db_url: str = ADMIN_DB_URL) -> dict | None:
    if not db_url:
        return None
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute(
            "SELECT metric_name, metric_value FROM workflow_qrels_results WHERE workflow_id = %s",
            (workflow_id,),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        if not rows:
            return None
        return {name: value for name, value in rows}
    except Exception as e:
        print(f"  [workflow] Failed to get qrels results: {e}")
        return None


def get_ranking_results(workflow_id: str, db_url: str = ADMIN_DB_URL) -> list[dict]:
    if not db_url:
        return []
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute(
            """SELECT nct_id, rank, combined_score, matching_score,
                      aggregation_score, relevance_score, eligibility_score,
                      brief_title, created_at
               FROM ranking_results
               WHERE workflow_id = %s
               ORDER BY rank""",
            (workflow_id,),
        )
        columns = [desc[0] for desc in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]

        # Enrich each trial with eligibility details
        for row in rows:
            nct_id = row["nct_id"]
            for elig_type in ["inclusion", "exclusion"]:
                cur.execute(
                    "SELECT criteria FROM trial_eligibility WHERE nct_id = %s AND eligibility_type = %s",
                    (nct_id, elig_type),
                )
                criteria_row = cur.fetchone()

                cur.execute(
                    """SELECT criterion_number, reasoning, eligibility_label
                       FROM workflow_trial_eligibility
                       WHERE workflow_id = %s AND nct_id = %s AND eligibility_type = %s
                       ORDER BY criterion_number""",
                    (workflow_id, nct_id, elig_type),
                )
                elig_rows = [
                    {"criterion_number": r[0], "reasoning": r[1], "eligibility_label": r[2]}
                    for r in cur.fetchall()
                ]

                row[elig_type] = {
                    "criteria": criteria_row[0] if criteria_row else "",
                    "eligibility": elig_rows,
                }

        cur.close()
        conn.close()
        return rows
    except Exception as e:
        print(f"  [workflow] Failed to get ranking results: {e}")
        return []
