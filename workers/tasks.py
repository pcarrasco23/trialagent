"""
RQ task for processing workflows.

This module is imported by RQ workers. Each function call processes
one workflow end-to-end.
"""

import asyncio
import os

import psycopg2

# Importing the agent modules registers them with the event bus singleton.
import agents.keyword_extraction_agent  # noqa: F401
import agents.trial_search_agent  # noqa: F401
import agents.eligibility_agent  # noqa: F401
import agents.aggregation_agent  # noqa: F401
import agents.ranking_agent  # noqa: F401
import agents.qrels_agent  # noqa: F401

from agents.event_bus import bus, AgentEvent
from lib.workflow import update_workflow_status

ADMIN_DB_URL = os.environ.get("ADMIN_DB_URL", "")


def _fetch_workflow(workflow_id: str) -> dict | None:
    """Fetch a workflow by ID and mark it as processing."""
    if not ADMIN_DB_URL:
        return None
    conn = psycopg2.connect(ADMIN_DB_URL)
    cur = conn.cursor()
    cur.execute(
        """UPDATE workflow SET status = 'processing'
           WHERE id = %s AND status = 'pending'
           RETURNING id, patient_id, content, top_k, trial_corpus, model""",
        (workflow_id,),
    )
    row = cur.fetchone()
    if row:
        import json

        cur.execute(
            "SELECT pg_notify('workflow_updates', %s)",
            (
                json.dumps(
                    {
                        "workflow_id": workflow_id,
                        "status": "processing",
                        "type": "workflow_status_change",
                    }
                ),
            ),
        )
    conn.commit()
    cur.close()
    conn.close()
    if not row:
        return None
    return dict(
        zip(["id", "patient_id", "content", "top_k", "trial_corpus", "model"], row)
    )


async def _run_workflow(workflow: dict):
    """Run the agent pipeline for a workflow."""
    workflow_id = workflow["id"]
    print(f"  Processing workflow {workflow_id} for patient {workflow['patient_id']}")

    bus.set_workflow_params(
        {
            "top_k": workflow.get("top_k", 20),
            "trial_corpus": workflow.get("trial_corpus", "clinical_trials_gov"),
            "model": workflow.get("model", "gpt-4o"),
        }
    )

    event = AgentEvent(
        message_type="PatientConditions",
        content=workflow["content"],
        workflow_id=workflow_id,
    )
    try:
        await bus.broadcast(event)
        update_workflow_status(workflow_id, "completed")
        print(f"  Workflow {workflow_id} completed")
    except Exception as e:
        update_workflow_status(workflow_id, "failed", str(e))
        print(f"  Workflow {workflow_id} failed: {e}")


def process_workflow(workflow_id: str):
    """RQ job entry point. Fetches workflow, runs the agent pipeline."""
    workflow = _fetch_workflow(workflow_id)
    if not workflow:
        print(f"  Workflow {workflow_id} not found or already claimed")
        return

    asyncio.run(_run_workflow(workflow))
