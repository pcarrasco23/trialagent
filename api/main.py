"""
TrialAgent HTTP API.

Accepts PatientConditions messages and queues them as workflow rows.
A separate worker process picks up pending workflows and runs the agent pipeline.

Run:
    uvicorn api.main:app --host 0.0.0.0 --port 8000
"""

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime

import psycopg2
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from redis import Redis
from rq import Queue

from api.pg_listener import PgWorkflowListener
from lib.workflow import create_workflow, get_workflow_status, get_ranking_results

ADMIN_DB_URL = os.environ.get("ADMIN_DB_URL", "")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")

listener = PgWorkflowListener(ADMIN_DB_URL)
redis_conn = Redis.from_url(REDIS_URL)
task_queue = Queue("workflows", connection=redis_conn)


@asynccontextmanager
async def lifespan(app: FastAPI):
    listener.start(asyncio.get_event_loop())
    yield


ALLOWED_MODELS = {
    "gpt-4",
    "gpt-4-turbo",
    "gpt-4o",
    "gpt-4o-mini",
    "gpt-4.1",
    "gpt-4.1-mini",
    "gpt-5.1",
}


class PatientConditionsRequest(BaseModel):
    patient_id: str
    content: str
    observations: str | None = None
    top_k: int = 20
    trial_corpus: str = "clinical_trials_gov"
    model: str = "gpt-4o"
    qrels: dict[str, int] | None = None


class PatientConditionsResponse(BaseModel):
    workflow_id: str
    status: str


app = FastAPI(title="TrialAgent API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/trial_agent_workflow", response_model=PatientConditionsResponse)
async def patient_conditions(
    req: PatientConditionsRequest,
) -> PatientConditionsResponse:
    if req.model not in ALLOWED_MODELS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid model '{req.model}'. Allowed models: {', '.join(sorted(ALLOWED_MODELS))}",
        )
    workflow_id = create_workflow(
        req.patient_id,
        req.content,
        req.top_k,
        observations=req.observations,
        trial_corpus=req.trial_corpus,
        model=req.model,
        qrels=req.qrels,
    )
    print(f"  Workflow ID: {workflow_id}")
    task_queue.enqueue("workers.tasks.process_workflow", workflow_id, job_timeout="30m")
    print(f"  Enqueued RQ job for workflow {workflow_id}")
    return PatientConditionsResponse(workflow_id=workflow_id, status="pending")


class WorkflowStatusResponse(BaseModel):
    id: str
    patient_id: str
    status: str
    failure_message: str | None
    agent_message: str | None
    created_at: datetime


@app.get("/trial_agent_workflow/{workflow_id}", response_model=WorkflowStatusResponse)
async def workflow_status(workflow_id: str) -> WorkflowStatusResponse:
    result = get_workflow_status(workflow_id)
    if not result:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return WorkflowStatusResponse(**result)


@app.get("/trial_agent_workflow/{workflow_id}/ranking_results")
async def ranking_results(workflow_id: str):
    results = get_ranking_results(workflow_id)
    if not results:
        status = get_workflow_status(workflow_id)
        if not status:
            raise HTTPException(status_code=404, detail="Workflow not found")
    return results


@app.get("/trial_agent_workflow/patient/{patient_id}/ranking_results")
def ranking_results_for_patient(patient_id: str):
    """Return ranking results for all workflows belonging to a patient_id."""
    conn = _get_admin_conn()
    cur = conn.cursor()
    cur.execute(
        """SELECT id, model, trial_corpus FROM workflow WHERE patient_id = %s ORDER BY created_at DESC""",
        (patient_id,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail="No workflows found for patient")

    results = []
    for workflow_id, model, trial_corpus in rows:
        ranking = get_ranking_results(workflow_id)
        status = get_workflow_status(workflow_id)
        results.append(
            {
                "workflow_id": workflow_id,
                "model": model,
                "trial_corpus": trial_corpus,
                "status": status,
                "ranking_results": ranking,
            }
        )

    return results


def _get_admin_conn():
    if not ADMIN_DB_URL:
        raise HTTPException(status_code=500, detail="ADMIN_DB_URL not configured")
    return psycopg2.connect(ADMIN_DB_URL)


@app.get("/api/patients")
def list_workflow_patients():
    conn = _get_admin_conn()
    cur = conn.cursor()
    cur.execute(
        """SELECT patient_id, COUNT(*) AS workflow_count,
                  MAX(created_at) AS last_workflow_at
           FROM workflow
           GROUP BY patient_id
           ORDER BY MAX(created_at) DESC"""
    )
    columns = [desc[0] for desc in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


@app.get("/api/patients/{patient_id}/workflows")
def list_patient_workflows(patient_id: str):
    conn = _get_admin_conn()
    cur = conn.cursor()
    cur.execute(
        """SELECT id, patient_id, content, status, failure_message, trial_corpus, model, created_at
           FROM workflow
           WHERE patient_id = %s
           ORDER BY created_at DESC""",
        (patient_id,),
    )
    columns = [desc[0] for desc in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


@app.get("/api/workflows/{workflow_id}/audits")
def list_workflow_audits(workflow_id: str):
    conn = _get_admin_conn()
    cur = conn.cursor()
    cur.execute(
        """SELECT id, workflow_id, agent_name, message_type,
                  audit_type, payload, total_tokens, created_at
           FROM audits
           WHERE workflow_id = %s
           ORDER BY created_at""",
        (workflow_id,),
    )
    columns = [desc[0] for desc in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


@app.get("/api/workflows/{workflow_id}/prompt-versions")
def list_workflow_prompt_versions(workflow_id: str):
    conn = _get_admin_conn()
    cur = conn.cursor()
    cur.execute(
        """SELECT wpv.prompt_id, wpv.prompt_version_number,
                  p.agent_name, p.prompt_key, p.prompt_type
           FROM workflow_prompt_version wpv
           JOIN prompts p ON wpv.prompt_id = p.id
           WHERE wpv.workflow_id = %s
           ORDER BY p.agent_name, p.prompt_key, p.prompt_type""",
        (workflow_id,),
    )
    columns = [desc[0] for desc in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


@app.get("/api/prompts/{prompt_id}/history")
def list_prompt_history(prompt_id: int):
    conn = _get_admin_conn()
    cur = conn.cursor()
    cur.execute(
        """SELECT id, version_number, agent_name, prompt_key, prompt_type,
                  prompt_text, description, is_active, created_at, updated_at
           FROM prompts_history
           WHERE id = %s
           ORDER BY version_number DESC""",
        (prompt_id,),
    )
    columns = [desc[0] for desc in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


@app.get("/api/prompts")
def list_prompts():
    conn = _get_admin_conn()
    cur = conn.cursor()
    cur.execute(
        """SELECT id, agent_name, prompt_key, prompt_type, prompt_text,
                  description, version_number, is_active, created_at, updated_at
           FROM prompts
           ORDER BY agent_name, prompt_key, prompt_type"""
    )
    columns = [desc[0] for desc in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


class UpdatePromptRequest(BaseModel):
    prompt_text: str


@app.put("/api/prompts/{prompt_id}")
def update_prompt(prompt_id: int, req: UpdatePromptRequest):
    conn = _get_admin_conn()
    cur = conn.cursor()

    # Fetch current prompt
    cur.execute("SELECT * FROM prompts WHERE id = %s", (prompt_id,))
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Prompt not found")

    columns = [desc[0] for desc in cur.description]
    prompt = dict(zip(columns, row))

    # Copy current version to history before modifying
    cur.execute(
        """INSERT INTO prompts_history
           (id, version_number, agent_name, prompt_key, prompt_type,
            prompt_text, description, is_active, created_at, updated_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            prompt["id"],
            prompt["version_number"],
            prompt["agent_name"],
            prompt["prompt_key"],
            prompt["prompt_type"],
            prompt["prompt_text"],
            prompt["description"],
            prompt["is_active"],
            prompt["created_at"],
            prompt["updated_at"],
        ),
    )

    # Update prompt text and increment version
    new_version = prompt["version_number"] + 1
    cur.execute(
        """UPDATE prompts
           SET prompt_text = %s, version_number = %s, updated_at = NOW()
           WHERE id = %s""",
        (req.prompt_text, new_version, prompt_id),
    )

    conn.commit()
    cur.close()
    conn.close()
    return {"id": prompt_id, "version_number": new_version}


@app.websocket("/ws/workflow/{workflow_id}")
async def workflow_ws(websocket: WebSocket, workflow_id: str):
    await websocket.accept()
    listener.subscribe(workflow_id, websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        listener.unsubscribe(workflow_id, websocket)
