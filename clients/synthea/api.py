"""
Read-only API for the synthea patient database.

Serves patient and condition data for the frontend viewer.

Run:
    uvicorn synthea.api:app --host 0.0.0.0 --port 8001
"""

import asyncio
import os
from pathlib import Path

import httpx
import psycopg2
import websockets as ws_client
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

SYNTHEA_FHIR_DB_URL = os.environ.get("SYNTHEA_FHIR_DB_URL", "")
TRIAL_AGENT_API_URL = os.environ.get("TRIAL_AGENT_API_URL", "http://localhost:8000")
TOP_K_TRIALS = int(os.environ.get("TOP_K_TRIALS", "5"))
MAX_CONDITIONS = int(os.environ.get("MAX_CONDITIONS", "50"))
MAX_OBSERVATIONS = int(os.environ.get("MAX_OBSERVATIONS", "50"))

app = FastAPI(title="Synthea Patient Viewer API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _get_conn():
    if not SYNTHEA_FHIR_DB_URL:
        raise HTTPException(
            status_code=500, detail="SYNTHEA_FHIR_DB_URL not configured"
        )
    return psycopg2.connect(SYNTHEA_FHIR_DB_URL)


@app.get("/api/patients")
def list_patients():
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """SELECT p.id, p.family_name, p.given_names, p.gender, p.birth_date,
                  p.city, p.state,
                  EXISTS(SELECT 1 FROM patient_trial_workflow ptw WHERE ptw.patient_id = p.id) AS has_workflows
           FROM patient p
           ORDER BY p.family_name, p.given_names"""
    )
    columns = [desc[0] for desc in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


@app.get("/api/patients/{patient_id}/conditions")
def patient_conditions(patient_id: str):
    conn = _get_conn()
    cur = conn.cursor()

    cur.execute("SELECT id FROM patient WHERE id = %s", (patient_id,))
    if not cur.fetchone():
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Patient not found")

    cur.execute(
        """SELECT c.id, c.code, cc.display, c.clinical_status,
                  c.category, c.onset_datetime
           FROM condition c
           LEFT JOIN condition_code cc ON c.code = cc.code
           WHERE c.patient_id = %s
           ORDER BY c.onset_datetime DESC""",
        (patient_id,),
    )
    columns = [desc[0] for desc in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


@app.get("/api/patients/{patient_id}/observations")
def patient_observations(patient_id: str):
    """Return diagnostic reports with their linked observations for a patient."""
    conn = _get_conn()
    cur = conn.cursor()

    cur.execute("SELECT id FROM patient WHERE id = %s", (patient_id,))
    if not cur.fetchone():
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Patient not found")

    cur.execute(
        """SELECT dr.id AS report_id,
                  dr.code AS report_code,
                  dr.code_display AS report_display,
                  dr.status AS report_status,
                  dr.effective_dt AS effective_date,
                  o.id AS observation_id,
                  o.code AS obs_code,
                  oc.display AS obs_display,
                  o.value_quantity,
                  o.value_unit,
                  o.value_string
           FROM diagnostic_report dr
           LEFT JOIN observation o
               ON dr.encounter_id = o.encounter_id
               AND dr.patient_id = o.patient_id
           LEFT JOIN observation_code oc
               ON o.code = oc.code
           WHERE dr.patient_id = %s
           ORDER BY dr.effective_dt, dr.id""",
        (patient_id,),
    )
    columns = [desc[0] for desc in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


class RunWorkflowRequest(BaseModel):
    model: str = "gpt-4.1"
    trial_corpus: str = "clinical_trials_gov"


@app.post("/api/patients/{patient_id}/run-workflow")
async def run_workflow(patient_id: str, req: RunWorkflowRequest = RunWorkflowRequest()):
    conn = _get_conn()
    cur = conn.cursor()

    cur.execute(
        "SELECT id, gender, birth_date FROM patient WHERE id = %s",
        (patient_id,),
    )
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Patient not found")

    patient = dict(zip(["id", "gender", "birth_date"], row))

    # Fetch observations via diagnostic reports
    cur.execute(
        """SELECT oc.display AS obs_display,
                  dr.code_display AS report_display,
                  o.value_quantity, o.value_unit, o.value_string
           FROM diagnostic_report dr
           LEFT JOIN observation o
               ON dr.encounter_id = o.encounter_id
               AND dr.patient_id = o.patient_id
           LEFT JOIN observation_code oc
               ON o.code = oc.code
           WHERE dr.patient_id = %s
           ORDER BY dr.effective_dt DESC""",
        (patient_id,),
    )
    obs_columns = [desc[0] for desc in cur.description]
    obs_rows = [dict(zip(obs_columns, r)) for r in cur.fetchall()]

    # Fetch conditions
    cur.execute(
        """SELECT c.id, c.code, cc.display, c.clinical_status,
                  c.category, c.onset_datetime
           FROM condition c
           LEFT JOIN condition_code cc ON c.code = cc.code
           WHERE c.patient_id = %s
           ORDER BY c.onset_datetime DESC""",
        (patient_id,),
    )
    cond_columns = [desc[0] for desc in cur.description]
    conditions = [dict(zip(cond_columns, r)) for r in cur.fetchall()]
    cur.close()
    conn.close()

    # Build content string (deduplicated, limited)
    seen_obs = set()
    observation_list = []
    for o in obs_rows:
        display = o.get("obs_display") or o.get("report_display") or "unknown"
        if display in seen_obs:
            continue
        seen_obs.add(display)
        if o.get("value_quantity") is not None:
            value = f"{float(o['value_quantity']):.2f} {o.get('value_unit') or ''}"
        elif o.get("value_string"):
            value = o["value_string"]
        else:
            value = ""
        observation_list.append(f"- {display}: {value}" if value else f"- {display}")
        if len(observation_list) >= MAX_OBSERVATIONS:
            break

    seen_cond = set()
    condition_list = []
    for c in conditions:
        display = c.get("display") or c.get("code") or "unknown"
        if display in seen_cond:
            continue
        seen_cond.add(display)
        status = c.get("clinical_status") or ""
        onset = str(c.get("onset_datetime") or "")
        condition_list.append(f"- {display} (status: {status}, onset: {onset})")
        if len(condition_list) >= MAX_CONDITIONS:
            break

    patient_desc = f"{patient.get('gender', '')}, DOB: {patient.get('birth_date', '')}"

    content = (
        f"Patient: {patient_desc}\n"
        f"Patient ID: {patient_id}\n\n"
        f"Current conditions ({len(conditions)}):\n" + "\n".join(condition_list)
    )

    observations_text = (
        (f"Observations ({len(observation_list)}):\n" + "\n".join(observation_list))
        if observation_list
        else None
    )

    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        response = await client.post(
            f"{TRIAL_AGENT_API_URL}/trial_agent_workflow",
            json={
                "patient_id": patient_id,
                "content": content,
                "observations": observations_text,
                "top_k": TOP_K_TRIALS,
                "model": req.model,
                "trial_corpus": req.trial_corpus,
            },
        )
        response.raise_for_status()
        result = response.json()

    workflow_id = result.get("workflow_id")
    if workflow_id:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO patient_trial_workflow (patient_id, trial_workflow_id) VALUES (%s, %s)",
            (patient_id, workflow_id),
        )
        conn.commit()
        cur.close()
        conn.close()

    return result


@app.get("/api/patients/{patient_id}/workflows")
def patient_workflows(patient_id: str):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """SELECT id, patient_id, trial_workflow_id, created_at
           FROM patient_trial_workflow
           WHERE patient_id = %s
           ORDER BY created_at DESC""",
        (patient_id,),
    )
    columns = [desc[0] for desc in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


@app.get("/api/workflows/{workflow_id}/status")
async def workflow_status(workflow_id: str):
    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
        response = await client.get(
            f"{TRIAL_AGENT_API_URL}/trial_agent_workflow/{workflow_id}"
        )
        if response.status_code == 404:
            raise HTTPException(status_code=404, detail="Workflow not found")
        response.raise_for_status()
        return response.json()


@app.get("/api/workflows/{workflow_id}/ranking_results")
async def workflow_ranking_results(workflow_id: str):
    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
        response = await client.get(
            f"{TRIAL_AGENT_API_URL}/trial_agent_workflow/{workflow_id}/ranking_results"
        )
        if response.status_code == 404:
            raise HTTPException(status_code=404, detail="Workflow not found")
        response.raise_for_status()
        return response.json()


@app.get("/api/patients/{patient_id}/ranking_results")
async def patient_ranking_results(patient_id: str):
    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
        response = await client.get(
            f"{TRIAL_AGENT_API_URL}/trial_agent_workflow/patient/{patient_id}/ranking_results"
        )
        if response.status_code == 404:
            raise HTTPException(status_code=404, detail="No workflows found for patient")
        response.raise_for_status()
        return response.json()


@app.get("/api/patients/{patient_id}/workflows")
async def patient_workflows(patient_id: str):
    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
        response = await client.get(
            f"{TRIAL_AGENT_API_URL}/trial_agent_workflow/patient/{patient_id}/workflows"
        )
        if response.status_code == 404:
            raise HTTPException(
                status_code=404, detail="No workflows found for patient"
            )
        response.raise_for_status()
        return response.json()


@app.websocket("/ws/workflow/{workflow_id}")
async def proxy_ws(websocket: WebSocket, workflow_id: str):
    await websocket.accept()
    ws_url = TRIAL_AGENT_API_URL.replace("http://", "ws://")
    async with ws_client.connect(f"{ws_url}/ws/workflow/{workflow_id}") as upstream:

        async def forward_to_client():
            async for message in upstream:
                await websocket.send_text(message)

        async def forward_to_upstream():
            try:
                while True:
                    data = await websocket.receive_text()
                    await upstream.send(data)
            except WebSocketDisconnect:
                pass

        done, pending = await asyncio.wait(
            [
                asyncio.create_task(forward_to_client()),
                asyncio.create_task(forward_to_upstream()),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()


STATIC_DIR = Path(__file__).parent / "frontend" / "dist"

if STATIC_DIR.is_dir():
    app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="static")

    @app.get("/{path:path}")
    def serve_frontend(path: str):
        file = STATIC_DIR / path
        if file.is_file():
            return FileResponse(file)
        return FileResponse(STATIC_DIR / "index.html")
