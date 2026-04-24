"""
Synthea BigQuery API — queries Google BigQuery for patient and condition data.

Data source: bigquery-public-data.fhir_synthea (FHIR R4 format)

Requires:
    - google-cloud-bigquery
    - GOOGLE_CLOUD_PROJECT env var set to your billing project

Run:
    uvicorn synthea_bigquery.api:app --host 0.0.0.0 --port 8003
"""

import asyncio
import os
from pathlib import Path

import httpx
import websockets as ws_client
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from google.cloud import bigquery

GOOGLE_CLOUD_PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "")
TRIAL_AGENT_API_URL = os.environ.get("TRIAL_AGENT_API_URL", "http://localhost:8000")
TOP_K_TRIALS = int(os.environ.get("TOP_K_TRIALS", "5"))
MAX_CONDITIONS = int(os.environ.get("MAX_CONDITIONS", "50"))
MAX_OBSERVATIONS = int(os.environ.get("MAX_OBSERVATIONS", "50"))
DATASET = "bigquery-public-data.fhir_synthea"

app = FastAPI(title="Synthea BigQuery API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _get_client():
    if not GOOGLE_CLOUD_PROJECT:
        raise HTTPException(
            status_code=500, detail="GOOGLE_CLOUD_PROJECT not configured"
        )
    return bigquery.Client(project=GOOGLE_CLOUD_PROJECT)


def _rows_to_dicts(result) -> list[dict]:
    fields = [field.name for field in result.schema]
    return [dict(zip(fields, row.values())) for row in result]


@app.get("/api/patients")
def list_patients():
    """Return patients born on 1984-12-31."""
    client = _get_client()
    query = f"""
        SELECT p.id,
               p.name[SAFE_OFFSET(0)].family AS family_name,
               p.name[SAFE_OFFSET(0)].given AS given_names,
               p.gender,
               p.birthDate,
               p.address[SAFE_OFFSET(0)].city AS city,
               p.address[SAFE_OFFSET(0)].state AS state
        FROM `{DATASET}.patient` p
        WHERE p.birthDate = '1984-12-31'
        ORDER BY p.name[SAFE_OFFSET(0)].family
    """
    results = client.query(query).result()
    rows = _rows_to_dicts(results)
    for row in rows:
        if row.get("given_names"):
            row["given_names"] = " ".join(row["given_names"])
    return rows


@app.get("/api/patients/{patient_id}/conditions")
def patient_conditions(patient_id: str):
    """Return conditions for a given patient, joined with encounter for dates."""
    client = _get_client()
    query = f"""
        SELECT c.id,
               c.code.coding[SAFE_OFFSET(0)].code AS code,
               c.code.coding[SAFE_OFFSET(0)].display AS display,
               c.clinicalStatus,
               c.onset.dateTime AS onset,
               c.context.encounterId,
               e.period.start AS admittime,
               e.period.end AS dischtime
        FROM `{DATASET}.condition` c
        LEFT JOIN `{DATASET}.encounter` e
            ON c.context.encounterId = e.id
        WHERE c.subject.patientId = @patient_id
        ORDER BY c.onset.dateTime DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("patient_id", "STRING", patient_id),
        ]
    )
    results = client.query(query, job_config=job_config).result()
    rows = _rows_to_dicts(results)
    if not rows:
        raise HTTPException(
            status_code=404, detail=f"No conditions found for patient {patient_id}"
        )
    return rows


@app.get("/api/patients/{patient_id}/observations")
def patient_observations(patient_id: str):
    """Return diagnostic reports with their linked observations for a patient."""
    client = _get_client()
    query = f"""
        SELECT dr.id AS report_id,
               dr.code.coding[SAFE_OFFSET(0)].code AS report_code,
               dr.code.coding[SAFE_OFFSET(0)].display AS report_display,
               dr.status AS report_status,
               dr.effective.dateTime AS effective_date,
               o.id AS observation_id,
               o.code.coding[SAFE_OFFSET(0)].code AS obs_code,
               o.code.coding[SAFE_OFFSET(0)].display AS obs_display,
               o.value.quantity.value AS value_quantity,
               o.value.quantity.unit AS value_unit,
               o.value.string AS value_string
        FROM `{DATASET}.diagnostic_report` dr,
             UNNEST(dr.result) r
        LEFT JOIN `{DATASET}.observation` o
            ON r.observationId = o.id
        WHERE dr.subject.patientId = @patient_id
        ORDER BY dr.effective.dateTime DESC, dr.id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("patient_id", "STRING", patient_id),
        ]
    )
    results = client.query(query, job_config=job_config).result()
    rows = _rows_to_dicts(results)
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No diagnostic reports found for patient {patient_id}",
        )
    return rows


class RunWorkflowRequest(BaseModel):
    model: str = "gpt-4.1"
    trial_corpus: str = "clinical_trials_gov"


@app.post("/api/patients/{patient_id}/run-workflow")
async def run_workflow(patient_id: str, req: RunWorkflowRequest = RunWorkflowRequest()):
    client = _get_client()

    # Fetch patient
    patient_query = f"""
        SELECT p.id, p.gender, p.birthDate
        FROM `{DATASET}.patient` p
        WHERE p.id = @patient_id
    """
    patient_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("patient_id", "STRING", patient_id),
        ]
    )
    patient_rows = _rows_to_dicts(
        client.query(patient_query, job_config=patient_config).result()
    )
    if not patient_rows:
        raise HTTPException(status_code=404, detail=f"Patient {patient_id} not found")
    patient = patient_rows[0]

    # Fetch observations via diagnostic reports
    obs_query = f"""
        SELECT dr.code.coding[SAFE_OFFSET(0)].display AS report_display,
               o.code.coding[SAFE_OFFSET(0)].display AS obs_display,
               o.value.quantity.value AS value_quantity,
               o.value.quantity.unit AS value_unit,
               o.value.string AS value_string
        FROM `{DATASET}.diagnostic_report` dr,
             UNNEST(dr.result) r
        LEFT JOIN `{DATASET}.observation` o
            ON r.observationId = o.id
        WHERE dr.subject.patientId = @patient_id
        ORDER BY dr.effective.dateTime DESC
    """
    obs_rows = _rows_to_dicts(
        client.query(obs_query, job_config=patient_config).result()
    )

    # Fetch conditions
    cond_query = f"""
        SELECT c.code.coding[SAFE_OFFSET(0)].display AS display
        FROM `{DATASET}.condition` c
        WHERE c.subject.patientId = @patient_id
        ORDER BY c.onset.dateTime DESC
    """
    cond_rows = _rows_to_dicts(
        client.query(cond_query, job_config=patient_config).result()
    )

    # Build content string (deduplicated, limited)
    seen_obs = set()
    observation_list = []
    for o in obs_rows:
        display = o.get("obs_display") or o.get("report_display") or "unknown"
        if display in seen_obs:
            continue
        seen_obs.add(display)
        if o.get("value_quantity") is not None:
            value = f"{o['value_quantity']:.2f} {o.get('value_unit') or ''}"
        elif o.get("value_string"):
            value = o["value_string"]
        else:
            value = ""
        observation_list.append(f"- {display}: {value}" if value else f"- {display}")
        if len(observation_list) >= MAX_OBSERVATIONS:
            break

    seen_cond = set()
    condition_list = []
    for c in cond_rows:
        display = c.get("display") or "unknown"
        if display in seen_cond:
            continue
        seen_cond.add(display)
        condition_list.append(f"- {display}")
        if len(condition_list) >= MAX_CONDITIONS:
            break

    patient_desc = f"{patient.get('gender', '')}, DOB: {patient.get('birthDate', '')}"

    content = (
        f"Patient: {patient_desc}\n"
        f"Patient ID: {patient_id}\n\n"
        f"Current conditions ({len(condition_list)}):\n" + "\n".join(condition_list)
    )

    observations_text = (
        (f"Observations ({len(observation_list)}):\n" + "\n".join(observation_list))
        if observation_list
        else None
    )

    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as http_client:
        response = await http_client.post(
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
        return response.json()


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

        _, pending = await asyncio.wait(
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
