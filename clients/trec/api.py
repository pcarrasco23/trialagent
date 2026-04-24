"""
TREC Clinical Trials API — serves patient topics from XML and triggers workflows.

Run:
    uvicorn trec.api:app --host 0.0.0.0 --port 8004
"""

import asyncio
import os
import xml.etree.ElementTree as ET
from pathlib import Path

import httpx
import websockets as ws_client
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from fastapi.responses import FileResponse

TRIAL_AGENT_API_URL = os.environ.get("TRIAL_AGENT_API_URL", "http://localhost:8000")
TOP_K_TRIALS = int(os.environ.get("TOP_K_TRIALS", "5"))
DATA_PATH = Path(__file__).parent / "data" / "topics2021.xml"
QRELS_PATH = Path(__file__).parent / "data" / "qrels2021.txt"

app = FastAPI(title="TREC Clinical Trials API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _load_topics() -> list[dict]:
    if not DATA_PATH.exists():
        return []
    tree = ET.parse(DATA_PATH)
    root = tree.getroot()
    topics = []
    for topic in root.findall("topic"):
        number = topic.get("number")
        text = (topic.text or "").strip()
        topics.append({"number": number, "text": text})
    return topics


TOPICS = _load_topics()


def _load_qrels() -> dict[str, dict[str, int]]:
    """Load qrels file into {topic_id: {doc_id: relevance}} format."""
    qrels = {}
    if not QRELS_PATH.exists():
        return qrels
    with open(QRELS_PATH) as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) != 4:
                continue
            topic_id, _, doc_id, relevance = parts
            qrels.setdefault(topic_id, {})[doc_id] = int(relevance)
    return qrels


QRELS = _load_qrels()


@app.get("/api/topics")
def list_topics():
    return [{"number": t["number"], "preview": t["text"][:100]} for t in TOPICS]


@app.get("/api/topics/{topic_number}")
def get_topic(topic_number: str):
    for t in TOPICS:
        if t["number"] == topic_number:
            return t
    raise HTTPException(status_code=404, detail=f"Topic {topic_number} not found")


class RunWorkflowRequest(BaseModel):
    model: str = "gpt-4.1"
    trial_corpus: str = "trec_2021_trial_corpus"
    include_qrels: bool = True


@app.post("/api/topics/{topic_number}/run-workflow")
async def run_workflow(topic_number: str, req: RunWorkflowRequest):
    topic = None
    for t in TOPICS:
        if t["number"] == topic_number:
            topic = t
            break
    if not topic:
        raise HTTPException(status_code=404, detail=f"Topic {topic_number} not found")

    payload = {
        "patient_id": f"trec-2021-{topic_number}",
        "content": topic["text"],
        "top_k": TOP_K_TRIALS,
        "trial_corpus": req.trial_corpus,
        "model": req.model,
    }
    if req.include_qrels:
        payload["qrels"] = QRELS.get(topic_number)

    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        response = await client.post(
            f"{TRIAL_AGENT_API_URL}/trial_agent_workflow",
            json=payload,
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
