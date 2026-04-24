"""
Lightweight server for the TrialAgent Dashboard frontend.
Serves the built React app and proxies /api and /ws requests to the trial-agent-api.
"""

import asyncio
import os
from pathlib import Path

import httpx
import websockets as ws_client
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response

TRIAL_AGENT_API_URL = os.environ.get("TRIAL_AGENT_API_URL", "http://localhost:8000")
STATIC_DIR = Path(__file__).parent / "dist"

app = FastAPI()


@app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy_api(path: str, request: Request):
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        url = f"{TRIAL_AGENT_API_URL}/api/{path}"
        response = await client.request(
            method=request.method,
            url=url,
            headers={k: v for k, v in request.headers.items() if k.lower() != "host"},
            content=await request.body(),
            params=request.query_params,
        )
        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=dict(response.headers),
        )


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
            [asyncio.create_task(forward_to_client()),
             asyncio.create_task(forward_to_upstream())],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()


app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="static")


@app.get("/{path:path}")
def serve_frontend(path: str):
    file = STATIC_DIR / path
    if file.is_file():
        return FileResponse(file)
    return FileResponse(STATIC_DIR / "index.html")
