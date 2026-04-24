"""
Lightweight server for the MIMIC-IV frontend.
Serves the built React app and proxies /api requests to the mimic-api.
"""

import os
from pathlib import Path

import httpx
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response

MIMIC_API_URL = os.environ.get("MIMIC_API_URL", "http://localhost:8003")
STATIC_DIR = Path(__file__).parent / "dist"

app = FastAPI()


@app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy_api(path: str, request: Request):
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        url = f"{MIMIC_API_URL}/api/{path}"
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


app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="static")


@app.get("/{path:path}")
def serve_frontend(path: str):
    file = STATIC_DIR / path
    if file.is_file():
        return FileResponse(file)
    return FileResponse(STATIC_DIR / "index.html")
