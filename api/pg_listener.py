"""
Postgres LISTEN/NOTIFY listener for real-time WebSocket workflow updates.

Runs a background thread that listens on the 'workflow_updates' channel.
When notifications arrive, pushes messages to connected WebSocket clients.
"""

import asyncio
import json
import select
import threading

import psycopg2
import psycopg2.extensions
from fastapi import WebSocket

from lib.workflow import AGENT_MESSAGES


class PgWorkflowListener:
    def __init__(self, db_url: str):
        self._db_url = db_url
        self._connections: dict[str, set[WebSocket]] = {}
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None

    def start(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop
        t = threading.Thread(target=self._listen_loop, daemon=True)
        t.start()
        print("  [pg_listener] Started listening on 'workflow_updates' channel")

    def subscribe(self, workflow_id: str, ws: WebSocket):
        with self._lock:
            self._connections.setdefault(workflow_id, set()).add(ws)

    def unsubscribe(self, workflow_id: str, ws: WebSocket):
        with self._lock:
            if workflow_id in self._connections:
                self._connections[workflow_id].discard(ws)
                if not self._connections[workflow_id]:
                    del self._connections[workflow_id]

    def _listen_loop(self):
        while True:
            try:
                conn = psycopg2.connect(self._db_url)
                conn.set_isolation_level(
                    psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
                )
                cur = conn.cursor()
                cur.execute("LISTEN workflow_updates;")
                print("  [pg_listener] Connected and listening")

                while True:
                    if select.select([conn], [], [], 5.0) == ([], [], []):
                        continue
                    conn.poll()
                    while conn.notifies:
                        notify = conn.notifies.pop(0)
                        self._handle_notify(notify.payload)
            except Exception as e:
                print(f"  [pg_listener] Connection lost: {e}, reconnecting in 2s...")
                import time
                time.sleep(2)

    def _handle_notify(self, payload: str):
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            return

        workflow_id = data.get("workflow_id")
        if not workflow_id:
            return

        msg = {"workflow_id": workflow_id}

        if data.get("type") == "workflow_status_change":
            msg["status"] = data.get("status")
            msg["agent_message"] = None
        elif data.get("agent_message"):
            msg["agent_message"] = data["agent_message"]
            msg["display_type"] = data.get("display_type", "status")
        else:
            agent_name = data.get("agent_name")
            msg["agent_name"] = agent_name
            msg["audit_type"] = data.get("audit_type")
            agent_message = AGENT_MESSAGES.get(agent_name)
            if agent_name == "trial_search_agent" and data.get("trial_corpus"):
                agent_message = f"Searching for matching clinical trials in the {data['trial_corpus']} clinical trial database..."
            msg["agent_message"] = agent_message

        with self._lock:
            sockets = list(self._connections.get(workflow_id, []))

        for ws in sockets:
            asyncio.run_coroutine_threadsafe(
                self._safe_send(ws, msg, workflow_id), self._loop
            )

    async def _safe_send(self, ws: WebSocket, msg: dict, workflow_id: str):
        try:
            await ws.send_json(msg)
        except Exception:
            self.unsubscribe(workflow_id, ws)
