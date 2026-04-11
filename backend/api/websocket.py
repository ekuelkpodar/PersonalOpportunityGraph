"""
websocket.py — WebSocket endpoint for live pipeline progress streaming.
"""
from __future__ import annotations

import asyncio
import json
from typing import Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter(tags=["websocket"])


@router.websocket("/ws/pipeline")
async def pipeline_progress_ws(websocket: WebSocket):
    """
    WebSocket endpoint that streams pipeline progress events.
    Events are JSON objects: {source, stage, message, pct, data, ts}.
    Sends terminal message when pipeline completes.
    """
    await websocket.accept()

    from backend.pipeline.orchestrator import get_progress_queue

    try:
        while True:
            queue = get_progress_queue()
            if queue is None:
                # No active pipeline — send heartbeat and wait
                await websocket.send_text(json.dumps({
                    "stage": "idle",
                    "message": "No pipeline running",
                    "pct": 0.0,
                }))
                await asyncio.sleep(5)
                continue

            # Drain queue
            try:
                msg = await asyncio.wait_for(queue.get(), timeout=2.0)
                await websocket.send_text(msg)

                data = json.loads(msg)
                if data.get("stage") == "terminal":
                    break
            except asyncio.TimeoutError:
                # Send heartbeat keep-alive
                await websocket.send_text(json.dumps({"stage": "heartbeat"}))
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.5)

    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        try:
            await websocket.close()
        except Exception:
            pass
