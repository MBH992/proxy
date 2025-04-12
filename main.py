# main.py - Python 기반 프록시 서버 (FastAPI + websockets)
# pip install -r requirements.txt
# uvicorn main:app --host 0.0.0.0 --port 8080

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import PlainTextResponse
import asyncio
import websockets
from typing import Dict

app = FastAPI()

# 세션 저장소 (메모리 기반)
session_store: Dict[str, str] = {}

@app.get("/health")
def health():
    return PlainTextResponse("Proxy OK")

@app.post("/register-session")
def register_session(payload: Dict[str, str]):
    session_id = payload.get("sessionId")
    vm_ip = payload.get("vmIp")

    if not session_id or not vm_ip:
        return {"error": "Missing sessionId or vmIp"}

    session_store[session_id] = vm_ip
    print(f"✅ 등록됨: {session_id} → {vm_ip}")
    return {"message": "등록 완료"}

@app.websocket("/session/{session_id}")
async def session_proxy(websocket: WebSocket, session_id: str):
    await websocket.accept()

    if session_id not in session_store:
        print(f"❌ 세션 미등록: {session_id}")
        await websocket.close()
        return

    target_ip = session_store[session_id]
    target_uri = f"ws://{target_ip}:8889"

    try:
        async with websockets.connect(target_uri) as target_ws:
            print(f"🔁 연결 중계: {session_id} → {target_uri}")

            async def client_to_target():
                try:
                    while True:
                        msg = await websocket.receive_text()
                        await target_ws.send(msg)
                except WebSocketDisconnect:
                    await target_ws.close()

            async def target_to_client():
                try:
                    async for msg in target_ws:
                        await websocket.send_text(msg)
                except Exception:
                    await websocket.close()

            await asyncio.gather(client_to_target(), target_to_client())

    except Exception as e:
        print(f"❗ 연결 실패: {e}")
        await websocket.close()