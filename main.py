# main.py - Python 기반 프록시 서버 (FastAPI + websockets)
import asyncio
import time
from typing import Dict
import requests
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import PlainTextResponse
import os

# --- 설정 ---
app = FastAPI()
# infra-launcher wnth
INFRA_LAUNCHER_URL = os.getenv("INFRA_LAUNCHER_URL", "http://10.0.0.4:8000") 
SESSION_TIMEOUT_SECONDS = 3600  # 1시간

# --- 세션 저장소 (메모리 기반) ---
# 이제 세션 정보에 vmIp와 마지막 활동 시간을 저장합니다.
# {"session-id": {"vmIp": "10.0.2.5", "last_activity": 1678886400.0}}
SESSIONS: Dict[str, Dict] = {}

# --- 헬퍼 함수 ---
async def delete_vm(session_id: str):
    """infra-launcher API를 호출하여 VM을 삭제하고 세션 저장소에서 제거합니다."""
    if session_id in SESSIONS:
        print(f"Deleting VM for session: {session_id}")
        try:
            # infra-launcher의 삭제 API 호출
            response = requests.delete(f"{INFRA_LAUNCHER_URL}/api/vm/{session_id}")
            if response.status_code == 200:
                print(f"✅ VM deletion request successful for session: {session_id}")
            else:
                print(f"Failed to delete VM for session: {session_id}. Status: {response.status_code}, Details: {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Error calling infra-launcher: {e}")
        finally:
            # API 호출 성공 여부와 관계없이 세션 저장소에서 제거
            SESSIONS.pop(session_id, None)

# --- 백그라운드 작업 ---
async def session_cleanup_task():
    """주기적으로 오래된 세션을 확인하고 삭제합니다."""
    while True:
        await asyncio.sleep(60)  # 1분마다 확인
        now = time.time()
        stale_sessions = [
            sid for sid, data in SESSIONS.items()
            if (now - data.get("last_activity", now)) > SESSION_TIMEOUT_SECONDS
        ]
        if stale_sessions:
            print(f"⏰ Found stale sessions: {stale_sessions}")
            for session_id in stale_sessions:
                await delete_vm(session_id)

@app.on_event("startup")
async def startup_event():
    """서버 시작 시 백그라운드 작업을 시작합니다."""
    print("Starting session cleanup task in background.")
    asyncio.create_task(session_cleanup_task())

# --- API 엔드포인트 ---
@app.get("/health")
def health():
    return PlainTextResponse("Proxy OK")

@app.post("/register-session")
async def register_session(payload: Dict[str, str]):
    session_id = payload.get("sessionId")
    vm_ip = payload.get("vmIp")

    if not session_id or not vm_ip:
        raise HTTPException(status_code=400, detail="Missing sessionId or vmIp")

    SESSIONS[session_id] = {
        "vmIp": vm_ip,
        "last_activity": time.time()
    }
    print(f"Session registered: {session_id} → {vm_ip}")
    return {"message": "Session registered successfully"}

@app.websocket("/session/{session_id}")
async def session_proxy(websocket: WebSocket, session_id: str):
    await websocket.accept()

    if session_id not in SESSIONS:
        print(f"Session not found: {session_id}")
        await websocket.close(code=1008)
        return

    # 활동 시간 갱신
    SESSIONS[session_id]["last_activity"] = time.time()
    
    target_ip = SESSIONS[session_id]["vmIp"]
    target_uri = f"ws://{target_ip}:8889"
    
    target_ws = None
    try:
        # 대상 VM의 웹소켓 서버에 연결
        target_ws = await websockets.connect(target_uri)
        print(f"Proxying connection: {session_id} → {target_uri}")

        async def client_to_target():
            """클라이언트 -> 타겟 VM으로 메시지 전달"""
            try:
                while True:
                    msg = await websocket.receive_text()
                    # 활동 시간 갱신
                    if session_id in SESSIONS:
                        SESSIONS[session_id]["last_activity"] = time.time()
                    await target_ws.send(msg)
            except WebSocketDisconnect:
                pass # 연결 종료 시 루프 탈출

        async def target_to_client():
            """타겟 VM -> 클라이언트로 메시지 전달"""
            try:
                async for msg in target_ws:
                    await websocket.send_text(msg)
            except websockets.exceptions.ConnectionClosed:
                pass # 연결 종료 시 루프 탈출

        # 두 작업을 동시에 실행
        await asyncio.gather(client_to_target(), target_to_client())

    except Exception as e:
        print(f"Connection error for session {session_id}: {e}")
    finally:
        # 연결이 어떤 이유로든 종료되면 항상 실행
        print(f"Closing connection for session: {session_id}")
        if target_ws:
            await target_ws.close()
        if websocket.client_state != 3: # CLOSED
             await websocket.close()
        
        # VM 삭제 함수 호출
        await delete_vm(session_id)
