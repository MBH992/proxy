# main.py - Python 기반 프록시 서버 (FastAPI + websockets)
import asyncio
import logging
import time
from typing import Dict
import requests
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, status
from fastapi.responses import PlainTextResponse
import os
import websockets
from websockets import exceptions as ws_exceptions

# --- 설정 ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("proxy")

app = FastAPI()
# infra-launcher wnth
INFRA_LAUNCHER_URL = os.getenv("INFRA_LAUNCHER_URL", "http://10.0.0.4:8000")
SESSION_TIMEOUT_SECONDS = 3600  # 1시간

# --- 세션 저장소 (메모리 기반) ---
# 세션 정보에 vmIp와 마지막 활동 시간 저장
# {"session-id": {"vmIp": "10.0.2.5", "last_activity": 1678886400.0}}
SESSIONS: Dict[str, Dict] = {}

# --- 헬퍼 함수 ---
async def _call_infra_launcher(method: str, endpoint: str, **kwargs) -> requests.Response:
    url = f"{INFRA_LAUNCHER_URL.rstrip('/')}{endpoint}"
    return await asyncio.to_thread(requests.request, method, url, timeout=10, **kwargs)


async def delete_vm(session_id: str):
    """infra-launcher API를 호출하여 VM을 삭제하고 세션 저장소에서 제거합니다."""
    logger.info("Deleting VM for session %s", session_id)
    try:
        response = await _call_infra_launcher("DELETE", f"/api/vm/{session_id}")
        if response.status_code == 200:
            logger.info("VM deletion request successful for session %s", session_id)
        else:
            logger.warning(
                "Failed to delete VM for session %s. Status: %s, Details: %s",
                session_id,
                response.status_code,
                response.text,
            )
    except requests.exceptions.RequestException as e:
        logger.error("Error calling infra-launcher for session %s: %s", session_id, e)
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
            logger.info("Found stale sessions: %s", stale_sessions)
            for session_id in stale_sessions:
                await delete_vm(session_id)

@app.on_event("startup")
async def startup_event():
    """서버 시작 시 백그라운드 작업을 시작합니다."""
    logger.info("Starting session cleanup task in background.")
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
    logger.info("Session registered: %s -> %s", session_id, vm_ip)
    return {"message": "Session registered successfully"}


@app.post("/launch", status_code=status.HTTP_201_CREATED)
async def launch_session():
    """Launch a new user VM via infra-launcher and return session info."""
    try:
        response = await _call_infra_launcher("POST", "/api/launch-vm")
    except requests.exceptions.RequestException as exc:
        logger.error("Failed to reach infra-launcher for launch request: %s", exc)
        raise HTTPException(status_code=502, detail="Infra-launcher is unreachable") from exc

    if response.status_code != 200:
        logger.warning(
            "Infra-launcher launch failed. Status: %s, Details: %s",
            response.status_code,
            response.text,
        )
        try:
            payload = response.json()
        except ValueError:
            payload = {"error": response.text}
        raise HTTPException(status_code=502, detail=payload)

    try:
        data = response.json()
    except ValueError as exc:
        logger.error("Infra-launcher returned non-JSON payload: %s", response.text)
        raise HTTPException(status_code=502, detail="Invalid response from infra-launcher") from exc

    session_id = data.get("session_id")
    vm_ip = data.get("vm_ip")
    if session_id and vm_ip:
        SESSIONS[session_id] = {
            "vmIp": vm_ip,
            "last_activity": time.time()
        }
        logger.info("Session %s launched with VM %s", session_id, vm_ip)
    else:
        logger.warning("Infra-launcher response missing expected fields: %s", data)

    return data


@app.delete("/session/{session_id}")
async def terminate_session(session_id: str):
    """Delete a user VM via infra-launcher and remove session state."""
    try:
        response = await _call_infra_launcher("DELETE", f"/api/vm/{session_id}")
    except requests.exceptions.RequestException as exc:
        logger.error("Failed to reach infra-launcher for delete request: %s", exc)
        raise HTTPException(status_code=502, detail="Infra-launcher is unreachable") from exc

    if response.status_code != 200:
        logger.warning(
            "Infra-launcher delete failed for session %s. Status: %s, Details: %s",
            session_id,
            response.status_code,
            response.text,
        )
        raise HTTPException(status_code=502, detail="Infra-launcher failed to delete the VM")

    SESSIONS.pop(session_id, None)
    try:
        payload = response.json()
    except ValueError:
        payload = {"status": "success", "message": "Deletion requested"}

    return payload

@app.websocket("/session/{session_id}")
async def session_proxy(websocket: WebSocket, session_id: str):
    await websocket.accept()

    if session_id not in SESSIONS:
        logger.warning("Session not found: %s", session_id)
        await websocket.close(code=1008)
        return

    # 활동 시간 갱신
    SESSIONS[session_id]["last_activity"] = time.time()
    
    target_ip = SESSIONS[session_id]["vmIp"]
    target_uri = f"ws://{target_ip}:8889"
    
    target_ws = None
    try:
        # 대상 VM의 웹소켓 서버에 연결
        target_ws = await websockets.connect(
            target_uri,
            open_timeout=10,
            ping_interval=30,
            ping_timeout=30,
        )
        logger.info("Proxying connection: %s -> %s", session_id, target_uri)

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
                logger.info("Client websocket disconnected for session %s", session_id)

        async def target_to_client():
            """타겟 VM -> 클라이언트로 메시지 전달"""
            try:
                async for msg in target_ws:
                    await websocket.send_text(msg)
            except ws_exceptions.ConnectionClosed:
                logger.info("Target websocket closed for session %s", session_id)

        # 두 작업을 동시에 실행
        await asyncio.gather(client_to_target(), target_to_client())

    except (ws_exceptions.InvalidURI, ws_exceptions.InvalidHandshake, OSError) as e:
        logger.error("Websocket setup failed for session %s: %s", session_id, e)
    except Exception as e:
        logger.exception("Connection error for session %s", session_id)
    finally:
        # 연결이 어떤 이유로든 종료되면 항상 실행
        logger.info("Closing connection for session: %s", session_id)
        if target_ws:
            await target_ws.close()
        if websocket.client_state != 3: # CLOSED
             await websocket.close()
        
        # VM 삭제 함수 호출
        await delete_vm(session_id)
