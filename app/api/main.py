from __future__ import annotations

import os
import json
import asyncio
import traceback
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
from dotenv import load_dotenv

from app.api.routes import router
from app.core.logging import setup_logging
from app.core.mongo import connect_mongo, close_mongo, get_db
from app.core.config import GDRIVE_FOLDER_ID

from app.services.job_store import ensure_indexes
from app.services.drive_store import ensure_drive_indexes
from app.services.drive_state_store import DriveStateStore
from app.services.drive_watch_service import ensure_watch_on_startup

from app.drive.changes import DriveChangesClient
from app.drive.scanner import scan_drive_incremental


PROJECT_ROOT = Path(__file__).resolve().parents[2]

# local/dev
load_dotenv(PROJECT_ROOT / ".env")


# ====================================================
# CONFIG
# ====================================================

PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")
DRIVE_WEBHOOK_SECRET = os.environ.get("DRIVE_WEBHOOK_SECRET", "")

DRIVE_USE_POLLING = os.environ.get("DRIVE_USE_POLLING", "true").lower() in (
    "1",
    "true",
    "yes",
)

DRIVE_POLL_SECONDS = int(os.environ.get("DRIVE_POLL_SECONDS", "60"))


# ====================================================
# LOG HELPER
# ====================================================

def _log(msg: str) -> None:
    print(msg, flush=True)


# ====================================================
# HELPERS
# ====================================================

def get_drive_client() -> DriveChangesClient:
    """
    Suporta:
    1) GOOGLE_SERVICE_ACCOUNT_JSON (Heroku/prod)
    2) GOOGLE_APPLICATION_CREDENTIALS (local/dev)
    """
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if raw:
        sa_info = json.loads(raw)
        _log(f"[drive][client] env_json client_email={sa_info.get('client_email')}")
        return DriveChangesClient(service_account_info=sa_info)

    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not cred_path:
        raise RuntimeError(
            "Credenciais não configuradas. Use GOOGLE_SERVICE_ACCOUNT_JSON "
            "ou GOOGLE_APPLICATION_CREDENTIALS."
        )

    p = Path(cred_path)
    if not p.is_absolute():
        p = PROJECT_ROOT / p

    if not p.exists():
        raise RuntimeError(f"Arquivo de credenciais não encontrado: {p}")

    sa_info = json.loads(p.read_text(encoding="utf-8"))
    _log(f"[drive][client] file_json client_email={sa_info.get('client_email')} path={p}")
    return DriveChangesClient(service_account_info=sa_info)


def get_drive_state_store() -> DriveStateStore:
    db = get_db()
    return DriveStateStore(db["drive_state"])


# ====================================================
# POLLING (fallback)
# ====================================================

async def drive_scheduler(poll_seconds: int = 60):
    while True:
        try:
            _log(f"[drive_scheduler] scanning root_id={GDRIVE_FOLDER_ID}")
            res = await scan_drive_incremental(GDRIVE_FOLDER_ID)
            _log(
                f"[drive_scheduler] ok "
                f"items_seen={res.get('items_seen')} "
                f"folders={res.get('folders_scanned')}"
            )
        except Exception:
            _log("[drive_scheduler] erro:\n" + traceback.format_exc())

        await asyncio.sleep(poll_seconds)


# ====================================================
# EVENT-DRIVEN JOB
# ====================================================

async def sync_drive_changes_job(app: FastAPI):
    """
    Job async chamado pelo webhook.
    Hoje reaproveita o scanner incremental.
    """
    _log("[sync_drive_changes_job] start")

    store: DriveStateStore = app.state.drive_state_store
    drive: DriveChangesClient = app.state.drive_client

    state = await store.get()
    if not state:
        _log("[sync_drive_changes_job] no state")
        return

    _log(
        f"[sync_drive_changes_job] state "
        f"start_page_token={(state.start_page_token or '')[:12]}... "
        f"channel_id={(state.channel_id or '')[:18]}... "
        f"resource_id={(state.resource_id or '')[:18]}..."
    )

    changes, new_start = drive.list_all_changes(
        start_page_token=state.start_page_token
    )

    if changes:
        _log(f"[sync_drive_changes_job] changes={len(changes)} -> scanning incremental")
        await scan_drive_incremental(GDRIVE_FOLDER_ID)
    else:
        _log("[sync_drive_changes_job] no changes")

    await store.update_token(start_page_token=new_start)
    _log(f"[sync_drive_changes_job] token updated new_start={(new_start or '')[:12]}...")


# ====================================================
# LIFESPAN
# ====================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    _log("[startup] begin")

    connect_mongo()
    _log("[startup] mongo connected")

    await ensure_indexes()
    _log("[startup] job indexes ok")

    await ensure_drive_indexes()
    _log("[startup] drive indexes ok")

    app.state.drive_client = get_drive_client()
    app.state.drive_state_store = get_drive_state_store()
    _log("[startup] drive client + drive state store ok")

    if DRIVE_USE_POLLING:
        app.state.drive_task = asyncio.create_task(
            drive_scheduler(poll_seconds=DRIVE_POLL_SECONDS)
        )
        _log(f"[startup] polling ON interval={DRIVE_POLL_SECONDS}s")
    else:
        app.state.drive_task = None
        _log("[startup] polling OFF (event-driven only)")

    # self-healing watch on startup
    try:
        watch_res = await ensure_watch_on_startup()
        _log(f"[startup][watch] {watch_res}")
    except Exception:
        _log("[startup][watch] erro:\n" + traceback.format_exc())

    _log("[startup] application startup complete")

    try:
        yield
    finally:
        _log("[shutdown] begin")

        task = getattr(app.state, "drive_task", None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            _log("[shutdown] drive polling task cancelled")

        close_mongo()
        _log("[shutdown] mongo closed")
        _log("[shutdown] complete")


# ====================================================
# APP
# ====================================================

def create_app() -> FastAPI:
    setup_logging()

    app = FastAPI(
        title="MVP IA - Drive RAG",
        version="0.1.0",
        lifespan=lifespan,
    )

    # ==========================================
    # START / RESET WATCH (fallback/admin)
    # ==========================================
    @app.post("/drive/watch/start")
    async def drive_watch_start():
        _log("[watch_start] requested")

        if not PUBLIC_BASE_URL:
            raise HTTPException(500, "PUBLIC_BASE_URL não configurado")

        if not DRIVE_WEBHOOK_SECRET:
            raise HTTPException(500, "DRIVE_WEBHOOK_SECRET não configurado")

        drive: DriveChangesClient = app.state.drive_client
        store: DriveStateStore = app.state.drive_state_store

        old = await store.get()

        if old:
            _log(
                f"[watch_start] existing state "
                f"channel_id={(old.channel_id or '')[:18]}... "
                f"resource_id={(old.resource_id or '')[:18]}... "
                f"start_page_token={(old.start_page_token or '')[:12]}... "
                f"expiration_ms={old.expiration_ms}"
            )

        if old and old.channel_id and old.resource_id:
            try:
                drive.stop_channel(
                    channel_id=old.channel_id,
                    resource_id=old.resource_id,
                )
                _log("[watch_start] old channel stopped")
            except Exception as e:
                _log(f"[watch_start] warn stop_channel err={type(e).__name__}:{e}")

        start_token = drive.get_start_page_token()
        webhook_url = f"{PUBLIC_BASE_URL}/drive/webhook"

        _log(f"[watch_start] creating watch webhook_url={webhook_url}")

        resp = drive.watch_changes(
            webhook_url=webhook_url,
            token=DRIVE_WEBHOOK_SECRET,
            page_token=start_token,
        )

        await store.upsert_watch(
            start_page_token=start_token,
            channel_id=resp["id"],
            resource_id=resp["resourceId"],
            expiration_ms=int(resp["expiration"]) if resp.get("expiration") else None,
        )

        _log(
            f"[watch_start] ok "
            f"channel_id={resp.get('id')} "
            f"resource_id={resp.get('resourceId')} "
            f"expiration={resp.get('expiration')} "
            f"start_page_token={start_token}"
        )

        return {
            "ok": True,
            "start_page_token": start_token,
            "watch": resp,
        }

    # ==========================================
    # WEBHOOK
    # ==========================================
    @app.post("/drive/webhook")
    async def drive_webhook(request: Request, bg: BackgroundTasks):
        h = request.headers

        channel_id = h.get("x-goog-channel-id")
        resource_id = h.get("x-goog-resource-id")
        token = h.get("x-goog-channel-token")
        resource_state = h.get("x-goog-resource-state")
        message_number = h.get("x-goog-message-number")
        resource_uri = h.get("x-goog-resource-uri")

        _log(
            "[drive_webhook] received "
            f"channel_id={channel_id} "
            f"resource_id={resource_id} "
            f"state={resource_state} "
            f"msg={message_number} "
            f"resource_uri={resource_uri}"
        )

        if token != DRIVE_WEBHOOK_SECRET:
            _log("[drive_webhook] invalid token")
            raise HTTPException(401, "invalid token")

        store: DriveStateStore = app.state.drive_state_store
        state = await store.get()

        if not state:
            _log("[drive_webhook] watch not initialized")
            raise HTTPException(409, "watch not initialized")

        if channel_id != state.channel_id or resource_id != state.resource_id:
            _log(
                "[drive_webhook] unknown channel "
                f"expected_channel={state.channel_id} "
                f"expected_resource={state.resource_id}"
            )
            raise HTTPException(409, "unknown channel")

        bg.add_task(sync_drive_changes_job, app)
        _log("[drive_webhook] sync job enqueued")

        return {"ok": True}

    # ==========================================
    # HEALTH / DEBUG
    # ==========================================
    @app.get("/drive/health")
    async def drive_health():
        store: DriveStateStore = app.state.drive_state_store
        state = await store.get()

        if not state:
            return {
                "ok": False,
                "watch_initialized": False,
                "public_base_url": bool(PUBLIC_BASE_URL),
                "webhook_secret": bool(DRIVE_WEBHOOK_SECRET),
                "polling": DRIVE_USE_POLLING,
                "poll_seconds": DRIVE_POLL_SECONDS,
            }

        return {
            "ok": True,
            "watch_initialized": True,
            "channel_id": state.channel_id,
            "resource_id": state.resource_id,
            "start_page_token": state.start_page_token,
            "expiration_ms": state.expiration_ms,
            "public_base_url": bool(PUBLIC_BASE_URL),
            "webhook_secret": bool(DRIVE_WEBHOOK_SECRET),
            "polling": DRIVE_USE_POLLING,
            "poll_seconds": DRIVE_POLL_SECONDS,
        }

    app.include_router(router)
    return app


app = create_app()