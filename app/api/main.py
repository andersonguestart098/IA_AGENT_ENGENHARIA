import os
import json
import asyncio
import traceback
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Request, BackgroundTasks, HTTPException

from app.api.routes import router
from app.core.logging import setup_logging
from app.core.mongo import connect_mongo, close_mongo, get_db
from app.services.job_store import ensure_indexes
from app.services.drive_store import ensure_drive_indexes

from app.core.config import GDRIVE_FOLDER_ID
from app.drive.scanner import scan_drive_incremental

from app.drive.changes import DriveChangesClient
from app.services.drive_state_store import DriveStateStore
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env")


# ====================================================
# CONFIG
# ====================================================

PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")
DRIVE_WEBHOOK_SECRET = os.environ.get("DRIVE_WEBHOOK_SECRET", "change-me")

DRIVE_USE_POLLING = os.environ.get("DRIVE_USE_POLLING", "true").lower() in (
    "1",
    "true",
    "yes",
)

DRIVE_POLL_SECONDS = int(os.environ.get("DRIVE_POLL_SECONDS", "60"))
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")


# ====================================================
# HELPERS
# ====================================================
import os
import json
from pathlib import Path

print("PROJECT_ROOT:", PROJECT_ROOT)
print("CRED:", os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

def get_drive_client() -> DriveChangesClient:
    # 1) Heroku / produção: JSON direto na env
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if raw:
        sa_info = json.loads(raw)
        return DriveChangesClient(service_account_info=sa_info)

    # 2) Dev: caminho do arquivo
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not cred_path:
        raise RuntimeError(
            "Credenciais não configuradas. Use GOOGLE_SERVICE_ACCOUNT_JSON "
            "ou GOOGLE_APPLICATION_CREDENTIALS."
        )

    p = Path(cred_path)

    # Se veio relativo (tipo secrets/xxx.json), resolve a partir da raiz do projeto
    if not p.is_absolute():
        p = PROJECT_ROOT / p  # usa o PROJECT_ROOT que definimos no topo

    if not p.exists():
        raise RuntimeError(f"Arquivo de credenciais não encontrado: {p}")

    sa_info = json.loads(p.read_text(encoding="utf-8"))
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
            print(f"[drive_scheduler] scanning... root_id={GDRIVE_FOLDER_ID}")
            res = await scan_drive_incremental(GDRIVE_FOLDER_ID)
            print(
                f"[drive_scheduler] ok items_seen={res['items_seen']} folders={res['folders_scanned']}"
            )
        except Exception:
            print("[drive_scheduler] erro:\n" + traceback.format_exc())

        await asyncio.sleep(poll_seconds)


# ====================================================
# EVENT-DRIVEN JOB
# ====================================================

async def sync_drive_changes_job(app: FastAPI):
    """
    Job async (IMPORTANTE pq Mongo é async).
    """

    store: DriveStateStore = app.state.drive_state_store
    drive: DriveChangesClient = app.state.drive_client

    state = await store.get()

    if not state:
        print("[sync_drive_changes_job] no state")
        return

    changes, new_start = drive.list_all_changes(
        start_page_token=state.start_page_token
    )

    if changes:
        print(f"[sync_drive_changes_job] changes={len(changes)}")

        # reaproveita teu scanner atual
        await scan_drive_incremental(GDRIVE_FOLDER_ID)

    await store.update_token(start_page_token=new_start)

    print("[sync_drive_changes_job] token updated")


# ====================================================
# LIFESPAN
# ====================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    connect_mongo()

    await ensure_indexes()
    await ensure_drive_indexes()

    app.state.drive_client = get_drive_client()
    app.state.drive_state_store = get_drive_state_store()

    if DRIVE_USE_POLLING:
        app.state.drive_task = asyncio.create_task(
            drive_scheduler(poll_seconds=DRIVE_POLL_SECONDS)
        )
        print(f"[startup] polling ON ({DRIVE_POLL_SECONDS}s)")
    else:
        app.state.drive_task = None
        print("[startup] polling OFF (event-driven only)")

    try:
        yield
    finally:
        task = getattr(app.state, "drive_task", None)

        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        close_mongo()


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
    # START WATCH
    # ==========================================
    @app.post("/drive/watch/start")
    async def drive_watch_start():

        if not PUBLIC_BASE_URL:
            raise HTTPException(500, "PUBLIC_BASE_URL não configurado")

        drive: DriveChangesClient = app.state.drive_client
        store: DriveStateStore = app.state.drive_state_store

        old = await store.get()

        if old:
            try:
                drive.stop_channel(
                    channel_id=old.channel_id,
                    resource_id=old.resource_id,
                )
            except Exception as e:
                print("[watch_start] warn stop:", e)

        start_token = drive.get_start_page_token()

        webhook_url = f"{PUBLIC_BASE_URL}/drive/webhook"

        resp = drive.watch_changes(
            webhook_url=webhook_url,
            token=DRIVE_WEBHOOK_SECRET,
        )

        await store.upsert_watch(
            start_page_token=start_token,
            channel_id=resp["id"],
            resource_id=resp["resourceId"],
            expiration_ms=int(resp["expiration"])
            if resp.get("expiration")
            else None,
        )

        return {"ok": True, "watch": resp}

    # ==========================================
    # WEBHOOK
    # ==========================================
    @app.post("/drive/webhook")
    async def drive_webhook(request: Request, bg: BackgroundTasks):

        h = request.headers

        channel_id = h.get("x-goog-channel-id")
        resource_id = h.get("x-goog-resource-id")
        token = h.get("x-goog-channel-token")

        if token != DRIVE_WEBHOOK_SECRET:
            raise HTTPException(401, "invalid token")

        store: DriveStateStore = app.state.drive_state_store
        state = await store.get()

        if not state:
            raise HTTPException(409, "watch not initialized")

        if (
            channel_id != state.channel_id
            or resource_id != state.resource_id
        ):
            raise HTTPException(409, "unknown channel")

        # 🔥 roda async corretamente
        bg.add_task(sync_drive_changes_job, app)

        return {"ok": True}

    app.include_router(router)
    return app


app = create_app()