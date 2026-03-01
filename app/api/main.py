import os
import json
import asyncio
import traceback
from contextlib import asynccontextmanager
from pathlib import Path

from arq.connections import create_pool, RedisSettings
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException

from app.api.routes import router
from app.core.logging import setup_logging
from app.core.mongo import connect_mongo, close_mongo, get_db
from app.services.job_store import ensure_indexes
from app.services.drive_store import ensure_drive_indexes

from app.core.config import GDRIVE_FOLDER_ID
from app.drive.scanner import scan_drive_incremental
from app.drive.changes import DriveChangesClient
from app.services.drive_state_store import DriveStateStore

PROJECT_ROOT = Path(__file__).resolve().parents[2]

if os.getenv("APP_ENV", "local") == "local":
    load_dotenv(PROJECT_ROOT / ".env")


PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
DRIVE_WEBHOOK_SECRET = os.getenv("DRIVE_WEBHOOK_SECRET", "change-me")

DRIVE_USE_POLLING = os.getenv("DRIVE_USE_POLLING", "true").lower() in ("1","true","yes")
DRIVE_POLL_SECONDS = int(os.getenv("DRIVE_POLL_SECONDS", "60"))


# ======================================================
# HELPERS
# ======================================================

def get_drive_client():
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

    if raw:
        sa_info = json.loads(raw)
        print(f"[drive] env_json client={sa_info.get('client_email')}")
        return DriveChangesClient(service_account_info=sa_info)

    raise RuntimeError("Credenciais Drive não configuradas")


def get_drive_state_store():
    db = get_db()
    return DriveStateStore(db["drive_state"])


# ======================================================
# POLLING
# ======================================================

async def drive_scheduler():
    while True:
        try:
            res = await scan_drive_incremental(GDRIVE_FOLDER_ID)
            print(f"[polling] items={res['items_seen']}")
        except Exception:
            print(traceback.format_exc())

        await asyncio.sleep(DRIVE_POLL_SECONDS)


# ======================================================
# LIFESPAN
# ======================================================

@asynccontextmanager
async def lifespan(app: FastAPI):

    connect_mongo()

    await ensure_indexes()
    await ensure_drive_indexes()

    app.state.drive_client = get_drive_client()
    app.state.drive_state_store = get_drive_state_store()
    app.state.drive_webhook_secret = DRIVE_WEBHOOK_SECRET

    redis_url = os.environ["REDIS_URL"]

    if redis_url.startswith("redis://"):
        redis_url = redis_url.replace("redis://", "rediss://", 1)

    app.state.redis = await create_pool(
        RedisSettings.from_dsn(redis_url)
    )

    if DRIVE_USE_POLLING:
        app.state.drive_task = asyncio.create_task(drive_scheduler())

    try:
        yield
    finally:

        task = getattr(app.state, "drive_task", None)
        if task:
            task.cancel()

        redis = getattr(app.state, "redis", None)
        if redis:
            await redis.close()

        close_mongo()


# ======================================================
# APP
# ======================================================

def create_app():

    setup_logging()

    app = FastAPI(
        title="MVP IA - Drive RAG",
        lifespan=lifespan,
    )

    @app.post("/drive/watch/start")
    async def drive_watch_start():

        if not PUBLIC_BASE_URL:
            raise HTTPException(500, "PUBLIC_BASE_URL missing")

        drive = app.state.drive_client
        store = app.state.drive_state_store

        old = await store.get()

        if old:
            try:
                drive.stop_channel(old.channel_id, old.resource_id)
            except:
                pass

        start_token = drive.get_start_page_token()

        resp = drive.watch_changes(
            webhook_url=f"{PUBLIC_BASE_URL}/drive/webhook",
            token=DRIVE_WEBHOOK_SECRET,
            page_token=start_token,
        )

        await store.upsert_watch(
            start_page_token=start_token,
            channel_id=resp["id"],
            resource_id=resp["resourceId"],
            expiration_ms=int(resp["expiration"]),
        )

        return {"ok": True}

    app.include_router(router)

    return app


app = create_app()