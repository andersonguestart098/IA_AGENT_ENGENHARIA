import os
import json
import asyncio
import traceback
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse

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

# Local: carrega .env (Heroku ignora isso)
if os.getenv("APP_ENV", "local") == "local":
    load_dotenv(PROJECT_ROOT / ".env")


# ======================================================
# CONFIG
# ======================================================

PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
DRIVE_WEBHOOK_SECRET = os.getenv("DRIVE_WEBHOOK_SECRET", "change-me")

# ✅ default = false (event-driven first)
DRIVE_USE_POLLING = os.getenv("DRIVE_USE_POLLING", "false").lower() in ("1", "true", "yes")
DRIVE_POLL_SECONDS = int(os.getenv("DRIVE_POLL_SECONDS", "60"))


# ======================================================
# HELPERS
# ======================================================

def get_drive_client() -> DriveChangesClient:
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON missing")

    sa_info = json.loads(raw)
    print(f"[drive] env_json client={sa_info.get('client_email')}")
    return DriveChangesClient(service_account_info=sa_info)


def get_drive_state_store() -> DriveStateStore:
    db = get_db()
    return DriveStateStore(db["drive_state"])


def _build_redis_settings_from_env() -> RedisSettings:
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        raise RuntimeError("REDIS_URL missing (adicione Heroku Redis ou defina a env)")

    # Heroku às vezes dá redis:// mas exige TLS -> rediss://
    if redis_url.startswith("redis://"):
        redis_url = redis_url.replace("redis://", "rediss://", 1)

    parsed = urlparse(redis_url)

    if not parsed.hostname or not parsed.port:
        raise RuntimeError(f"REDIS_URL inválida: {redis_url}")

    return RedisSettings(
        host=parsed.hostname,
        port=parsed.port,
        password=parsed.password,
        ssl=True,
        ssl_cert_reqs="none",  # ✅ HEROKU FIX (cert self-signed na cadeia)
    )


# ======================================================
# POLLING (fallback)
# ======================================================

async def drive_scheduler():
    while True:
        try:
            res = await scan_drive_incremental(GDRIVE_FOLDER_ID)
            # teu scanner pode retornar items_seen (ou files_seen). tenta pegar ambos.
            items = res.get("items_seen", res.get("files_seen", None))
            print(f"[polling] ok items={items}")
        except Exception:
            print("[polling] erro:\n" + traceback.format_exc())

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

    # Redis pool (WEB precisa disso pra enfileirar jobs do webhook)
    redis_settings = _build_redis_settings_from_env()
    app.state.redis = await create_pool(redis_settings)

    if DRIVE_USE_POLLING:
        app.state.drive_task = asyncio.create_task(drive_scheduler())
        print(f"[startup] polling ON ({DRIVE_POLL_SECONDS}s)")
    else:
        app.state.drive_task = None
        print("[startup] polling OFF (event-driven only)")

    try:
        yield
    finally:
        # cancela polling se existir
        task = getattr(app.state, "drive_task", None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # fecha redis sempre
        redis = getattr(app.state, "redis", None)
        if redis:
            await redis.close()

        close_mongo()


# ======================================================
# APP
# ======================================================

def create_app() -> FastAPI:
    setup_logging()

    app = FastAPI(
        title="MVP IA - Drive RAG",
        lifespan=lifespan,
    )

    # start watch (chamado manualmente 1x após deploy)
    @app.post("/drive/watch/start")
    async def drive_watch_start():
        if not PUBLIC_BASE_URL:
            raise HTTPException(500, "PUBLIC_BASE_URL missing")

        drive: DriveChangesClient = app.state.drive_client
        store: DriveStateStore = app.state.drive_state_store

        old = await store.get()
        if old:
            try:
                drive.stop_channel(old.channel_id, old.resource_id)
            except Exception as e:
                print("[watch_start] warn stop_channel:", e)

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
            expiration_ms=int(resp["expiration"]) if resp.get("expiration") else None,
        )

        return {"ok": True, "watch": resp}

    app.include_router(router)
    return app


app = create_app()