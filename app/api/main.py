from __future__ import annotations

import os
import json
import asyncio
import traceback
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse

from fastapi import FastAPI
from dotenv import load_dotenv
from arq.connections import RedisSettings, create_pool

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

REDIS_URL = os.environ.get("REDIS_URL", "")
if REDIS_URL.startswith("redis://"):
    REDIS_URL = REDIS_URL.replace("redis://", "rediss://", 1)


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


def get_redis_settings() -> RedisSettings:
    if not REDIS_URL:
        raise RuntimeError("REDIS_URL não configurado")

    parsed = urlparse(REDIS_URL)

    return RedisSettings(
        host=parsed.hostname,
        port=parsed.port or 6379,
        password=parsed.password,
        ssl=True,
        ssl_cert_reqs="none",
    )


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
    app.state.drive_webhook_secret = DRIVE_WEBHOOK_SECRET
    app.state.public_base_url = PUBLIC_BASE_URL
    app.state.drive_use_polling = DRIVE_USE_POLLING
    app.state.drive_poll_seconds = DRIVE_POLL_SECONDS
    _log("[startup] drive client + drive state store ok")

    # redis pool da API (pra enfileirar no worker)
    try:
        app.state.redis = await create_pool(get_redis_settings())
        _log("[startup] redis pool ok")
    except Exception:
        app.state.redis = None
        _log("[startup] redis pool erro:\n" + traceback.format_exc())

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

        redis = getattr(app.state, "redis", None)
        if redis:
            await redis.close()
            _log("[shutdown] redis pool closed")

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

    app.include_router(router)
    return app


app = create_app()