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

# ====================================================
# CONFIG
# ====================================================

PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")
DRIVE_WEBHOOK_SECRET = os.environ.get("DRIVE_WEBHOOK_SECRET", "change-me")

DRIVE_USE_POLLING = os.environ.get("DRIVE_USE_POLLING", "true").lower() in ("1", "true", "yes")
DRIVE_POLL_SECONDS = int(os.environ.get("DRIVE_POLL_SECONDS", "60"))

print("PROJECT_ROOT:", PROJECT_ROOT)
print("[drive] credentials loaded from env")

# ====================================================
# HELPERS
# ====================================================

def get_drive_client() -> DriveChangesClient:
    # Produção (Heroku): JSON direto na env
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if raw:
        sa_info = json.loads(raw)
        print(f"[drive] credentials_source=env_json client_email={sa_info.get('client_email')}")
        return DriveChangesClient(service_account_info=sa_info)

    # Dev (local): caminho do arquivo
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not cred_path:
        raise RuntimeError(
            "Credenciais não configuradas. Use GOOGLE_SERVICE_ACCOUNT_JSON (prod) "
            "ou GOOGLE_APPLICATION_CREDENTIALS (dev)."
        )

    p = Path(cred_path)
    if not p.is_absolute():
        p = PROJECT_ROOT / p

    if not p.exists():
        raise RuntimeError(f"Arquivo de credenciais não encontrado: {p}")

    sa_info = json.loads(p.read_text(encoding="utf-8"))
    print(f"[drive] credentials_source=file_path client_email={sa_info.get('client_email')}")
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
            print(f"[drive_scheduler] ok items_seen={res['items_seen']} folders={res['folders_scanned']}")
        except Exception:
            print("[drive_scheduler] erro:\n" + traceback.format_exc())

        await asyncio.sleep(poll_seconds)


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
    app.state.drive_webhook_secret = DRIVE_WEBHOOK_SECRET

    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        raise RuntimeError("REDIS_URL não configurado (adicione Heroku Redis ou defina a env).")

    app.state.redis = await create_pool(RedisSettings.from_dsn(redis_url))

    if DRIVE_USE_POLLING:
        app.state.drive_task = asyncio.create_task(drive_scheduler(poll_seconds=DRIVE_POLL_SECONDS))
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

        # fecha redis SEMPRE (independente do polling)
        redis = getattr(app.state, "redis", None)
        if redis:
            await redis.close()

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

    @app.post("/drive/watch/start")
    async def drive_watch_start():
        if not PUBLIC_BASE_URL:
            raise HTTPException(500, "PUBLIC_BASE_URL não configurado")

        drive: DriveChangesClient = app.state.drive_client
        store: DriveStateStore = app.state.drive_state_store

        old = await store.get()
        if old:
            try:
                drive.stop_channel(channel_id=old.channel_id, resource_id=old.resource_id)
            except Exception as e:
                print("[watch_start] warn stop:", e)

        start_token = drive.get_start_page_token()
        webhook_url = f"{PUBLIC_BASE_URL}/drive/webhook"

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

        return {"ok": True, "watch": resp}

    app.include_router(router)
    return app


app = create_app()