import os
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from arq import cron
from arq.connections import RedisSettings

from app.core.mongo import connect_mongo, close_mongo, get_db
from app.core.config import GDRIVE_FOLDER_ID
from app.drive.scanner import scan_drive_incremental
from app.services.drive_state_store import DriveStateStore
from app.drive.changes import DriveChangesClient


# ======================================================
# HELPERS
# ======================================================

def _utcnow_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def get_drive_client() -> DriveChangesClient:
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON não configurado")

    sa_info = json.loads(raw)
    print(f"[worker][drive] client_email={sa_info.get('client_email')}")
    return DriveChangesClient(service_account_info=sa_info)


def get_drive_state_store() -> DriveStateStore:
    db = get_db()
    return DriveStateStore(db["drive_state"])


# ======================================================
# STARTUP / SHUTDOWN
# ======================================================

async def startup(ctx):
    connect_mongo()
    ctx["drive"] = get_drive_client()
    ctx["store"] = get_drive_state_store()
    print("[worker] startup ok")


async def shutdown(ctx):
    close_mongo()
    print("[worker] shutdown ok")


# ======================================================
# JOB: PROCESS CHANGES
# ======================================================

async def process_drive_changes(ctx, payload: Optional[Dict[str, Any]] = None):

    store: DriveStateStore = ctx["store"]
    drive: DriveChangesClient = ctx["drive"]

    state = await store.get()

    if not state:
        print("[worker][process] no state")
        return {"ok": False}

    changes, new_start = drive.list_all_changes(
        start_page_token=state.start_page_token
    )

    print(f"[worker][process] changes={len(changes)}")

    await scan_drive_incremental(GDRIVE_FOLDER_ID)

    await store.update_token(start_page_token=new_start)

    return {"ok": True, "changes": len(changes)}


# ======================================================
# AUTO RENEW WATCH
# ======================================================

async def renew_watch_if_needed(ctx):

    PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")
    DRIVE_WEBHOOK_SECRET = os.environ.get("DRIVE_WEBHOOK_SECRET", "")

    if not PUBLIC_BASE_URL or not DRIVE_WEBHOOK_SECRET:
        print("[worker][renew] missing config")
        return

    store: DriveStateStore = ctx["store"]
    drive: DriveChangesClient = ctx["drive"]

    state = await store.get()
    if not state or not state.expiration_ms:
        return

    now_ms = _utcnow_ms()
    hours_left = (state.expiration_ms - now_ms) / (1000 * 60 * 60)

    if hours_left > 12:
        print(f"[worker][renew] ok ({hours_left:.1f}h left)")
        return

    print("[worker][renew] renewing watch...")

    try:
        drive.stop_channel(
            channel_id=state.channel_id,
            resource_id=state.resource_id,
        )
    except Exception as e:
        print("[worker][renew] warn:", e)

    webhook_url = f"{PUBLIC_BASE_URL}/drive/webhook"

    resp = drive.watch_changes(
        webhook_url=webhook_url,
        token=DRIVE_WEBHOOK_SECRET,
        page_token=state.start_page_token,
    )

    await store.upsert_watch(
        start_page_token=state.start_page_token,
        channel_id=resp["id"],
        resource_id=resp["resourceId"],
        expiration_ms=int(resp["expiration"]) if resp.get("expiration") else None,
    )

    print("[worker][renew] renewed ok")


# ======================================================
# REDIS SETTINGS (HEROKU TLS FIX)
# ======================================================

redis_url = os.environ["REDIS_URL"]

if redis_url.startswith("redis://"):
    redis_url = redis_url.replace("redis://", "rediss://", 1)


class WorkerSettings:
    redis_settings = RedisSettings.from_dsn(redis_url)

    functions = [process_drive_changes]

    on_startup = startup
    on_shutdown = shutdown

    cron_jobs = [
        cron(renew_watch_if_needed, minute={0, 10, 20, 30, 40, 50}),
    ]