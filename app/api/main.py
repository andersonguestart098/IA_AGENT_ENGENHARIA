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


def _utcnow_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def get_drive_client():
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    sa_info = json.loads(raw)
    print(f"[worker][drive] client={sa_info.get('client_email')}")
    return DriveChangesClient(service_account_info=sa_info)


def get_drive_state_store():
    db = get_db()
    return DriveStateStore(db["drive_state"])


async def startup(ctx):
    connect_mongo()
    ctx["drive"] = get_drive_client()
    ctx["store"] = get_drive_state_store()


async def shutdown(ctx):
    close_mongo()


async def process_drive_changes(ctx, payload=None):

    store = ctx["store"]
    drive = ctx["drive"]

    state = await store.get()

    if not state:
        print("[worker] no state")
        return

    changes, new_start = drive.list_all_changes(
        start_page_token=state.start_page_token
    )

    print(f"[worker] changes={len(changes)}")

    await scan_drive_incremental(GDRIVE_FOLDER_ID)

    await store.update_token(start_page_token=new_start)


async def renew_watch_if_needed(ctx):

    PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
    DRIVE_WEBHOOK_SECRET = os.getenv("DRIVE_WEBHOOK_SECRET", "")

    store = ctx["store"]
    drive = ctx["drive"]

    state = await store.get()
    if not state or not state.expiration_ms:
        return

    hours_left = (state.expiration_ms - _utcnow_ms()) / (1000 * 60 * 60)

    if hours_left > 12:
        return

    try:
        drive.stop_channel(state.channel_id, state.resource_id)
    except:
        pass

    resp = drive.watch_changes(
        webhook_url=f"{PUBLIC_BASE_URL}/drive/webhook",
        token=DRIVE_WEBHOOK_SECRET,
        page_token=state.start_page_token,
    )

    await store.upsert_watch(
        start_page_token=state.start_page_token,
        channel_id=resp["id"],
        resource_id=resp["resourceId"],
        expiration_ms=int(resp["expiration"]),
    )


# 🔥 HEROKU REDIS FIX
redis_url = os.environ["REDIS_URL"]

if redis_url.startswith("redis://"):
    redis_url = redis_url.replace("redis://", "rediss://", 1)


class WorkerSettings:
    redis_settings = RedisSettings.from_dsn(
        redis_url,
        ssl_cert_reqs="none",
    )

    functions = [process_drive_changes]

    on_startup = startup
    on_shutdown = shutdown

    cron_jobs = [
        cron(renew_watch_if_needed, minute={0, 10, 20, 30, 40, 50}),
    ]