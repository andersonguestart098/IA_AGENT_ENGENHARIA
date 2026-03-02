from __future__ import annotations

import os
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

from arq import cron
from arq.connections import RedisSettings, create_pool

from app.core.mongo import connect_mongo, close_mongo, get_db
from app.services.drive_state_store import DriveStateStore
from app.drive.changes import DriveChangesClient
from app.services.drive_store import upsert_drive_file, mark_drive_file_deleted
from app.ingest.drive_index_pipeline import index_new_drive_files


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
# REDIS SETTINGS (HEROKU TLS FIX)
# ======================================================

redis_url = os.environ["REDIS_URL"]
if redis_url.startswith("redis://"):
    redis_url = redis_url.replace("redis://", "rediss://", 1)


class WorkerSettings:
    from urllib.parse import urlparse

    parsed = urlparse(redis_url)

    redis_settings = RedisSettings(
        host=parsed.hostname,
        port=parsed.port or 6379,
        password=parsed.password,
        ssl=True,
        ssl_cert_reqs="none",
    )

    functions = []
    on_startup = None
    on_shutdown = None
    cron_jobs = []


# ======================================================
# STARTUP / SHUTDOWN
# ======================================================

async def startup(ctx):
    connect_mongo()

    ctx["drive"] = get_drive_client()
    ctx["store"] = get_drive_state_store()

    # pool do arq (isso retorna ArqRedis e tem enqueue_job)
    ctx["redis"] = await create_pool(WorkerSettings.redis_settings)

    print("[worker] startup ok")


async def shutdown(ctx):
    redis = ctx.get("redis")
    if redis:
        await redis.close()

    close_mongo()
    print("[worker] shutdown ok")


# ======================================================
# PROCESSAMENTO "SÓ IDs" (SEM BFS)
# ======================================================

def _normalize_file_for_upsert(file_obj: Dict[str, Any]) -> Dict[str, Any]:
    # Drive API usa "id"; changes pode vir com "fileId"
    if "id" not in file_obj and "fileId" in file_obj:
        file_obj["id"] = file_obj["fileId"]
    return file_obj


async def _process_changes_only_ids(
    drive: DriveChangesClient,
    changes: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    - removed=True -> marca deleted
    - se não vier change.file -> busca por ID
    - preenche parent_folder_id/name
    """
    upserted = 0
    deleted = 0
    fetched_by_id = 0
    skipped = 0
    errors: List[str] = []

    folder_name_cache: Dict[str, str] = {}

    for ch in changes:
        file_id = ch.get("fileId")
        removed = bool(ch.get("removed", False))

        if not file_id:
            skipped += 1
            continue

        try:
            if removed:
                await mark_drive_file_deleted(file_id, reason="removed")
                deleted += 1
                continue

            file_obj = ch.get("file")
            if not file_obj:
                file_obj = drive.get_file_metadata(file_id)
                if not file_obj:
                    await mark_drive_file_deleted(file_id, reason="metadata_unavailable")
                    deleted += 1
                    continue
                fetched_by_id += 1

            file_obj = _normalize_file_for_upsert(file_obj)

            if file_obj.get("trashed") is True:
                await mark_drive_file_deleted(file_id, reason="trashed")
                deleted += 1
                continue

            parents = file_obj.get("parents") or []
            parent_id = parents[0] if parents else None

            parent_name = ""
            if parent_id:
                if parent_id in folder_name_cache:
                    parent_name = folder_name_cache[parent_id]
                else:
                    parent_name = drive.get_folder_name(parent_id) or ""
                    folder_name_cache[parent_id] = parent_name

            file_obj["parent_folder_id"] = parent_id
            file_obj["parent_folder_name"] = parent_name

            await upsert_drive_file(file_obj)
            upserted += 1

        except Exception as e:
            errors.append(f"{file_id}: {type(e).__name__} {e}")

    return {
        "upserted": upserted,
        "deleted": deleted,
        "fetched_by_id": fetched_by_id,
        "skipped": skipped,
        "errors": errors[:20],
    }


# ======================================================
# JOB: INDEXAÇÃO (ARQ)
# ======================================================

async def index_new_files_job(ctx, payload: Optional[Dict[str, Any]] = None):
    """
    Job separado, pode ser disparado:
    - manualmente
    - após changes
    - cron (se quiser)
    """
    redis = ctx["redis"]

    got_lock = await redis.set("drive:indexing", "1", nx=True, ex=300)
    if not got_lock:
        print("[worker][index] already indexing, skipping")
        return {"ok": False, "reason": "locked"}

    try:
        limit = int((payload or {}).get("limit", 25))
        res = await index_new_drive_files(limit=limit)
        print(f"[worker][index] {res}")
        return res
    finally:
        await redis.delete("drive:indexing")


# ======================================================
# JOB: PROCESS CHANGES
# ======================================================

async def process_drive_changes(ctx, payload: Optional[Dict[str, Any]] = None):
    redis = ctx["redis"]

    lock_id = str(uuid.uuid4())
    got_lock = await redis.set("drive:processing", lock_id, nx=True, ex=180)

    if not got_lock:
        print("[worker] already processing, skipping")
        return {"ok": False, "reason": "locked"}

    try:
        store: DriveStateStore = ctx["store"]
        drive: DriveChangesClient = ctx["drive"]

        state = await store.get()
        if not state:
            print("[worker] no state")
            return {"ok": False, "reason": "no_state"}

        changes, new_start = drive.list_all_changes(start_page_token=state.start_page_token)
        n = len(changes or [])
        print(f"[worker] changes={n}")

        # ✅ se não tem changes: só avança token e sai
        if n == 0:
            await store.update_token(start_page_token=new_start)
            return {"ok": True, "changes": 0, "skipped": "no_changes"}

        # ✅ processa SOMENTE ids afetados
        stats = await _process_changes_only_ids(drive, changes)

        # atualiza token SEMPRE
        await store.update_token(start_page_token=new_start)

        # ✅ AQUI fica o enqueue_job (depois de processar changes)
        # Só dispara indexação se algo foi upsertado ou deletado.
        if (stats.get("upserted", 0) > 0) or (stats.get("deleted", 0) > 0):
            await redis.enqueue_job("index_new_files_job", {"source": "after_changes", "limit": 25})

        return {"ok": True, "changes": n, "stats": stats}

    finally:
        current = await redis.get("drive:processing")
        if current and current.decode() == lock_id:
            await redis.delete("drive:processing")


# ======================================================
# AUTO RENEW WATCH (CRON)
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
        drive.stop_channel(channel_id=state.channel_id, resource_id=state.resource_id)
    except Exception as e:
        print("[worker][renew] warn:", e)

    resp = drive.watch_changes(
        webhook_url=f"{PUBLIC_BASE_URL}/drive/webhook",
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
# WORKER SETTINGS FINAL
# ======================================================

WorkerSettings.functions = [
    process_drive_changes,
    index_new_files_job
]

WorkerSettings.on_startup = startup
WorkerSettings.on_shutdown = shutdown

WorkerSettings.cron_jobs = [
    cron(renew_watch_if_needed, minute={0, 10, 20, 30, 40, 50}),
]