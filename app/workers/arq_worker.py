import os
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

from arq import cron
from arq.connections import RedisSettings, create_pool

from app.core.mongo import connect_mongo, close_mongo, get_db
from app.core.config import GDRIVE_FOLDER_ID
from app.services.drive_state_store import DriveStateStore
from app.drive.changes import DriveChangesClient
from app.services.drive_store import upsert_drive_file


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
# Heroku normalmente fornece redis:// mesmo sendo TLS; troca para rediss://
if redis_url.startswith("redis://"):
    redis_url = redis_url.replace("redis://", "rediss://", 1)


class WorkerSettings:
    # arq RedisSettings.from_dsn não aceita ssl=... em algumas versões,
    # então fazemos parse e setamos ssl aqui.
    from urllib.parse import urlparse

    parsed = urlparse(redis_url)

    redis_settings = RedisSettings(
        host=parsed.hostname,
        port=parsed.port or 6379,
        password=parsed.password,
        ssl=True,
        ssl_cert_reqs="none",  # Heroku Redis usa cadeia que frequentemente dá CERT_VERIFY_FAILED
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

    # deixa redis disponível no ctx (pool do arq)
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
    """
    Garante que o documento tenha as chaves que o upsert usa.
    """
    if "id" not in file_obj and "fileId" in file_obj:
        file_obj["id"] = file_obj["fileId"]
    return file_obj


async def _mark_removed(file_id: str) -> None:
    """
    Marca arquivo removido/trashed (mantém histórico no Mongo).
    """
    await upsert_drive_file(
        {
            "id": file_id,
            "name": None,
            "mimeType": None,
            "size": None,
            "modifiedTime": None,
            "removed": True,
            "trashed": True,
        }
    )


async def _process_changes_only_ids(
    drive: DriveChangesClient,
    changes: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    - Se change.removed => marca removido
    - Se change.file vier => upsert direto
    - Se change.file NÃO vier => busca drive.get_file(fileId) e upsert
    """
    upserted = 0
    removed = 0
    fetched_by_id = 0
    skipped = 0
    errors: List[str] = []

    for ch in changes:
        file_id = ch.get("fileId")
        is_removed = bool(ch.get("removed"))

        if not file_id:
            skipped += 1
            continue

        try:
            if is_removed:
                await _mark_removed(file_id)
                removed += 1
                continue

            file_obj = ch.get("file")
            if not file_obj:
                # não veio objeto completo -> busca por ID
                file_obj = drive.get_file(file_id)
                if not file_obj:
                    skipped += 1
                    continue
                fetched_by_id += 1

            file_obj = _normalize_file_for_upsert(file_obj)
            await upsert_drive_file(file_obj)
            upserted += 1

        except Exception as e:
            errors.append(f"{file_id}: {type(e).__name__} {e}")

    return {
        "upserted": upserted,
        "removed": removed,
        "fetched_by_id": fetched_by_id,
        "skipped": skipped,
        "errors": errors[:20],
    }


# ======================================================
# JOB: PROCESS CHANGES
# ======================================================

async def process_drive_changes(ctx, payload: Optional[Dict[str, Any]] = None):
    redis = ctx["redis"]

    # 🔒 LOCK DISTRIBUÍDO SEGURO
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

        changes, new_start = drive.list_all_changes(
            start_page_token=state.start_page_token
        )

        n = len(changes or [])
        print(f"[worker] changes={n}")

        # ✅ Otimização: se não tem changes, só avança token e sai
        if n == 0:
            await store.update_token(start_page_token=new_start)
            return {"ok": True, "changes": 0, "skipped": "no_changes"}

        # ✅ Processa SOMENTE os IDs afetados (sem BFS)
        stats = await _process_changes_only_ids(drive, changes)

        # Atualiza token do Drive Changes SEMPRE
        await store.update_token(start_page_token=new_start)

        return {"ok": True, "changes": n, "stats": stats}

    finally:
        # unlock seguro (não apaga lock alheio)
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

WorkerSettings.functions = [process_drive_changes]
WorkerSettings.on_startup = startup
WorkerSettings.on_shutdown = shutdown
WorkerSettings.cron_jobs = [
    cron(renew_watch_if_needed, minute={0, 10, 20, 30, 40, 50}),
]