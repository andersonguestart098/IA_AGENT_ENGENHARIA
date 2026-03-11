from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request

from app.services.job_store import create_job, get_job
from app.workers.demo_worker import run_demo_job

from app.drive.scanner import scan_drive_incremental
from app.core.config import GDRIVE_FOLDER_ID
from app.drive.client import list_files_in_folder
from app.ingest.qdrant_indexer import get_qdrant
from app.drive.changes import DriveChangesClient
from app.services.drive_state_store import DriveStateStore


router = APIRouter()


def _log(msg: str) -> None:
    print(msg, flush=True)


@router.get("/health")
def health():
    return {"status": "ok"}


@router.post("/jobs/demo")
async def create_demo_job(background: BackgroundTasks, seconds: int = 5):
    job = await create_job("demo_sleep", payload={"seconds": seconds})
    background.add_task(run_demo_job, job["_id"], seconds)
    return {"job_id": job["_id"], "status": job["status"]}


@router.get("/jobs/{job_id}")
async def read_job(job_id: str):
    job = await get_job(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    job["id"] = job.pop("_id")
    job["created_at"] = job["created_at"].isoformat()
    job["updated_at"] = job["updated_at"].isoformat()
    return job


@router.post("/drive/scan")
async def drive_scan(root_id: str | None = None):
    rid = root_id or GDRIVE_FOLDER_ID
    _log(f"[drive_scan] requested root_id={rid}")
    return await scan_drive_incremental(rid)


@router.get("/drive/debug")
def drive_debug(root_id: str | None = None):
    rid = root_id or GDRIVE_FOLDER_ID
    children = list_files_in_folder(rid)
    return {
        "root_id": rid,
        "children_len": len(children),
        "children_sample": [
            {"id": c.get("id"), "name": c.get("name"), "mimeType": c.get("mimeType")}
            for c in children[:10]
        ],
    }


@router.post("/drive/watch/start")
async def drive_watch_start(request: Request):
    app = request.app

    public_base_url = getattr(app.state, "public_base_url", "")
    webhook_secret = getattr(app.state, "drive_webhook_secret", "")

    if not public_base_url:
        raise HTTPException(500, "PUBLIC_BASE_URL não configurado")

    if not webhook_secret:
        raise HTTPException(500, "DRIVE_WEBHOOK_SECRET não configurado")

    drive: DriveChangesClient = app.state.drive_client
    store: DriveStateStore = app.state.drive_state_store

    _log("[watch_start] requested")

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
    webhook_url = f"{public_base_url}/drive/webhook"

    _log(f"[watch_start] creating watch webhook_url={webhook_url}")

    resp = drive.watch_changes(
        webhook_url=webhook_url,
        token=webhook_secret,
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


@router.post("/drive/webhook")
async def drive_webhook(request: Request):
    app = request.app
    h = request.headers

    channel_id = h.get("x-goog-channel-id")
    resource_id = h.get("x-goog-resource-id")
    token = h.get("x-goog-channel-token")
    resource_state = (h.get("x-goog-resource-state") or "").lower()
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

    # 1) secret
    secret = getattr(app.state, "drive_webhook_secret", None)
    if not secret:
        _log("[drive_webhook] drive_webhook_secret missing in app.state")
        raise HTTPException(500, "drive_webhook_secret missing in app.state")

    if token != secret:
        _log("[drive_webhook] invalid token")
        raise HTTPException(401, "invalid token")

    # 2) state store
    store = getattr(app.state, "drive_state_store", None)
    if not store:
        _log("[drive_webhook] drive_state_store missing")
        raise HTTPException(500, "drive_state_store missing")

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

    # 3) ignora eventos que não são mudança real
    if resource_state in {"sync", "not_exists"}:
        _log(f"[drive_webhook] ignored state={resource_state}")
        return {"ok": True, "ignored": resource_state}

    # 4) redis obrigatório
    redis = getattr(app.state, "redis", None)
    if not redis:
        _log("[drive_webhook] redis pool missing in app.state")
        raise HTTPException(500, "redis pool missing in app.state")

    # 5) DEDUPE por message_number (TTL 1h)
    if message_number:
        dedupe_key = f"drive:webhook:dedupe:{channel_id}:{message_number}"
        first = await redis.set(dedupe_key, "1", nx=True, ex=3600)
        if not first:
            _log(f"[drive_webhook] deduped message_number={message_number}")
            return {"ok": True, "deduped": True}

    # 6) THROTTLE curto
    cooldown_key = "drive:webhook:cooldown"
    cooldown = await redis.set(cooldown_key, "1", nx=True, ex=10)
    if not cooldown:
        _log("[drive_webhook] throttled")
        return {"ok": True, "throttled": True}

    # 7) enqueue worker job
    job = await redis.enqueue_job("process_drive_changes", {"source": "webhook"})
    _log(f"[drive_webhook] worker job enqueued job_id={getattr(job, 'job_id', None)}")

    return {"ok": True, "enqueued": True, "state": resource_state}


@router.get("/drive/health")
async def drive_health(request: Request):
    app = request.app
    store: DriveStateStore = app.state.drive_state_store
    state = await store.get()

    if not state:
        return {
            "ok": False,
            "watch_initialized": False,
            "public_base_url": bool(getattr(app.state, "public_base_url", "")),
            "webhook_secret": bool(getattr(app.state, "drive_webhook_secret", "")),
            "polling": bool(getattr(app.state, "drive_use_polling", False)),
            "poll_seconds": getattr(app.state, "drive_poll_seconds", None),
            "redis": bool(getattr(app.state, "redis", None)),
        }

    return {
        "ok": True,
        "watch_initialized": True,
        "channel_id": state.channel_id,
        "resource_id": state.resource_id,
        "start_page_token": state.start_page_token,
        "expiration_ms": state.expiration_ms,
        "public_base_url": bool(getattr(app.state, "public_base_url", "")),
        "webhook_secret": bool(getattr(app.state, "drive_webhook_secret", "")),
        "polling": bool(getattr(app.state, "drive_use_polling", False)),
        "poll_seconds": getattr(app.state, "drive_poll_seconds", None),
        "redis": bool(getattr(app.state, "redis", None)),
    }


@router.get("/qdrant/ping")
async def qdrant_ping():
    try:
        client = get_qdrant()
        info = client.get_collections()
        return {"ok": True, "collections": [c.name for c in info.collections]}
    except Exception as e:
        raise HTTPException(500, f"qdrant ping failed: {e}")