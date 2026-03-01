from fastapi import APIRouter, BackgroundTasks, HTTPException, Request

from app.services.job_store import create_job, get_job
from app.workers.demo_worker import run_demo_job

from app.drive.scanner import scan_drive_incremental
from app.core.config import GDRIVE_FOLDER_ID
from app.drive.client import list_files_in_folder
from app.ingest.qdrant_indexer import get_qdrant

router = APIRouter()


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


# ✅ WEBHOOK NO ROUTER (não no app)
@router.post("/drive/webhook")
async def drive_webhook(request: Request):
    app = request.app
    h = request.headers

    # headers padrão do Drive
    channel_id = h.get("x-goog-channel-id")
    resource_id = h.get("x-goog-resource-id")
    token = h.get("x-goog-channel-token")
    resource_state = (h.get("x-goog-resource-state") or "").lower()
    message_number = h.get("x-goog-message-number")  # string

    # 1) secret
    secret = getattr(app.state, "drive_webhook_secret", None)
    if not secret:
        raise HTTPException(500, "drive_webhook_secret missing in app.state")
    if token != secret:
        raise HTTPException(401, "invalid token")

    # 2) state store
    store = getattr(app.state, "drive_state_store", None)
    if not store:
        raise HTTPException(500, "drive_state_store missing")

    state = await store.get()
    if not state:
        raise HTTPException(409, "watch not initialized")

    if channel_id != state.channel_id or resource_id != state.resource_id:
        raise HTTPException(409, "unknown channel")

    # 3) ignora eventos que não são mudança real
    # sync = handshake/primeira notificação
    # not_exists = canal expirou/recurso inválido
    if resource_state in {"sync", "not_exists"}:
        return {"ok": True, "ignored": resource_state}

    # 4) redis obrigatório
    redis = getattr(app.state, "redis", None)
    if not redis:
        raise HTTPException(500, "redis pool missing in app.state")

    # 5) DEDUPE por message_number (TTL 1h)
    # (se message_number vier vazio, ainda funciona só com throttle)
    if message_number:
        dedupe_key = f"drive:webhook:dedupe:{channel_id}:{message_number}"
        first = await redis.set(dedupe_key, "1", nx=True, ex=3600)
        if not first:
            return {"ok": True, "deduped": True}

    # 6) THROTTLE: não enfileira um job a cada notificação (TTL curto)
    # útil quando chegam 20 webhooks em sequência.
    cooldown_key = "drive:webhook:cooldown"
    cooldown = await redis.set(cooldown_key, "1", nx=True, ex=10)
    if not cooldown:
        return {"ok": True, "throttled": True}

    # 7) enqueue job
    await redis.enqueue_job("process_drive_changes", {"source": "webhook"})
    return {"ok": True, "enqueued": True, "state": resource_state}

@router.get("/qdrant/ping")
async def qdrant_ping():
    try:
        client = get_qdrant()
        info = client.get_collections()
        return {"ok": True, "collections": [c.name for c in info.collections]}
    except Exception as e:
        raise HTTPException(500, f"qdrant ping failed: {e}")