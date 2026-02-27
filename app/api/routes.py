from fastapi import APIRouter, BackgroundTasks, HTTPException
from app.services.job_store import create_job, get_job
from app.workers.demo_worker import run_demo_job
from app.drive.scanner import scan_drive_incremental
from app.core.config import GDRIVE_FOLDER_ID
from app.drive.client import list_files_in_folder

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
        "children_sample": [{"id": c.get("id"), "name": c.get("name"), "mimeType": c.get("mimeType")} for c in children[:10]],
    }