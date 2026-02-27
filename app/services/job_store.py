from datetime import datetime
from typing import Any, Dict, Optional
import uuid

from app.core.mongo import get_db

JOBS = "jobs"

async def ensure_indexes() -> None:
    db = get_db()
    await db[JOBS].create_index("status")
    await db[JOBS].create_index("type")

async def create_job(job_type: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    db = get_db()
    job_id = str(uuid.uuid4())
    now = datetime.utcnow()
    doc = {
        "_id": job_id,
        "type": job_type,
        "status": "queued",
        "payload": payload or {},
        "result": None,
        "error": None,
        "created_at": now,
        "updated_at": now,
    }
    await db[JOBS].insert_one(doc)
    return doc

async def set_status(job_id: str, status: str, result: Any = None, error: Optional[str] = None) -> None:
    db = get_db()
    await db[JOBS].update_one(
        {"_id": job_id},
        {"$set": {"status": status, "result": result, "error": error, "updated_at": datetime.utcnow()}}
    )

async def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    db = get_db()
    return await db[JOBS].find_one({"_id": job_id})