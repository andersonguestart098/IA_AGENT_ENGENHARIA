from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional

from pymongo import ASCENDING
from app.core.mongo import get_db

COLLECTION = "drive_files"


async def ensure_drive_indexes() -> None:
    db = get_db()
    col = db[COLLECTION]
    await col.create_index([("file_id", ASCENDING)], unique=True, name="ux_file_id")
    await col.create_index([("status", ASCENDING), ("modified_time", ASCENDING)], name="ix_status_modified")
    await col.create_index([("parent_folder_id", ASCENDING)], name="ix_parent_folder_id")


async def upsert_drive_file(file_doc: Dict) -> None:
    db = get_db()
    col = db[COLLECTION]

    now = datetime.now(timezone.utc)
    file_id = file_doc["id"]

    update = {
        "$set": {
            "file_id": file_id,
            "name": file_doc.get("name"),
            "mime_type": file_doc.get("mimeType"),
            "size": int(file_doc["size"]) if file_doc.get("size") else None,
            "modified_time": file_doc.get("modifiedTime"),
            "parent_folder_id": file_doc.get("parent_folder_id"),
            "parent_folder_name": file_doc.get("parent_folder_name"),
            "last_seen_at": now.isoformat(),
            "trashed": bool(file_doc.get("trashed", False)),
        },
        "$setOnInsert": {
            "status": "NEW",
            "indexed_at": None,
            "error": None,
            "created_at": now.isoformat(),
        },
    }

    existing = await col.find_one({"file_id": file_id}, {"modified_time": 1, "status": 1})
    if existing and existing.get("modified_time") != file_doc.get("modifiedTime"):
        update["$set"]["status"] = "NEW"
        update["$set"]["indexed_at"] = None
        update["$set"]["error"] = None

    await col.update_one({"file_id": file_id}, update, upsert=True)


async def mark_drive_file_deleted(file_id: str, reason: str = "removed") -> None:
    """
    Marca arquivo como DELETED quando change.removed=True ou sem acesso.
    """
    db = get_db()
    col = db[COLLECTION]

    now = datetime.now(timezone.utc).isoformat()

    await col.update_one(
        {"file_id": file_id},
        {
            "$set": {
                "status": "DELETED",
                "deleted_at": now,
                "delete_reason": reason,
                "updated_at": now,
            }
        },
        upsert=True,
    )


async def list_new_files(limit: int = 50) -> List[Dict]:
    db = get_db()
    col = db[COLLECTION]
    cursor = col.find({"status": "NEW"}).sort("modified_time", ASCENDING).limit(limit)
    return [doc async for doc in cursor]