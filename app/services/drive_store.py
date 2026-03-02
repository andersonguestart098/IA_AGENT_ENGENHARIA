# app/services/drive_store.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List

from pymongo import ASCENDING
from app.core.mongo import get_db

COLLECTION = "drive_files"


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def ensure_drive_indexes() -> None:
    db = get_db()
    col = db[COLLECTION]

    await col.create_index([("file_id", ASCENDING)], unique=True, name="ux_file_id")
    await col.create_index([("status", ASCENDING), ("modified_time", ASCENDING)], name="ix_status_modified")
    await col.create_index([("parent_folder_id", ASCENDING)], name="ix_parent_folder_id")


async def upsert_drive_file(file_doc: Dict) -> None:
    """
    Regras:
    - nunca duplica paths entre $set e $setOnInsert (evita code 40)
    - status/indexed_at/error só mudam:
        - no INSERT (status=NEW, indexed_at=None, error=None)
        - quando modified_time muda (reindex -> status=NEW, indexed_at=None, error=None)
    - se modified_time NÃO muda: só atualiza metadados/last_seen_at (não mexe em status/indexed_at)
    """
    db = get_db()
    col = db[COLLECTION]

    now_iso = _utc_iso()

    file_id = file_doc["id"]
    modified_time = file_doc.get("modifiedTime")

    existing = await col.find_one({"file_id": file_id}, {"modified_time": 1})

    # Sempre atualiza metadados
    set_fields = {
        "file_id": file_id,
        "name": file_doc.get("name"),
        "mime_type": file_doc.get("mimeType"),
        "size": int(file_doc["size"]) if file_doc.get("size") else None,
        "modified_time": modified_time,
        "parent_folder_id": file_doc.get("parent_folder_id"),
        "parent_folder_name": file_doc.get("parent_folder_name"),
        "trashed": bool(file_doc.get("trashed", False)),
        "last_seen_at": now_iso,
        "updated_at": now_iso,
    }

    update = {
        "$set": set_fields,
        "$setOnInsert": {
            "created_at": now_iso,
        },
    }

    # INSERT
    if not existing:
        update["$set"]["status"] = "NEW"
        update["$set"]["indexed_at"] = None
        update["$set"]["error"] = None

    # UPDATE com mudança => reindex
    elif existing.get("modified_time") != modified_time:
        update["$set"]["status"] = "NEW"
        update["$set"]["indexed_at"] = None
        update["$set"]["error"] = None

    await col.update_one({"file_id": file_id}, update, upsert=True)


async def mark_drive_file_deleted(file_id: str, reason: str = "removed") -> None:
    db = get_db()
    col = db[COLLECTION]
    now_iso = _utc_iso()

    await col.update_one(
        {"file_id": file_id},
        {
            "$set": {
                "status": "DELETED",
                "trashed": True,
                "error": reason,
                "updated_at": now_iso,
            },
            "$setOnInsert": {"created_at": now_iso},
        },
        upsert=True,
    )


async def list_new_files(limit: int = 50) -> List[Dict]:
    db = get_db()
    col = db[COLLECTION]

    cursor = (
        col.find({"status": "NEW", "trashed": {"$ne": True}})
        .sort("modified_time", ASCENDING)
        .limit(limit)
    )
    return [doc async for doc in cursor]


async def mark_indexed(file_id: str) -> None:
    db = get_db()
    col = db[COLLECTION]
    now_iso = _utc_iso()

    await col.update_one(
        {"file_id": file_id},
        {"$set": {"status": "INDEXED", "indexed_at": now_iso, "error": None, "updated_at": now_iso}},
    )


async def mark_error(file_id: str, error: str) -> None:
    db = get_db()
    col = db[COLLECTION]
    now_iso = _utc_iso()

    await col.update_one(
        {"file_id": file_id},
        {"$set": {"status": "ERROR", "error": error[:4000], "updated_at": now_iso}},
    )