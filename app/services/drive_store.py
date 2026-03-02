# app/services/drive_store.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional

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
    - mantém indexed_at intacto (quem setta é mark_indexed)
    - se modified_time mudou -> status NEW, limpa error e indexed_at
    - se não mudou -> só atualiza metadados/last_seen_at
    """
    db = get_db()
    col = db[COLLECTION]

    now_iso = _utc_iso()

    file_id = file_doc["id"]
    modified_time = file_doc.get("modifiedTime")

    existing = await col.find_one({"file_id": file_id}, {"modified_time": 1})

    # base: sempre atualiza metadados
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

    set_on_insert = {
        "status": "NEW",
        "indexed_at": None,
        "error": None,
        "created_at": now_iso,
    }

    update = {
        "$set": set_fields,
        "$setOnInsert": set_on_insert,
    }

    # se mudou modified_time -> força reindex
    if existing and existing.get("modified_time") != modified_time:
        update["$set"]["status"] = "NEW"
        update["$set"]["error"] = None
        update["$set"]["indexed_at"] = None  # <-- aqui é OK, só no $set (não no $setOnInsert)

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
            "$setOnInsert": {
                "file_id": file_id,
                "created_at": now_iso,
            },
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