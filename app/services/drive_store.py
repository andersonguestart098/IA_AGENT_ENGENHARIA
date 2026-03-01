# app/services/drive_store.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional

from pymongo import ASCENDING
from app.core.mongo import get_db

COLLECTION = "drive_files"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _safe_int(v) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        return None


async def ensure_drive_indexes() -> None:
    db = get_db()
    col = db[COLLECTION]

    # unique file_id
    await col.create_index([("file_id", ASCENDING)], unique=True, name="ux_file_id")

    # fluxo de indexação
    await col.create_index(
        [("status", ASCENDING), ("modified_time", ASCENDING)],
        name="ix_status_modified",
    )

    # navegação por pasta
    await col.create_index([("parent_folder_id", ASCENDING)], name="ix_parent_folder_id")


async def upsert_drive_file(file_doc: Dict) -> None:
    """
    Upsert idempotente:
    - mantém status NEW no insert
    - se modified_time mudou => volta status NEW + limpa indexed_at/error
    - se vier removed/trashed => marca status REMOVED (não fica reindexando)
    """
    db = get_db()
    col = db[COLLECTION]

    now = _utcnow()

    file_id = file_doc.get("id") or file_doc.get("fileId")
    if not file_id:
        raise ValueError("upsert_drive_file: file_doc sem 'id'/'fileId'")

    modified_time = file_doc.get("modifiedTime")
    is_removed = bool(file_doc.get("removed")) or bool(file_doc.get("trashed"))

    # 1) busca mínima pra saber se mudou modified_time (e evitar setar NEW à toa)
    existing = await col.find_one(
        {"file_id": file_id},
        {"modified_time": 1, "status": 1},
    )

    modified_changed = False
    if existing:
        modified_changed = existing.get("modified_time") != modified_time

    # 2) monta update
    update = {
        "$set": {
            "file_id": file_id,
            "name": file_doc.get("name"),
            "mime_type": file_doc.get("mimeType"),
            "size": _safe_int(file_doc.get("size")),
            "modified_time": modified_time,
            "parent_folder_id": file_doc.get("parent_folder_id"),
            "parent_folder_name": file_doc.get("parent_folder_name"),
            "last_seen_at": now.isoformat(),
        },
        "$setOnInsert": {
            "status": "NEW",
            "indexed_at": None,
            "error": None,
            "created_at": now.isoformat(),
        },
    }

    # 3) se removido/trashed, marca REMOVED e não tenta reindexar
    if is_removed:
        update["$set"]["status"] = "REMOVED"
        update["$set"]["indexed_at"] = None
        update["$set"]["error"] = None

    # 4) se mudou modified_time, força NEW (reindex automático)
    elif existing and modified_changed:
        update["$set"]["status"] = "NEW"
        update["$set"]["indexed_at"] = None
        update["$set"]["error"] = None

    # 5) sempre atualiza updated_at
    update["$set"]["updated_at"] = now.isoformat()

    await col.update_one({"file_id": file_id}, update, upsert=True)


async def list_new_files(limit: int = 50) -> List[Dict]:
    db = get_db()
    col = db[COLLECTION]
    cursor = (
        col.find({"status": "NEW"})
        .sort("modified_time", ASCENDING)
        .limit(limit)
    )
    return [doc async for doc in cursor]