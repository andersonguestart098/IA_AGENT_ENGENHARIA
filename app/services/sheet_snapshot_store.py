# app/services/sheet_snapshot_store.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any

from pymongo import ASCENDING
from app.core.mongo import get_db

COLLECTION = "drive_sheet_snapshots"


def _utc():
    return datetime.now(timezone.utc).isoformat()


async def ensure_sheet_snapshot_indexes() -> None:
    db = get_db()
    col = db[COLLECTION]

    # histórico por versão (file_id + sheet + sheet_sha1)
    await col.create_index(
        [("file_id", ASCENDING), ("sheet", ASCENDING), ("sheet_sha1", ASCENDING)],
        unique=True,
        name="ux_file_sheet_version",
    )
    await col.create_index([("file_id", ASCENDING), ("updated_at", ASCENDING)], name="ix_file_updated")
    await col.create_index([("parent_folder_id", ASCENDING)], name="ix_parent_folder_id")


async def upsert_sheet_snapshot(doc: Dict[str, Any]) -> None:
    """
    Salva uma versão do snapshot (imutável por sheet_sha1).
    - Se o mesmo conteúdo chegar de novo (mesmo sha), apenas atualiza updated_at.
    """
    db = get_db()
    col = db[COLLECTION]

    await col.update_one(
        {
            "file_id": doc["file_id"],
            "sheet": doc["sheet"],
            "sheet_sha1": doc["sheet_sha1"],
        },
        {
            "$set": {
                "updated_at": _utc(),
            },
            "$setOnInsert": {
                **doc,
                "created_at": _utc(),
                "updated_at": _utc(),
            },
        },
        upsert=True,
    )