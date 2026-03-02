# app/services/sheet_snapshot_store.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any

from app.core.mongo import get_db

COLLECTION = "drive_sheet_snapshots"


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


async def upsert_sheet_snapshot(doc: Dict[str, Any]) -> None:
    db = get_db()
    col = db[COLLECTION]

    # evita conflito: se vier no doc, remove
    clean = dict(doc)
    clean.pop("updated_at", None)
    clean.pop("created_at", None)

    await col.update_one(
        {"file_id": clean["file_id"], "sheet": clean["sheet"]},
        {
            "$set": {**clean, "updated_at": _utc()},
            "$setOnInsert": {"created_at": _utc()},
        },
        upsert=True,
    )