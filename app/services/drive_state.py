from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Dict

from app.core.mongo import get_db

COLLECTION = "drive_state"
DOC_ID = "default"  # 1 único doc de estado


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def get_state() -> Dict:
    db = get_db()
    col = db[COLLECTION]
    doc = await col.find_one({"_id": DOC_ID})
    if not doc:
        doc = {"_id": DOC_ID, "last_sync_at": None, "updated_at": _now_iso()}
        await col.insert_one(doc)
    return doc


async def get_last_sync_at() -> Optional[str]:
    state = await get_state()
    return state.get("last_sync_at")


async def set_last_sync_at(ts_iso: str) -> None:
    db = get_db()
    col = db[COLLECTION]
    await col.update_one(
        {"_id": DOC_ID},
        {"$set": {"last_sync_at": ts_iso, "updated_at": _now_iso()}},
        upsert=True,
    )