from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any

from pymongo import ASCENDING
from app.core.mongo import get_db

COLLECTION = "drive_sheet_diffs"


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def ensure_sheet_diff_indexes() -> None:
    db = get_db()
    col = db[COLLECTION]

    await col.create_index(
        [("file_id", ASCENDING), ("sheet", ASCENDING), ("created_at", ASCENDING)],
        name="ix_file_sheet_created"
    )

    await col.create_index(
        [("current_sheet_sha1", ASCENDING)],
        name="ix_current_sheet_sha1"
    )

    await col.create_index(
        [("obra_name", ASCENDING), ("parent_folder_name", ASCENDING), ("created_at", ASCENDING)],
        name="ix_obra_folder_created"
    )

    await col.create_index(
        [("file_name", ASCENDING), ("created_at", ASCENDING)],
        name="ix_file_name_created"
    )


async def insert_sheet_diff(doc: Dict[str, Any]) -> None:
    db = get_db()
    col = db[COLLECTION]

    payload = dict(doc)
    payload["created_at"] = _utc_iso()

    await col.insert_one(payload)