from datetime import datetime, timezone
from app.core.mongo import get_db

COLLECTION = "drive_sheet_snapshots"


def _utc():
    return datetime.now(timezone.utc).isoformat()


async def upsert_sheet_snapshot(doc):
    db = get_db()
    col = db[COLLECTION]

    await col.update_one(
        {
            "file_id": doc["file_id"],
            "sheet": doc["sheet"],
        },
        {
            "$set": {
                **doc,
                "updated_at": _utc(),
            },
            "$setOnInsert": {
                "created_at": _utc(),
            },
        },
        upsert=True,
    )