# app/services/drive_state_store.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Any, Dict

from pymongo import ReturnDocument

try:
    # Motor (async)
    from motor.motor_asyncio import AsyncIOMotorCollection
except Exception:  # pragma: no cover
    AsyncIOMotorCollection = Any  # typing fallback


@dataclass
class DriveWatchState:
    id: str  # fixed key, e.g. "primary"
    start_page_token: str
    channel_id: str
    resource_id: str
    expiration_ms: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class DriveStateStore:
    """
    Collection schema (single doc is fine):
      _id: "primary"
      start_page_token: str
      channel_id: str
      resource_id: str
      expiration_ms: int | null
      created_at: datetime
      updated_at: datetime
    """

    def __init__(self, collection: AsyncIOMotorCollection, key: str = "primary"):
        self.col = collection
        self.key = key

    def _to_state(self, doc: Dict[str, Any]) -> DriveWatchState:
        return DriveWatchState(
            id=str(doc["_id"]),
            start_page_token=doc.get("start_page_token", "") or "",
            channel_id=doc.get("channel_id", "") or "",
            resource_id=doc.get("resource_id", "") or "",
            expiration_ms=doc.get("expiration_ms"),
            created_at=doc.get("created_at"),
            updated_at=doc.get("updated_at"),
        )

    async def get(self) -> Optional[DriveWatchState]:
        doc = await self.col.find_one({"_id": self.key})
        return self._to_state(doc) if doc else None

    async def upsert_watch(
        self,
        *,
        start_page_token: str,
        channel_id: str,
        resource_id: str,
        expiration_ms: Optional[int],
    ) -> DriveWatchState:
        now = datetime.now(timezone.utc)

        doc = await self.col.find_one_and_update(
            {"_id": self.key},
            {
                "$set": {
                    "start_page_token": start_page_token,
                    "channel_id": channel_id,
                    "resource_id": resource_id,
                    "expiration_ms": int(expiration_ms) if expiration_ms is not None else None,
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )

        # Em casos raros pode vir None dependendo do driver/config
        if not doc:
            doc = await self.col.find_one({"_id": self.key})
            if not doc:
                raise RuntimeError("Falha ao upsert_watch: documento não encontrado após upsert.")

        return self._to_state(doc)

    async def update_token(self, *, start_page_token: str) -> None:
        now = datetime.now(timezone.utc)
        await self.col.update_one(
            {"_id": self.key},
            {"$set": {"start_page_token": start_page_token, "updated_at": now}},
            upsert=True,
        )

    async def clear(self) -> None:
        await self.col.delete_one({"_id": self.key})

    async def as_dict(self) -> Optional[Dict[str, Any]]:
        st = await self.get()
        return st.__dict__ if st else None