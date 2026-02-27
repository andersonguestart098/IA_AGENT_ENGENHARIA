# app/services/drive_state_store.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Any, Dict

from pymongo.collection import Collection
from pymongo import ReturnDocument


@dataclass
class DriveWatchState:
    id: str  # fixed key, e.g. "primary"
    start_page_token: str
    channel_id: str
    resource_id: str
    expiration_ms: Optional[int] = None  # epoch ms from Drive
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

    def __init__(self, collection: Collection, key: str = "primary"):
        self.col = collection
        self.key = key

    def get(self) -> Optional[DriveWatchState]:
        doc = self.col.find_one({"_id": self.key})
        if not doc:
            return None
        return DriveWatchState(
            id=doc["_id"],
            start_page_token=doc["start_page_token"],
            channel_id=doc["channel_id"],
            resource_id=doc["resource_id"],
            expiration_ms=doc.get("expiration_ms"),
            created_at=doc.get("created_at"),
            updated_at=doc.get("updated_at"),
        )

    def upsert_watch(
        self,
        *,
        start_page_token: str,
        channel_id: str,
        resource_id: str,
        expiration_ms: Optional[int],
    ) -> DriveWatchState:
        now = datetime.now(timezone.utc)
        doc = self.col.find_one_and_update(
            {"_id": self.key},
            {
                "$set": {
                    "start_page_token": start_page_token,
                    "channel_id": channel_id,
                    "resource_id": resource_id,
                    "expiration_ms": expiration_ms,
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return DriveWatchState(
            id=doc["_id"],
            start_page_token=doc["start_page_token"],
            channel_id=doc["channel_id"],
            resource_id=doc["resource_id"],
            expiration_ms=doc.get("expiration_ms"),
            created_at=doc.get("created_at"),
            updated_at=doc.get("updated_at"),
        )

    def update_token(self, *, start_page_token: str) -> None:
        now = datetime.now(timezone.utc)
        self.col.update_one(
            {"_id": self.key},
            {"$set": {"start_page_token": start_page_token, "updated_at": now}},
        )

    def clear(self) -> None:
        self.col.delete_one({"_id": self.key})

    def as_dict(self) -> Optional[Dict[str, Any]]:
        st = self.get()
        return st.__dict__ if st else None