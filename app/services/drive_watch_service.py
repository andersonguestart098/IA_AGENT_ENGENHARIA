from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from typing import Optional

from app.drive.changes import DriveChangesClient
from app.core.mongo import get_db
from app.services.drive_state_store import DriveStateStore


def _utcnow_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _get_drive_client() -> DriveChangesClient:
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON não configurado")

    sa_info = json.loads(raw)
    return DriveChangesClient(service_account_info=sa_info)


def _get_store() -> DriveStateStore:
    db = get_db()
    return DriveStateStore(db["drive_state"])


async def ensure_watch_on_startup() -> dict:
    public_base_url = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")
    webhook_secret = os.environ.get("DRIVE_WEBHOOK_SECRET", "")

    if not public_base_url or not webhook_secret:
        return {
            "ok": False,
            "reason": "missing_config",
            "missing": {
                "PUBLIC_BASE_URL": not bool(public_base_url),
                "DRIVE_WEBHOOK_SECRET": not bool(webhook_secret),
            },
        }

    drive = _get_drive_client()
    store = _get_store()

    state = await store.get()
    now_ms = _utcnow_ms()

    # sem state -> cria watch novo
    if not state:
        start_page_token = drive.get_start_page_token()
        resp = drive.watch_changes(
            webhook_url=f"{public_base_url}/drive/webhook",
            token=webhook_secret,
            page_token=start_page_token,
        )

        await store.upsert_watch(
            start_page_token=start_page_token,
            channel_id=resp["id"],
            resource_id=resp["resourceId"],
            expiration_ms=int(resp["expiration"]) if resp.get("expiration") else None,
        )

        return {
            "ok": True,
            "action": "created",
            "start_page_token": start_page_token,
            "expiration_ms": int(resp["expiration"]) if resp.get("expiration") else None,
        }

    # com state e expiration
    if state.expiration_ms:
        hours_left = (state.expiration_ms - now_ms) / (1000 * 60 * 60)

        # saudável
        if hours_left > 12:
            return {
                "ok": True,
                "action": "kept",
                "hours_left": round(hours_left, 2),
            }

        # perto de expirar -> renova
        try:
            if state.channel_id and state.resource_id:
                drive.stop_channel(channel_id=state.channel_id, resource_id=state.resource_id)
        except Exception:
            pass

        resp = drive.watch_changes(
            webhook_url=f"{public_base_url}/drive/webhook",
            token=webhook_secret,
            page_token=state.start_page_token,
        )

        await store.upsert_watch(
            start_page_token=state.start_page_token,
            channel_id=resp["id"],
            resource_id=resp["resourceId"],
            expiration_ms=int(resp["expiration"]) if resp.get("expiration") else None,
        )

        return {
            "ok": True,
            "action": "renewed",
            "hours_left_before": round(hours_left, 2),
            "expiration_ms": int(resp["expiration"]) if resp.get("expiration") else None,
        }

    # state existe mas sem expiration -> recria
    resp = drive.watch_changes(
        webhook_url=f"{public_base_url}/drive/webhook",
        token=webhook_secret,
        page_token=state.start_page_token,
    )

    await store.upsert_watch(
        start_page_token=state.start_page_token,
        channel_id=resp["id"],
        resource_id=resp["resourceId"],
        expiration_ms=int(resp["expiration"]) if resp.get("expiration") else None,
    )

    return {
        "ok": True,
        "action": "recreated_missing_expiration",
        "expiration_ms": int(resp["expiration"]) if resp.get("expiration") else None,
    }