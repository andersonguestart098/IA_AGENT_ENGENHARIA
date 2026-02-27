# app/drive/changes.py
from __future__ import annotations

import uuid
from typing import Dict, Any, List, Optional, Tuple

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


DRIVE_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    # se quiser mínimo: drive.readonly + drive.metadata.readonly (depende do que tu vai fazer)
]


class DriveChangesClient:
    def __init__(self, *, service_account_info: Dict[str, Any]):
        creds = Credentials.from_service_account_info(
            service_account_info, scopes=DRIVE_SCOPES
        )
        self.svc = build("drive", "v3", credentials=creds, cache_discovery=False)

    def get_start_page_token(self) -> str:
        resp = self.svc.changes().getStartPageToken().execute()
        return resp["startPageToken"]

    def watch_changes(
        self,
        *,
        webhook_url: str,
        channel_id: Optional[str] = None,
        token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Creates a webhook channel for changes.
        """
        channel_id = channel_id or str(uuid.uuid4())

        body = {
            "id": channel_id,
            "type": "web_hook",
            "address": webhook_url,
        }
        if token:
            # token custom teu p/ validar origem
            body["token"] = token

        resp = self.svc.changes().watch(body=body).execute()
        # resp contains: id, resourceId, expiration, etc.
        return resp

    def stop_channel(self, *, channel_id: str, resource_id: str) -> None:
        body = {"id": channel_id, "resourceId": resource_id}
        self.svc.channels().stop(body=body).execute()

    def list_changes_page(
        self,
        *,
        page_token: str,
        page_size: int = 100,
        include_removed: bool = True,
        supports_all_drives: bool = True,
        include_items_from_all_drives: bool = True,
    ) -> Dict[str, Any]:
        """
        Returns a single page of changes.
        """
        return (
            self.svc.changes()
            .list(
                pageToken=page_token,
                pageSize=page_size,
                fields="nextPageToken,newStartPageToken,changes(fileId,removed,file(name,mimeType,modifiedTime,trashed,parents))",
                includeRemoved=include_removed,
                supportsAllDrives=supports_all_drives,
                includeItemsFromAllDrives=include_items_from_all_drives,
            )
            .execute()
        )

    def list_all_changes(
        self,
        *,
        start_page_token: str,
        max_pages: int = 50,
    ) -> Tuple[List[Dict[str, Any]], str]:
        """
        Pull changes until nextPageToken ends.
        Returns (changes, new_start_page_token)
        """
        all_changes: List[Dict[str, Any]] = []
        token = start_page_token
        new_start = start_page_token

        for _ in range(max_pages):
            resp = self.list_changes_page(page_token=token)
            changes = resp.get("changes", [])
            all_changes.extend(changes)

            token = resp.get("nextPageToken")
            if not token:
                # Drive may send a newStartPageToken when you're fully caught up
                new_start = resp.get("newStartPageToken", new_start)
                break

        return all_changes, new_start