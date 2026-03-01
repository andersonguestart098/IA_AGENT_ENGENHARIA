from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


class DriveChangesClient:
    """
    Client responsável por:
    - start_page_token
    - listagem de changes
    - watch (webhook)
    - lookup rápido por file_id
    """

    def __init__(self, service_account_info: Dict[str, Any]):
        creds = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=SCOPES,
        )
        self.service = build("drive", "v3", credentials=creds, cache_discovery=False)

    # ======================================================
    # TOKENS / CHANGES
    # ======================================================

    def get_start_page_token(self) -> str:
        resp = self.service.changes().getStartPageToken().execute()
        return resp["startPageToken"]

    def list_all_changes(self, start_page_token: str) -> Tuple[List[Dict[str, Any]], str]:
        """
        Retorna:
            (changes, new_start_page_token)
        changes contém: fileId, file, removed
        """

        changes: List[Dict[str, Any]] = []
        page_token = start_page_token
        new_start_token = start_page_token

        while page_token:
            resp = (
                self.service.changes()
                .list(
                    pageToken=page_token,
                    spaces="drive",
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                    fields="nextPageToken,newStartPageToken,changes(fileId,file,removed)",
                )
                .execute()
            )

            changes.extend(resp.get("changes", []))

            page_token = resp.get("nextPageToken")
            if resp.get("newStartPageToken"):
                new_start_token = resp["newStartPageToken"]

        return changes, new_start_token

    # ======================================================
    # WATCH (WEBHOOK)
    # ======================================================

    def watch_changes(self, webhook_url: str, token: str, page_token: str) -> Dict[str, Any]:
        """
        Cria watch channel no Google Drive.
        """
        body = {
            "id": f"drive-watch-{os.urandom(6).hex()}",
            "type": "web_hook",
            "address": webhook_url,
            "token": token,
        }

        return (
            self.service.changes()
            .watch(
                pageToken=page_token,
                body=body,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
            )
            .execute()
        )

    def stop_channel(self, channel_id: str, resource_id: str) -> None:
        try:
            self.service.channels().stop(body={"id": channel_id, "resourceId": resource_id}).execute()
        except Exception as e:
            print(f"[drive][stop_channel] warn: {e}")

    # ======================================================
    # FILE LOOKUP (🔥 PROCESSA SÓ IDs)
    # ======================================================

    def get_file_metadata(self, file_id: str) -> Optional[Dict[str, Any]]:
        """
        Busca metadados completos do arquivo por ID.
        Retorna None se não existir / sem permissão (404/403).
        """
        try:
            return (
                self.service.files()
                .get(
                    fileId=file_id,
                    fields="id,name,mimeType,modifiedTime,size,parents,trashed",
                    supportsAllDrives=True,
                )
                .execute()
            )
        except HttpError as e:
            print(f"[drive][get_file_metadata] warn file_id={file_id} err={e}")
            return None

    def get_folder_name(self, folder_id: str) -> Optional[str]:
        """
        Busca nome da pasta pai (pra preencher parent_folder_name).
        """
        try:
            meta = (
                self.service.files()
                .get(
                    fileId=folder_id,
                    fields="id,name",
                    supportsAllDrives=True,
                )
                .execute()
            )
            return meta.get("name")
        except HttpError as e:
            print(f"[drive][get_folder_name] warn folder_id={folder_id} err={e}")
            return None