# app/drive/changes.py
from __future__ import annotations

import os
import io
from googleapiclient.http import MediaIoBaseDownload
from typing import Any, Dict, List, Optional, Tuple

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


class DriveChangesClient:
    """
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
        Retorna: (changes, new_start_page_token)
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
                    # ✅ IMPORTANTE: subcampos do file explícitos
                    fields=(
                        "nextPageToken,newStartPageToken,"
                        "changes(fileId,removed,file("
                        "id,name,mimeType,modifiedTime,size,parents,trashed"
                        "))"
                    ),
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
            self.service.channels().stop(
                body={"id": channel_id, "resourceId": resource_id}
            ).execute()
        except Exception as e:
            print(f"[drive][stop_channel] warn: {e}")

    # ======================================================
    # FILE LOOKUP (PROCESSA SÓ IDs)
    # ======================================================

    def get_file_metadata(self, file_id: str) -> Optional[Dict[str, Any]]:
        """
        Busca metadados do arquivo por ID (rápido).
        Retorna None se não existir/sem permissão.
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
        Busca nome de uma pasta (pra preencher parent_folder_name).
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


    # ======================================================
    # DOWNLOAD / EXPORT (para ingestão)
    # ======================================================

    def download_file_bytes(self, file_id: str) -> bytes:
        """
        Download de arquivos binários (PDF, DOCX, XLSX, etc.) via files.get_media.
        """
        try:
            request = self.service.files().get_media(
                fileId=file_id,
                supportsAllDrives=True,
            )
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024)  # 1MB
            done = False
            while not done:
                _status, done = downloader.next_chunk()
            return fh.getvalue()
        except HttpError as e:
            raise RuntimeError(f"drive download failed file_id={file_id}: {e}") from e

    def export_file_bytes(self, file_id: str, export_mime: str) -> bytes:
        """
        Export de arquivos Google (Docs/Sheets/Slides) via files.export.
        Ex: export_mime="text/plain" ou "text/csv".
        """
        try:
            request = self.service.files().export(
                fileId=file_id,
                mimeType=export_mime,
            )
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024)
            done = False
            while not done:
                _status, done = downloader.next_chunk()
            return fh.getvalue()
        except HttpError as e:
            raise RuntimeError(
                f"drive export failed file_id={file_id} mime={export_mime}: {e}"
            ) from e