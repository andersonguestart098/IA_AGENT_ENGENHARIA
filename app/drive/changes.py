# app/drive/changes.py
from __future__ import annotations

import os
import uuid
from typing import Dict, Any, Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build


SCOPES = ["https://www.googleapis.com/auth/drive"]
# Para só leitura + watch, na prática precisa do escopo de drive (watch é "write-ish").
# Se quiser testar com readonly e der 403, troca pra SCOPES acima (já está).


class DriveChangesClient:
    def __init__(self, *, service_account_info: Dict[str, Any]):
        creds = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=SCOPES,
        )

        self.svc = build(
            "drive",
            "v3",
            credentials=creds,
            cache_discovery=False,
        )

    def get_start_page_token(self) -> str:
        resp = self.svc.changes().getStartPageToken().execute()
        return resp["startPageToken"]

    def list_all_changes(self, *, start_page_token: str) -> tuple[list[dict], str]:
        """
        Busca changes a partir do token, paginando até acabar.
        Retorna (changes, new_start_page_token).
        """
        page_token = start_page_token
        all_changes: list[dict] = []

        while True:
            resp = (
                self.svc.changes()
                .list(
                    pageToken=page_token,
                    spaces="drive",
                    includeRemoved=True,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                )
                .execute()
            )

            all_changes.extend(resp.get("changes", []))

            page_token = resp.get("nextPageToken")
            if not page_token:
                new_start = resp.get("newStartPageToken", start_page_token)
                return all_changes, new_start

    def watch_changes(
        self,
        *,
        webhook_url: str,
        token: str,
        page_token: str,
        channel_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Cria um 'channel' e registra o webhook para mudanças.
        O Drive EXIGE pageToken como PARAM na chamada watch.
        """
        channel_id = channel_id or str(uuid.uuid4())

        body = {
            "id": channel_id,
            "type": "web_hook",
            "address": webhook_url,
            "token": token,
        }

        # ✅ AQUI: pageToken obrigatório
        resp = (
            self.svc.changes()
            .watch(pageToken=page_token, body=body)
            .execute()
        )
        return resp

    def stop_channel(self, *, channel_id: str, resource_id: str) -> None:
        body = {"id": channel_id, "resourceId": resource_id}
        self.svc.channels().stop(body=body).execute()