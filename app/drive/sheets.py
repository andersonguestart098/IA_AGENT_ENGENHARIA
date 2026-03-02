from __future__ import annotations

from typing import Any, Dict, List

from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


class SheetsValuesClient:

    def __init__(self, service_account_info: Dict[str, Any]):
        creds = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=SCOPES,
        )
        self.service = build("sheets", "v4", credentials=creds, cache_discovery=False)

    def list_sheets(self, spreadsheet_id: str) -> List[str]:
        meta = self.service.spreadsheets().get(
            spreadsheetId=spreadsheet_id
        ).execute()

        return [
            s["properties"]["title"]
            for s in meta.get("sheets", [])
        ]

    def get_values(self, spreadsheet_id: str, sheet_name: str):
        """
        Retorna valores calculados (não fórmulas)
        """
        resp = self.service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=sheet_name,
            valueRenderOption="UNFORMATTED_VALUE",  # 🔥 chave da precisão
        ).execute()

        return resp.get("values", [])