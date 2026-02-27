import os
import time
import logging
from typing import List, Dict, Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

log = logging.getLogger("drive.client")

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
FOLDER_MIME = "application/vnd.google-apps.folder"

_drive_service = None


def get_drive_service():
    """
    Singleton simples do client do Drive (reutiliza conexão e cache interno).
    """
    global _drive_service
    if _drive_service is not None:
        return _drive_service

    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not cred_path:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS não configurado no .env")

    if not os.path.exists(cred_path):
        raise RuntimeError(f"Arquivo de credencial não encontrado: {cred_path}")

    creds = service_account.Credentials.from_service_account_file(
        cred_path,
        scopes=SCOPES,
    )

    _drive_service = build(
        "drive",
        "v3",
        credentials=creds,
        cache_discovery=False,
    )
    return _drive_service


def _should_retry_http_error(e: HttpError) -> bool:
    """
    Regras simples de retry:
    - 429 (rate limit)
    - 5xx (instabilidade)
    - alguns 403 podem ser quota/usage limits (nem sempre), mas tentamos 1-2 retries.
    """
    try:
        status = int(getattr(e.resp, "status", 0) or 0)
    except Exception:
        status = 0

    if status in (429, 500, 502, 503, 504):
        return True

    # Alguns 403 são "rateLimitExceeded" / "userRateLimitExceeded"
    # O body costuma ter reason, mas nem sempre; retry curto é ok.
    if status == 403:
        return True

    return False


def list_files_in_folder(
    folder_id: str,
    modified_after: Optional[str] = None,
    only_folders: bool = False,
    only_files: bool = False,
    page_size: int = 200,
    max_retries: int = 4,
    retry_base_sleep: float = 0.8,
) -> List[Dict]:
    """
    Lista itens dentro de uma pasta.
    - default: traz TUDO (pastas + arquivos)
    - only_folders=True: só pastas
    - only_files=True: só arquivos (não pastas)

    Params:
    - modified_after: RFC3339/ISO com timezone (ex: 2026-02-27T12:00:00Z)
    - max_retries: retries para 429/5xx/alguns 403 (quota)
    """
    if only_folders and only_files:
        raise ValueError("Escolhe só um: only_folders OU only_files")

    service = get_drive_service()

    q_parts = [
        f"'{folder_id}' in parents",
        "trashed = false",
    ]

    if only_folders:
        q_parts.append(f"mimeType = '{FOLDER_MIME}'")
    elif only_files:
        q_parts.append(f"mimeType != '{FOLDER_MIME}'")

    if modified_after:
        # Drive prefere RFC3339; ISO com timezone geralmente funciona.
        q_parts.append(f"modifiedTime > '{modified_after}'")

    q = " and ".join(q_parts)

    results: List[Dict] = []
    page_token: Optional[str] = None

    attempt = 0
    while True:
        try:
            while True:
                resp = (
                    service.files()
                    .list(
                        q=q,
                        fields="nextPageToken, files(id,name,mimeType,modifiedTime,size,parents)",
                        pageSize=page_size,
                        pageToken=page_token,
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True,
                    )
                    .execute()
                )

                results.extend(resp.get("files", []))
                page_token = resp.get("nextPageToken")
                if not page_token:
                    break

            return results

        except HttpError as e:
            attempt += 1
            log.warning(
                "Erro listando folder_id=%s (attempt=%s/%s). status=%s q=%s",
                folder_id,
                attempt,
                max_retries,
                getattr(e.resp, "status", None),
                q,
            )

            if attempt >= max_retries or not _should_retry_http_error(e):
                log.exception("Falha definitiva listando folder_id=%s q=%s", folder_id, q)
                raise RuntimeError(f"Erro no Google Drive API ao listar folder_id={folder_id}") from e

            # backoff exponencial simples
            sleep_s = retry_base_sleep * (2 ** (attempt - 1))
            time.sleep(sleep_s)
            # volta pro loop e tenta de novo