from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Dict, List

from app.drive.client import list_files_in_folder
from app.services.drive_store import upsert_drive_file


def _is_folder(item: Dict) -> bool:
    return item.get("mimeType") == "application/vnd.google-apps.folder"


async def scan_drive_root() -> Dict:
    root_id = os.getenv("GDRIVE_FOLDER_ID")
    if not root_id:
        raise RuntimeError("GDRIVE_FOLDER_ID não configurado no .env")

    # 1) pega subpastas (OBRA A/B/C/D)
    children = list_files_in_folder(root_id)
    folders = [c for c in children if _is_folder(c)]

    total_files = 0
    per_folder = []

    # 2) para cada pasta, lista arquivos e upserta
    for folder in folders:
        folder_id = folder["id"]
        folder_name = folder.get("name")

        items = list_files_in_folder(folder_id)
        files_only = [x for x in items if not _is_folder(x)]

        for f in files_only:
            f["parent_folder_id"] = folder_id
            f["parent_folder_name"] = folder_name
            await upsert_drive_file(f)

        total_files += len(files_only)
        per_folder.append({"folder": folder_name, "count": len(files_only)})

    return {
        "root_id": root_id,
        "folders_found": len(folders),
        "files_seen": total_files,
        "per_folder": per_folder,
        "ts": datetime.now(timezone.utc).isoformat(),
    }