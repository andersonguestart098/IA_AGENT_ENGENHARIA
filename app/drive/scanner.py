from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional

from app.drive.client import list_files_in_folder
from app.services.drive_store import upsert_drive_file
from app.services.drive_state import get_last_sync_at, set_last_sync_at

FOLDER_MIME = "application/vnd.google-apps.folder"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def scan_drive_incremental(root_id: str) -> Dict:
    last_sync = await get_last_sync_at()
    scan_started = _now_iso()

    # BFS por pastas
    queue: List[Tuple[str, str]] = [(root_id, "ROOT")]

    folders_scanned = 0
    items_seen = 0
    per_folder = []

    while queue:
        folder_id, folder_name = queue.pop(0)
        folders_scanned += 1

        # aqui vem TUDO (pastas + arquivos). last_sync filtra por modifiedTime.
        children = list_files_in_folder(folder_id, modified_after=last_sync)

        count_here = 0
        for item in children:
            item["parent_folder_id"] = folder_id
            item["parent_folder_name"] = folder_name

            await upsert_drive_file(item)
            count_here += 1
            items_seen += 1

            if item.get("mimeType") == FOLDER_MIME:
                queue.append((item["id"], item.get("name", "SEM_NOME")))

        per_folder.append({"folder": folder_name, "count": count_here})

    # checkpoint só no fim (se deu tudo certo)
    await set_last_sync_at(scan_started)

    return {
        "root_id": root_id,
        "last_sync_at": last_sync,
        "new_last_sync_at": scan_started,
        "folders_scanned": folders_scanned,
        "items_seen": items_seen,
        "per_folder": per_folder[:50],
    }