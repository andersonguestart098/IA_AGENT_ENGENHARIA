from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Optional, List

from app.core.mongo import get_db
from app.core.config import GDRIVE_FOLDER_ID
from app.drive.client import list_files_in_folder

from app.services.drive_store import upsert_drive_file


STATE_COLLECTION = "drive_state"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _get_last_scan_iso(root_folder_id: str) -> Optional[str]:
    db = get_db()
    col = db[STATE_COLLECTION]
    doc = await col.find_one({"_id": f"root:{root_folder_id}"}, {"last_scan_at": 1})
    return doc.get("last_scan_at") if doc else None


async def _set_last_scan_iso(root_folder_id: str, iso: str) -> None:
    db = get_db()
    col = db[STATE_COLLECTION]
    await col.update_one(
        {"_id": f"root:{root_folder_id}"},
        {"$set": {"last_scan_at": iso, "updated_at": _utc_now_iso()}},
        upsert=True,
    )


async def scan_drive_incremental(root_folder_id: Optional[str] = None) -> Dict:
    """
    Faz scan incremental:
    - lista subpastas da raiz (OBRA A/B/C/D)
    - para cada subpasta, lista arquivos modificados depois de last_scan_at
    - upsert no Mongo (drive_files)
    - atualiza last_scan_at no final
    """
    root_id = root_folder_id or GDRIVE_FOLDER_ID
    if not root_id:
        raise RuntimeError("GDRIVE_FOLDER_ID não configurado no .env")

    last_scan_at = await _get_last_scan_iso(root_id)

    # 1) lista subpastas da raiz (sempre, pois pastas podem ter sido criadas)
    folders = list_files_in_folder(root_id, only_folders=True)

    total_files_seen = 0
    folders_found = len(folders)
    per_folder: List[Dict] = []

    for folder in folders:
        folder_id = folder["id"]
        folder_name = folder.get("name", "")

        # 2) lista arquivos dentro da pasta, mas só os modificados depois do último scan
        files = list_files_in_folder(
            folder_id,
            modified_after=last_scan_at,
            only_folders=False,
        )

        # 3) upsert no Mongo com metadados de parent
        for f in files:
            f["parent_folder_id"] = folder_id
            f["parent_folder_name"] = folder_name
            await upsert_drive_file(f)

        count = len(files)
        total_files_seen += count
        per_folder.append({"folder": folder_name, "count": count})

    # 4) Atualiza cursor de scan (agora)
    now_iso = _utc_now_iso()
    await _set_last_scan_iso(root_id, now_iso)

    return {
        "root_id": root_id,
        "last_scan_at_before": last_scan_at,
        "last_scan_at_after": now_iso,
        "folders_found": folders_found,
        "files_seen": total_files_seen,
        "per_folder": per_folder,
    }