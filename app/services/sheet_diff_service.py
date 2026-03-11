from __future__ import annotations

import json
import hashlib
from typing import Dict, Any, List, Tuple, Optional

from app.core.mongo import get_db


def _stable_row_hash(row: Dict[str, Any]) -> str:
    clean = {}
    for k, v in row.items():
        if str(k).startswith("__formula__") or str(k).startswith("__missing__"):
            continue
        clean[k] = v

    raw = json.dumps(clean, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _row_preview(row: Dict[str, Any]) -> str:
    pairs = []
    for k, v in row.items():
        if str(k).startswith("__"):
            continue
        if v is None or str(v).strip() == "":
            continue
        pairs.append(f"{k}={v}")
    return " | ".join(pairs[:6])


def diff_rows(previous_rows: List[Dict[str, Any]], current_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    prev_map = {_stable_row_hash(r): r for r in previous_rows}
    curr_map = {_stable_row_hash(r): r for r in current_rows}

    prev_keys = set(prev_map.keys())
    curr_keys = set(curr_map.keys())

    added_keys = curr_keys - prev_keys
    removed_keys = prev_keys - curr_keys
    common_keys = prev_keys & curr_keys

    added_rows = [curr_map[k] for k in added_keys]
    removed_rows = [prev_map[k] for k in removed_keys]
    unchanged_rows = [curr_map[k] for k in common_keys]

    return {
        "added_count": len(added_rows),
        "removed_count": len(removed_rows),
        "unchanged_count": len(unchanged_rows),
        "added_rows": added_rows[:20],
        "removed_rows": removed_rows[:20],
    }


async def get_previous_snapshot(file_id: str, sheet: str, current_sheet_sha1: str) -> Optional[Dict[str, Any]]:
    db = get_db()
    col = db["drive_sheet_snapshots"]

    doc = await col.find_one(
        {
            "file_id": file_id,
            "sheet": sheet,
            "sheet_sha1": {"$ne": current_sheet_sha1},
        },
        sort=[("updated_at", -1)]
    )
    return doc


def build_diff_summary(file_name: str, sheet: str, diff: Dict[str, Any]) -> str:
    parts = [
        f"Arquivo {file_name}, aba {sheet}.",
        f"Linhas adicionadas: {diff['added_count']}.",
        f"Linhas removidas: {diff['removed_count']}.",
    ]

    if diff["added_rows"]:
        previews = [_row_preview(r) for r in diff["added_rows"][:3]]
        parts.append("Novos lançamentos: " + " ; ".join(previews) + ".")

    if diff["removed_rows"]:
        previews = [_row_preview(r) for r in diff["removed_rows"][:3]]
        parts.append("Lançamentos removidos: " + " ; ".join(previews) + ".")

    return " ".join(parts)