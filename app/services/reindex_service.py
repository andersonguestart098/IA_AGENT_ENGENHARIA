from __future__ import annotations

import os
from typing import Dict, Any, List

from app.core.mongo import get_db
from app.ingest.drive_index_pipeline import index_new_drive_files
from app.ingest.qdrant_indexer import get_qdrant, delete_by_file_id

QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "drive_rag")


def _log(msg: str) -> None:
    print(msg, flush=True)


async def mark_all_files_pending() -> int:
    db = get_db()

    result = await db["drive_files"].update_many(
        {},
        {
            "$set": {
                "indexed": False,
                "index_error": None,
            }
        },
    )

    modified = int(result.modified_count)
    _log(f"[reindex][mark_pending] modified={modified}")
    return modified


async def reindex_all_drive_files(batch_size: int = 25) -> Dict[str, Any]:
    db = get_db()
    qdrant = get_qdrant()

    _log(f"[reindex][all] start batch_size={batch_size} collection={QDRANT_COLLECTION}")

    docs = await db["drive_files"].find(
        {"file_id": {"$exists": True, "$ne": None}}
    ).to_list(length=None)

    total_docs = len(docs)
    _log(f"[reindex][all] total_docs={total_docs}")

    deleted = 0
    failed_delete: List[str] = []

    _log("[reindex][all] phase=delete_vectors start")

    for idx, doc in enumerate(docs, start=1):
        file_id = doc.get("file_id")
        if not file_id:
            continue

        try:
            delete_by_file_id(qdrant, QDRANT_COLLECTION, file_id)
            deleted += 1

            if idx % 25 == 0 or idx == total_docs:
                _log(
                    f"[reindex][all] phase=delete_vectors progress="
                    f"{idx}/{total_docs} deleted={deleted} failed={len(failed_delete)}"
                )

        except Exception as e:
            failed_delete.append(file_id)
            _log(
                f"[reindex][all] phase=delete_vectors error "
                f"file_id={file_id} err={type(e).__name__}:{e}"
            )

    _log(
        f"[reindex][all] phase=delete_vectors done "
        f"deleted={deleted} failed={len(failed_delete)}"
    )

    _log("[reindex][all] phase=mark_pending start")
    updated = await mark_all_files_pending()
    _log(f"[reindex][all] phase=mark_pending done updated={updated}")

    total_indexed = 0
    total_errors = 0
    total_points = 0
    total_sheet_tabs = 0
    total_sheet_rows = 0
    total_diffs = 0
    failed_files: List[str] = []

    batch_num = 0

    _log("[reindex][all] phase=rebuild_index start")

    while True:
        batch_num += 1
        _log(f"[reindex][all] batch={batch_num} start limit={batch_size}")

        result = await index_new_drive_files(limit=batch_size)

        indexed = int(result.get("indexed", 0))
        errors = int(result.get("errors", 0))
        points = int(result.get("total_points", 0))
        sheet_tabs = int(result.get("sheet_tabs", 0))
        sheet_rows = int(result.get("sheet_rows", 0))
        diffs = int(result.get("diffs_created", 0))
        failed_batch = result.get("failed", []) or []

        total_indexed += indexed
        total_errors += errors
        total_points += points
        total_sheet_tabs += sheet_tabs
        total_sheet_rows += sheet_rows
        total_diffs += diffs
        failed_files.extend(failed_batch)

        _log(
            f"[reindex][all] batch={batch_num} done "
            f"indexed={indexed} errors={errors} "
            f"points={points} sheet_tabs={sheet_tabs} "
            f"sheet_rows={sheet_rows} diffs={diffs} "
            f"failed_in_batch={len(failed_batch)}"
        )

        if failed_batch:
            _log(
                f"[reindex][all] batch={batch_num} failed_sample="
                f"{failed_batch[:10]}"
            )

        # condição de parada correta:
        # se o pipeline não indexou nada e não teve erro, acabou a fila
        if indexed == 0 and errors == 0:
            _log(f"[reindex][all] batch={batch_num} stop_condition=empty_cycle")
            break

    summary = {
        "ok": True,
        "marked_pending": updated,
        "deleted_vectors": deleted,
        "failed_vector_deletes": failed_delete[:50],
        "indexed": total_indexed,
        "errors": total_errors,
        "total_points": total_points,
        "sheet_tabs": total_sheet_tabs,
        "sheet_rows": total_sheet_rows,
        "diffs_created": total_diffs,
        "failed_files": failed_files[:100],
    }

    _log(
        f"[reindex][all] done "
        f"marked_pending={summary['marked_pending']} "
        f"deleted_vectors={summary['deleted_vectors']} "
        f"indexed={summary['indexed']} "
        f"errors={summary['errors']} "
        f"total_points={summary['total_points']} "
        f"sheet_tabs={summary['sheet_tabs']} "
        f"sheet_rows={summary['sheet_rows']} "
        f"diffs_created={summary['diffs_created']} "
        f"failed_files={len(failed_files)}"
    )

    return summary


async def reindex_single_file(file_id: str) -> Dict[str, Any]:
    db = get_db()
    qdrant = get_qdrant()

    _log(f"[reindex][file] start file_id={file_id} collection={QDRANT_COLLECTION}")

    doc = await db["drive_files"].find_one({"file_id": file_id})
    if not doc:
        _log(f"[reindex][file] not_found file_id={file_id}")
        return {
            "ok": False,
            "message": f"Arquivo {file_id} não encontrado.",
        }

    try:
        delete_by_file_id(qdrant, QDRANT_COLLECTION, file_id)
        _log(f"[reindex][file] delete_vectors done file_id={file_id}")
    except Exception as e:
        _log(
            f"[reindex][file] delete_vectors warn "
            f"file_id={file_id} err={type(e).__name__}:{e}"
        )

    update_result = await db["drive_files"].update_one(
        {"file_id": file_id},
        {
            "$set": {
                "indexed": False,
                "index_error": None,
            }
        },
    )

    _log(
        f"[reindex][file] mark_pending "
        f"file_id={file_id} matched={update_result.matched_count} "
        f"modified={update_result.modified_count}"
    )

    result = await index_new_drive_files(limit=1)

    _log(
        f"[reindex][file] done "
        f"file_id={file_id} indexed={result.get('indexed', 0)} "
        f"errors={result.get('errors', 0)} "
        f"points={result.get('total_points', 0)}"
    )

    return {
        "ok": True,
        "file_id": file_id,
        "result": result,
    }