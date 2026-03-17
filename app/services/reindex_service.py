from __future__ import annotations

from typing import Dict, Any, List

from app.core.mongo import get_db
from app.services.drive_store import list_new_files
from app.ingest.drive_index_pipeline import index_new_drive_files
from app.ingest.qdrant_indexer import get_qdrant, delete_by_file_id


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
    return int(result.modified_count)


async def reindex_all_drive_files(batch_size: int = 25) -> Dict[str, Any]:
    db = get_db()
    qdrant = get_qdrant()

    # Busca todos os arquivos conhecidos
    docs = await db["drive_files"].find(
        {"file_id": {"$exists": True, "$ne": None}}
    ).to_list(length=None)

    deleted = 0
    failed_delete: List[str] = []

    # apaga vetores antigos por file_id
    for doc in docs:
        file_id = doc.get("file_id")
        if not file_id:
            continue
        try:
            delete_by_file_id(qdrant, "drive_rag", file_id)
            deleted += 1
        except Exception:
            failed_delete.append(file_id)

    updated = await mark_all_files_pending()

    total_indexed = 0
    total_errors = 0
    total_points = 0
    total_sheet_tabs = 0
    total_sheet_rows = 0
    total_diffs = 0
    failed_files: List[str] = []

    while True:
        pending = await list_new_files(limit=batch_size)
        if not pending:
            break

        result = await index_new_drive_files(limit=batch_size)

        total_indexed += int(result.get("indexed", 0))
        total_errors += int(result.get("errors", 0))
        total_points += int(result.get("total_points", 0))
        total_sheet_tabs += int(result.get("sheet_tabs", 0))
        total_sheet_rows += int(result.get("sheet_rows", 0))
        total_diffs += int(result.get("diffs_created", 0))
        failed_files.extend(result.get("failed", []))

        # segurança: se não indexou nem errou, evita loop infinito
        if int(result.get("indexed", 0)) == 0 and int(result.get("errors", 0)) == 0:
            break

    return {
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


async def reindex_single_file(file_id: str) -> Dict[str, Any]:
    db = get_db()
    qdrant = get_qdrant()

    doc = await db["drive_files"].find_one({"file_id": file_id})
    if not doc:
        return {
            "ok": False,
            "message": f"Arquivo {file_id} não encontrado.",
        }

    try:
        delete_by_file_id(qdrant, "drive_rag", file_id)
    except Exception:
        pass

    await db["drive_files"].update_one(
        {"file_id": file_id},
        {
            "$set": {
                "indexed": False,
                "index_error": None,
            }
        },
    )

    result = await index_new_drive_files(limit=1)

    return {
        "ok": True,
        "file_id": file_id,
        "result": result,
    }