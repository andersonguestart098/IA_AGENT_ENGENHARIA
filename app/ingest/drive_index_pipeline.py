# app/ingest/drive_index_pipeline.py
from __future__ import annotations

from typing import Dict, Any, List, Optional

from app.services.drive_store import list_new_files, mark_indexed, mark_error
from app.ingest.qdrant_indexer import get_qdrant, ensure_collection, upsert_point

QDRANT_COLLECTION = "drive_rag"
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"  # leve e bom p/ começar

_model = None  # cache global do modelo


def _build_text(doc: Dict[str, Any]) -> str:
    # MVP: indexa metadados (já dá pra testar fluxo).
    parts = [
        f"file_name: {doc.get('name')}",
        f"parent_folder: {doc.get('parent_folder_name')}",
        f"mime_type: {doc.get('mime_type')}",
        f"modified_time: {doc.get('modified_time')}",
    ]
    return "\n".join([p for p in parts if p])


def _get_model():
    global _model
    if _model is not None:
        return _model

    try:
        from sentence_transformers import SentenceTransformer
    except Exception as e:
        raise RuntimeError(
            "Faltou instalar sentence-transformers. "
            "Adiciona no requirements.txt: sentence-transformers==2.7.0"
        ) from e

    _model = SentenceTransformer(EMBED_MODEL)
    return _model


def _embed_texts(texts: List[str]) -> List[List[float]]:
    model = _get_model()
    vectors = model.encode(texts, normalize_embeddings=True).tolist()
    return vectors


async def index_new_drive_files(limit: int = 25) -> Dict[str, Any]:
    docs = await list_new_files(limit=limit)
    if not docs:
        return {"ok": True, "indexed": 0, "skipped": "no_new"}

    texts = [_build_text(d) for d in docs]
    vectors = _embed_texts(texts)

    client = get_qdrant()
    vector_size = len(vectors[0])
    ensure_collection(client, QDRANT_COLLECTION, vector_size)

    indexed = 0
    errors = 0
    failed: List[str] = []

    for doc, vec in zip(docs, vectors):
        file_id = doc["file_id"]
        try:
            payload = {
                "file_id": file_id,
                "name": doc.get("name"),
                "parent_folder_id": doc.get("parent_folder_id"),
                "parent_folder_name": doc.get("parent_folder_name"),
                "mime_type": doc.get("mime_type"),
                "modified_time": doc.get("modified_time"),
            }

            upsert_point(
                client,
                QDRANT_COLLECTION,
                point_id=file_id,  # id do Qdrant = file_id do Drive
                vector=vec,
                payload=payload,
            )

            await mark_indexed(file_id)
            indexed += 1

        except Exception as e:
            await mark_error(file_id, str(e))
            errors += 1
            failed.append(file_id)

    return {
        "ok": True,
        "indexed": indexed,
        "errors": errors,
        "total": len(docs),
        "failed": failed[:20],
    }