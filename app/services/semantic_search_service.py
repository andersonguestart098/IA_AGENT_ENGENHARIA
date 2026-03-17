from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from mistralai import Mistral
from qdrant_client.http import models as rest

from app.ingest.qdrant_indexer import get_qdrant

QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "drive_rag")
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")
MISTRAL_EMBED_MODEL = os.getenv("MISTRAL_EMBED_MODEL", "mistral-embed")

_mistral_client: Optional[Mistral] = None


def _get_mistral() -> Mistral:
    global _mistral_client
    if _mistral_client is not None:
        return _mistral_client

    if not MISTRAL_API_KEY:
        raise RuntimeError("MISTRAL_API_KEY não configurado")

    _mistral_client = Mistral(api_key=MISTRAL_API_KEY)
    return _mistral_client


def embed_query(text: str) -> List[float]:
    client = _get_mistral()
    resp = client.embeddings.create(
        model=MISTRAL_EMBED_MODEL,
        inputs=[text],
    )
    return resp.data[0].embedding


def _must_match(key: str, value: Optional[str]):
    if not value:
        return None
    return rest.FieldCondition(
        key=key,
        match=rest.MatchValue(value=value),
    )


def build_qdrant_filter(scope: Dict[str, Optional[str]]) -> Optional[rest.Filter]:
    must = []

    obra = _must_match("obra_name", scope.get("obra"))
    folder = _must_match("parent_folder_name", scope.get("folder"))
    file_name = _must_match("name", scope.get("file_name"))

    if obra:
        must.append(obra)
    if folder:
        must.append(folder)
    if file_name:
        must.append(file_name)

    if not must:
        return None

    return rest.Filter(must=must)


def semantic_search(
    question: str,
    scope: Dict[str, Optional[str]],
    limit: int = 8,
    score_threshold: float = 0.55,
) -> List[Dict[str, Any]]:
    client = get_qdrant()
    vector = embed_query(question)
    qfilter = build_qdrant_filter(scope)

    hits = client.search(
        collection_name=QDRANT_COLLECTION,
        query_vector=vector,
        query_filter=qfilter,
        limit=limit,
        score_threshold=score_threshold,
        with_payload=True,
    )

    results: List[Dict[str, Any]] = []
    for h in hits:
        payload = h.payload or {}
        results.append(
            {
                "score": h.score,
                "file_id": payload.get("file_id"),
                "file_name": payload.get("name"),
                "sheet": payload.get("sheet"),
                "obra_name": payload.get("obra_name"),
                "folder_name": payload.get("parent_folder_name"),
                "row_start": payload.get("row_start"),
                "row_end": payload.get("row_end"),
                "text": payload.get("text"),
                "payload": payload,
            }
        )

    return results


def build_context_from_hits(hits: List[Dict[str, Any]], max_chars: int = 12000) -> str:
    parts: List[str] = []
    total = 0

    for i, hit in enumerate(hits, start=1):
        block = (
            f"[TRECHO {i} | score={hit['score']:.4f} | obra={hit.get('obra_name')} | "
            f"arquivo={hit.get('file_name')} | aba={hit.get('sheet')} | "
            f"linhas={hit.get('row_start')}..{hit.get('row_end')}]\n"
            f"{hit.get('text', '')}\n"
        )

        if total + len(block) > max_chars:
            break

        parts.append(block)
        total += len(block)

    return "\n".join(parts).strip()