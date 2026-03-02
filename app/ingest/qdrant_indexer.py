# app/ingest/qdrant_indexer.py
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from qdrant_client import QdrantClient
from qdrant_client.http import models as rest


def get_qdrant() -> QdrantClient:
    url = os.getenv("QDRANT_URL")
    api_key = os.getenv("QDRANT_API_KEY")

    if not url:
        raise RuntimeError("QDRANT_URL não configurado")

    if not api_key:
        api_key = None

    return QdrantClient(url=url, api_key=api_key)


def ensure_collection(client: QdrantClient, collection: str, vector_size: int) -> None:
    try:
        _ = client.get_collection(collection)
        return
    except Exception:
        pass

    client.create_collection(
        collection_name=collection,
        vectors_config=rest.VectorParams(size=vector_size, distance=rest.Distance.COSINE),
    )


def upsert_points(client: QdrantClient, collection: str, points: List[Dict[str, Any]]) -> None:
    if not points:
        return

    client.upsert(
        collection_name=collection,
        points=[
            rest.PointStruct(id=p["id"], vector=p["vector"], payload=p["payload"])
            for p in points
        ],
    )


def delete_by_file_id(client: QdrantClient, collection: str, file_id: str) -> None:
    """
    Remove todos os pontos do arquivo (reindex limpo).
    Depende de payload ter 'file_id'.
    """
    client.delete(
        collection_name=collection,
        points_selector=rest.FilterSelector(
            filter=rest.Filter(
                must=[rest.FieldCondition(key="file_id", match=rest.MatchValue(value=file_id))]
            )
        ),
    )