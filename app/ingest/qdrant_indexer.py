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
        # pode ser vazio em Qdrant local, mas no cloud geralmente precisa
        api_key = None

    return QdrantClient(url=url, api_key=api_key)


def ensure_collection(client: QdrantClient, collection: str, vector_size: int) -> None:
    """
    Cria coleção se não existir. Se existir, não mexe.
    """
    try:
        _ = client.get_collection(collection)
        return
    except Exception:
        pass

    client.create_collection(
        collection_name=collection,
        vectors_config=rest.VectorParams(
            size=vector_size,
            distance=rest.Distance.COSINE,
        ),
    )


def upsert_point(
    client: QdrantClient,
    collection: str,
    *,
    point_id: str,
    vector: List[float],
    payload: Dict[str, Any],
) -> None:
    client.upsert(
        collection_name=collection,
        points=[
            rest.PointStruct(
                id=point_id,
                vector=vector,
                payload=payload,
            )
        ],
    )