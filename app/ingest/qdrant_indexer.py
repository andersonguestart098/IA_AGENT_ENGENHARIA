from __future__ import annotations

import os
import uuid
from typing import Any, Dict, List, Optional

from qdrant_client import QdrantClient
from qdrant_client.http import models as rest


def get_qdrant() -> QdrantClient:
    url = os.getenv("QDRANT_URL")
    api_key = os.getenv("QDRANT_API_KEY")

    if not url:
        raise RuntimeError("QDRANT_URL não configurado")

    return QdrantClient(url=url, api_key=api_key or None)


def make_point_id(key: str) -> str:
    # UUID determinístico (estável entre reindex)
    return str(uuid.uuid5(uuid.NAMESPACE_URL, key))


def ensure_collection(client: QdrantClient, collection: str, vector_size: int) -> None:
    """
    - Cria coleção se não existir
    - Garante payload indexes necessários pro pipeline (delete_by_file_id e buscas futuras)
    """
    exists = True
    try:
        _ = client.get_collection(collection)
    except Exception:
        exists = False

    if not exists:
        client.create_collection(
            collection_name=collection,
            vectors_config=rest.VectorParams(
                size=vector_size,
                distance=rest.Distance.COSINE,
            ),
        )

    # ✅ IMPORTANTÍSSIMO: índices de payload pra filtros
    # file_id e parent_folder_id devem ser "keyword"
    # (se tua Qdrant Cloud for antiga, esse método ainda funciona)
    try:
        client.create_payload_index(
            collection_name=collection,
            field_name="file_id",
            field_schema=rest.PayloadSchemaType.KEYWORD,
        )
    except Exception:
        pass

    try:
        client.create_payload_index(
            collection_name=collection,
            field_name="parent_folder_id",
            field_schema=rest.PayloadSchemaType.KEYWORD,
        )
    except Exception:
        pass

    try:
        client.create_payload_index(
            collection_name=collection,
            field_name="mime_type",
            field_schema=rest.PayloadSchemaType.KEYWORD,
        )
    except Exception:
        pass


def upsert_points(client: QdrantClient, collection: str, points: List[Dict[str, Any]]) -> None:
    if not points:
        return

    client.upsert(
        collection_name=collection,
        points=[
            rest.PointStruct(
                id=p["id"],
                vector=p["vector"],
                payload=p["payload"],
            )
            for p in points
        ],
    )


def delete_by_file_id(client: QdrantClient, collection: str, file_id: str) -> None:
    """
    Remove todos os pontos do arquivo (reindex limpo).
    Depende de payload ter 'file_id' + payload index criado.
    """
    client.delete(
        collection_name=collection,
        points_selector=rest.FilterSelector(
            filter=rest.Filter(
                must=[
                    rest.FieldCondition(
                        key="file_id",
                        match=rest.MatchValue(value=file_id),
                    )
                ]
            )
        ),
    )