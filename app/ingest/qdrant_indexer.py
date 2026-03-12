from __future__ import annotations

import os
import uuid
from typing import Any, Dict, List

from qdrant_client import QdrantClient
from qdrant_client.http import models as rest


def get_qdrant() -> QdrantClient:
    url = os.getenv("QDRANT_URL")
    api_key = os.getenv("QDRANT_API_KEY")

    if not url:
        raise RuntimeError("QDRANT_URL não configurado")

    return QdrantClient(url=url, api_key=api_key or None)


def make_point_id(key: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, key))


def _safe_create_payload_index(
    client: QdrantClient,
    collection: str,
    field_name: str,
    field_schema: rest.PayloadSchemaType,
) -> None:
    try:
        client.create_payload_index(
            collection_name=collection,
            field_name=field_name,
            field_schema=field_schema,
        )
        print(f"[qdrant] payload index ok field={field_name}")
    except Exception as e:
        print(f"[qdrant] payload index skip field={field_name} err={type(e).__name__}:{e}")


def ensure_collection(client: QdrantClient, collection: str, vector_size: int) -> None:
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
        print(f"[qdrant] collection created name={collection} size={vector_size}")

    keyword_fields = [
        "file_id",
        "parent_folder_id",
        "parent_folder_name",
        "obra_name",
        "obra_folder_id",
        "mime_type",
        "name",
        "sheet",
        "doc_kind",
    ]

    for field in keyword_fields:
        _safe_create_payload_index(
            client=client,
            collection=collection,
            field_name=field,
            field_schema=rest.PayloadSchemaType.KEYWORD,
        )


def upsert_points(client: QdrantClient, collection: str, points: List[Dict[str, Any]]) -> None:
    if not points:
        print("[qdrant] upsert skipped: no points")
        return

    point_structs = [
        rest.PointStruct(
            id=p["id"],
            vector=p["vector"],
            payload=p["payload"],
        )
        for p in points
    ]

    preview_payload = points[0].get("payload", {}) if points else {}
    print(
        f"[qdrant] upsert start collection={collection} "
        f"points={len(points)} "
        f"file_id={preview_payload.get('file_id')} "
        f"sheet={preview_payload.get('sheet')} "
        f"sheet_sha1={preview_payload.get('sheet_sha1')} "
        f"snapshot_ref={preview_payload.get('snapshot_ref')}"
    )

    op_info = client.upsert(
        collection_name=collection,
        points=point_structs,
        wait=True,
    )

    print(f"[qdrant] upsert done collection={collection} result={op_info}")


def delete_by_file_id(client: QdrantClient, collection: str, file_id: str) -> None:
    print(f"[qdrant] delete start collection={collection} file_id={file_id}")

    op_info = client.delete(
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
        wait=True,
    )

    print(f"[qdrant] delete done collection={collection} file_id={file_id} result={op_info}")