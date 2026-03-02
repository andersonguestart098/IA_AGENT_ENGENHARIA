# app/ingest/qdrant_indexer.py
from __future__ import annotations

import os
import uuid
from typing import Any, Dict, List, Optional

from qdrant_client import QdrantClient
from qdrant_client.http import models as rest


# UUID fixo para gerar IDs determinísticos (não muda entre deploys)
_QDRANT_POINT_NAMESPACE = uuid.UUID("f2d4c6c0-9d8d-4f22-9d0b-3b7b7cfb4c11")


def get_qdrant() -> QdrantClient:
    url = os.getenv("QDRANT_URL")
    api_key = os.getenv("QDRANT_API_KEY")

    if not url:
        raise RuntimeError("QDRANT_URL não configurado")

    if not api_key:
        api_key = None

    return QdrantClient(url=url, api_key=api_key)


def ensure_collection(client: QdrantClient, collection: str, vector_size: int) -> None:
    """
    Cria coleção se não existir. Se existir, não mexe.
    """
    try:
        client.get_collection(collection)
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


def make_point_id(key: str) -> str:
    """
    Qdrant Point ID compatível (UUID), determinístico.
    - 'key' pode ser qualquer string (ex: file_id:sheet:sha1:idx)
    - retorna UUID string
    """
    if not key:
        # fallback extremo
        return str(uuid.uuid4())
    return str(uuid.uuid5(_QDRANT_POINT_NAMESPACE, key))


def _coerce_point_id(raw_id: Any) -> Any:
    """
    Aceita:
    - int (ok)
    - UUID string (ok)
    - qualquer string -> converte para UUID determinístico
    """
    if isinstance(raw_id, int):
        return raw_id

    if isinstance(raw_id, uuid.UUID):
        return str(raw_id)

    if isinstance(raw_id, str):
        s = raw_id.strip()
        # se já é UUID válido, mantém
        try:
            _ = uuid.UUID(s)
            return s
        except Exception:
            return make_point_id(s)

    # fallback: transforma em string e gera uuid5
    return make_point_id(str(raw_id))


def upsert_points(client: QdrantClient, collection: str, points: List[Dict[str, Any]]) -> None:
    """
    points: [{id, vector, payload}, ...]
    - id pode ser string livre; aqui vira UUID determinístico
    """
    if not points:
        return

    structs: List[rest.PointStruct] = []
    for p in points:
        pid = _coerce_point_id(p.get("id"))
        payload = p.get("payload") or {}
        vector = p.get("vector")

        # guarda o id "humano" no payload para debug/rastreio
        if "point_key" not in payload:
            payload["point_key"] = str(p.get("id"))

        structs.append(rest.PointStruct(id=pid, vector=vector, payload=payload))

    client.upsert(
        collection_name=collection,
        points=structs,
    )


def delete_by_file_id(client: QdrantClient, collection: str, file_id: str) -> None:
    """
    Remove todos os pontos do arquivo (reindex limpo).
    Depende do payload ter 'file_id'.

    Importante: não quebra o job se a collection ainda não existir.
    """
    try:
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
    except Exception as e:
        # comum no primeiro run (collection não existe ainda)
        print(f"[ingest][qdrant] delete skipped file_id={file_id} err={type(e).__name__}:{e}")