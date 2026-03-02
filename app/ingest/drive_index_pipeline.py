# app/ingest/drive_index_pipeline.py
from __future__ import annotations

import os
import io
import json
import time
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

from mistralai import Mistral

from app.services.drive_store import list_new_files, mark_indexed, mark_error
from app.ingest.qdrant_indexer import get_qdrant, ensure_collection, upsert_points, delete_by_file_id
from app.drive.changes import DriveChangesClient
from app.services.sheet_snapshot_store import upsert_sheet_snapshot
from app.ingest.xlsx_extractor import extract_xlsx_structured

QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "drive_rag")

# Mistral embeddings (leve pro Heroku)
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")
MISTRAL_EMBED_MODEL = os.getenv("MISTRAL_EMBED_MODEL", "mistral-embed")
MISTRAL_EMBED_BATCH = int(os.getenv("MISTRAL_EMBED_BATCH", "96"))
EMBED_TEXT_MAX_CHARS = int(os.getenv("EMBED_TEXT_MAX_CHARS", "12000"))

# Chunk planilha (linhas)
SHEET_ROWS_PER_CHUNK = int(os.getenv("SHEET_ROWS_PER_CHUNK", "60"))
SHEET_MAX_COLS = int(os.getenv("SHEET_MAX_COLS", "80"))

# Segurança/limites
MAX_ROWS_PER_SHEET = int(os.getenv("MAX_ROWS_PER_SHEET", "5000"))  # evita planilha gigante travar

XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
GOOGLE_FOLDER = "application/vnd.google-apps.folder"

_mistral_client: Optional[Mistral] = None


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _get_mistral() -> Mistral:
    global _mistral_client
    if _mistral_client is not None:
        return _mistral_client
    if not MISTRAL_API_KEY:
        raise RuntimeError("MISTRAL_API_KEY não configurado")
    _mistral_client = Mistral(api_key=MISTRAL_API_KEY)
    return _mistral_client


def _truncate_for_embedding(text: str) -> str:
    if not text:
        return text
    if len(text) <= EMBED_TEXT_MAX_CHARS:
        return text
    return text[:EMBED_TEXT_MAX_CHARS]


def _embed_texts(texts: List[str]) -> List[List[float]]:
    """
    Embedding via Mistral.
    Logs por batch pra debugar latência/rate-limit.
    """
    client = _get_mistral()
    out: List[List[float]] = []

    clean = [_truncate_for_embedding(t or "") for t in texts]

    for i in range(0, len(clean), MISTRAL_EMBED_BATCH):
        batch = clean[i : i + MISTRAL_EMBED_BATCH]
        t0 = time.time()
        resp = client.embeddings.create(model=MISTRAL_EMBED_MODEL, inputs=batch)
        out.extend([d.embedding for d in resp.data])
        dt = time.time() - t0
        print(f"[ingest][embed] batch={i//MISTRAL_EMBED_BATCH} size={len(batch)} dt={dt:.2f}s")

    return out


def _load_sa_info_from_env() -> Dict[str, Any]:
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON não configurado")
    return json.loads(raw)


def _get_drive_client_from_env() -> DriveChangesClient:
    sa_info = _load_sa_info_from_env()
    return DriveChangesClient(service_account_info=sa_info)


def _build_payload_base(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "source": "gdrive",
        "file_id": doc.get("file_id"),
        "name": doc.get("name"),
        "parent_folder_id": doc.get("parent_folder_id"),
        "parent_folder_name": doc.get("parent_folder_name"),
        "mime_type": doc.get("mime_type"),
        "modified_time": doc.get("modified_time"),
        "size": doc.get("size"),
    }


def _fmt(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        s = f"{v:.10f}".rstrip("0").rstrip(".")
        return s
    if isinstance(v, datetime):
        return v.isoformat()
    return str(v).strip()


def _sheet_rows_to_chunks_text(
    *,
    sheet_name: str,
    header: List[str],
    rows: List[Dict[str, Any]],
    rows_per_chunk: int = SHEET_ROWS_PER_CHUNK,
) -> List[Tuple[int, int, str]]:
    """
    chunks: (row_start, row_end, text)
    row_start/end são índices 0-based em relação a rows (sem header).
    """
    chunks: List[Tuple[int, int, str]] = []
    if not rows:
        return chunks

    header = (header or [])[:SHEET_MAX_COLS]

    for i in range(0, len(rows), rows_per_chunk):
        block = rows[i : i + rows_per_chunk]
        lines: List[str] = []

        lines.append(f"## Sheet: {sheet_name}")
        lines.append("HEADER: " + " | ".join([h for h in header if h]))
        lines.append("ROWS:")

        for j, r in enumerate(block):
            parts = []
            for col in header:
                val = _fmt(r.get(col))
                if val != "":
                    parts.append(f"{col}={val}")

            # rastreio opcional de fórmulas (se teu extractor preencher)
            for col in header:
                f = r.get(f"__formula__{col}")
                miss = r.get(f"__missing__{col}")
                if f and miss:
                    parts.append(f"{col}_FORMULA={f}")

            if parts:
                lines.append(f"L{i + j + 1}: " + " ; ".join(parts))

        text = "\n".join(lines).strip()
        if text:
            chunks.append((i, min(i + rows_per_chunk, len(rows)) - 1, text))

    return chunks


async def index_new_drive_files(limit: int = 25) -> Dict[str, Any]:
    docs = await list_new_files(limit=limit)
    if not docs:
        return {"ok": True, "indexed": 0, "skipped": "no_new"}

    drive = _get_drive_client_from_env()
    qdrant = get_qdrant()

    indexed = 0
    errors = 0
    failed: List[str] = []
    totals = {"files": len(docs), "points": 0, "sheet_tabs": 0, "sheet_rows": 0}

    collection_ready = False

    print(f"[ingest] start limit={limit} docs={len(docs)} collection={QDRANT_COLLECTION}")

    for doc in docs:
        file_id = doc.get("file_id")
        mime_type = doc.get("mime_type")
        name = doc.get("name")

        if not file_id or not mime_type:
            await mark_error(file_id or "?", "missing file_id or mime_type")
            errors += 1
            failed.append(file_id or "?")
            continue

        print(f"[ingest][file] start file_id={file_id} name={name} mime={mime_type}")

        # ignora pasta
        if mime_type == GOOGLE_FOLDER:
            await mark_indexed(file_id)
            indexed += 1
            print(f"[ingest][file] skip folder -> indexed file_id={file_id}")
            continue

        # pipeline focada em XLSX
        if mime_type != XLSX_MIME:
            await mark_error(file_id, f"unsupported mime for xlsx-only pipeline: {mime_type}")
            errors += 1
            failed.append(file_id)
            print(f"[ingest][file] unsupported mime -> error file_id={file_id} mime={mime_type}")
            continue

        try:
            # delete seguro (pode falhar se coleção não existe ainda)
            try:
                delete_by_file_id(qdrant, QDRANT_COLLECTION, file_id)
                print(f"[ingest][qdrant] deleted existing points file_id={file_id}")
            except Exception as e:
                print(f"[ingest][qdrant] delete skipped file_id={file_id} err={type(e).__name__}:{e}")

            # download do arquivo (bytes)
            t0 = time.time()
            data = drive.download_file_bytes(file_id=file_id)
            dt = time.time() - t0
            print(f"[ingest][download] ok file_id={file_id} bytes={len(data)} sha256={_sha256_bytes(data)[:12]} dt={dt:.2f}s")

            # extrai estruturado
            t0 = time.time()
            extracted = extract_xlsx_structured(
                data,
                max_cols=SHEET_MAX_COLS,
                max_rows_per_sheet=MAX_ROWS_PER_SHEET,
            )
            dt = time.time() - t0

            file_sha256 = extracted.get("file_sha256")
            sheets = extracted.get("sheets") or []
            print(f"[ingest][extract] ok file_id={file_id} sheets={len(sheets)} file_sha256={str(file_sha256)[:12]} dt={dt:.2f}s")

            if not sheets:
                await mark_error(file_id, "xlsx has no readable sheets/rows")
                errors += 1
                failed.append(file_id)
                print(f"[ingest][file] no sheets -> error file_id={file_id}")
                continue

            payload_base = _build_payload_base(doc)
            payload_base["file_sha256"] = file_sha256

            all_points: List[Dict[str, Any]] = []

            for s in sheets:
                sheet_name = s["sheet"]
                header = s.get("header") or []
                rows = s.get("rows") or []
                schema = s.get("schema")
                sheet_sha1 = s.get("sheet_sha1")

                totals["sheet_tabs"] += 1
                totals["sheet_rows"] += len(rows)

                print(f"[ingest][sheet] name={sheet_name} rows={len(rows)} sha1={str(sheet_sha1)[:10]}")

                # snapshot determinístico no Mongo
                snapshot_doc = {
                    "file_id": file_id,
                    "file_name": name,
                    "modified_time": doc.get("modified_time"),
                    "parent_folder_id": doc.get("parent_folder_id"),
                    "parent_folder_name": doc.get("parent_folder_name"),
                    "mime_type": mime_type,
                    "file_sha256": file_sha256,
                    "sheet": sheet_name,
                    "sheet_sha1": sheet_sha1,
                    "header": header,
                    "rows": rows,
                    "row_count": len(rows),
                    "schema": schema,
                    "indexed_at": _utc_iso(),
                }
                await upsert_sheet_snapshot(snapshot_doc)
                print(f"[ingest][mongo] snapshot upserted file_id={file_id} sheet={sheet_name}")

                # chunks semânticos por bloco de linhas
                chunks = _sheet_rows_to_chunks_text(
                    sheet_name=sheet_name,
                    header=header,
                    rows=rows,
                    rows_per_chunk=SHEET_ROWS_PER_CHUNK,
                )
                if not chunks:
                    print(f"[ingest][sheet] no chunks produced sheet={sheet_name}")
                    continue

                texts = [t for (_rs, _re, t) in chunks]

                # embedding
                vectors = _embed_texts(texts)

                # cria coleção na primeira vez que tiver vetor
                if not collection_ready:
                    vector_size = len(vectors[0])
                    ensure_collection(qdrant, QDRANT_COLLECTION, vector_size)
                    collection_ready = True
                    print(f"[ingest][qdrant] ensure_collection ok name={QDRANT_COLLECTION} size={vector_size}")

                # points
                for idx, (chunk, vec) in enumerate(zip(chunks, vectors)):
                    row_start, row_end, text = chunk

                    payload = dict(payload_base)
                    payload.update(
                        {
                            "doc_kind": "xlsx_sheet",
                            "sheet": sheet_name,
                            "sheet_sha1": sheet_sha1,
                            "row_start": row_start,
                            "row_end": row_end,
                            "chunk_index": idx,
                            "chunk_sha1": _sha1(text),
                            "snapshot_ref": f"{file_id}:{sheet_name}:{sheet_sha1}",
                            "text": text,
                        }
                    )

                    all_points.append(
                        {
                            "id": f"{file_id}:{sheet_name}:{sheet_sha1}:{idx}",
                            "vector": vec,
                            "payload": payload,
                        }
                    )

            if not all_points:
                await mark_error(file_id, "no points generated from xlsx (no chunks/rows?)")
                errors += 1
                failed.append(file_id)
                print(f"[ingest][file] no points -> error file_id={file_id}")
                continue

            # upsert batch
            t0 = time.time()
            upsert_points(qdrant, QDRANT_COLLECTION, all_points)
            dt = time.time() - t0
            totals["points"] += len(all_points)
            print(f"[ingest][qdrant] upsert ok file_id={file_id} points={len(all_points)} dt={dt:.2f}s")

            await mark_indexed(file_id)
            indexed += 1
            print(f"[ingest][file] indexed ok file_id={file_id}")

        except Exception as e:
            await mark_error(file_id, f"{type(e).__name__}: {e}")
            errors += 1
            failed.append(file_id)
            print(f"[ingest][file] ERROR file_id={file_id} err={type(e).__name__}:{e}")

    return {
        "ok": True,
        "indexed": indexed,
        "errors": errors,
        "total_files": totals["files"],
        "total_points": totals["points"],
        "sheet_tabs": totals["sheet_tabs"],
        "sheet_rows": totals["sheet_rows"],
        "failed": failed[:20],
    }