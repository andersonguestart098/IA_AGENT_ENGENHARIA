from __future__ import annotations

import os
import json
import time
import hashlib
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

from mistralai import Mistral

from app.services.drive_store import list_new_files, mark_indexed, mark_error
from app.ingest.qdrant_indexer import (
    get_qdrant,
    ensure_collection,
    upsert_points,
    delete_by_file_id,
    make_point_id,
)
from app.drive.changes import DriveChangesClient
from app.services.sheet_snapshot_store import upsert_sheet_snapshot
from app.services.sheet_diff_service import (
    get_previous_snapshot,
    diff_rows,
    build_diff_summary,
)
from app.services.sheet_diff_store import insert_sheet_diff
from app.ingest.xlsx_extractor import extract_xlsx_structured
from app.services.drive_hierarchy_service import resolve_obra_context

QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "drive_rag")

# Mistral embeddings
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")
MISTRAL_EMBED_MODEL = os.getenv("MISTRAL_EMBED_MODEL", "mistral-embed")
MISTRAL_EMBED_BATCH = int(os.getenv("MISTRAL_EMBED_BATCH", "96"))
EMBED_TEXT_MAX_CHARS = int(os.getenv("EMBED_TEXT_MAX_CHARS", "12000"))

# Chunk planilha
SHEET_ROWS_PER_CHUNK = int(os.getenv("SHEET_ROWS_PER_CHUNK", "20"))
SHEET_MAX_COLS = int(os.getenv("SHEET_MAX_COLS", "80"))

# Segurança / limites
MAX_ROWS_PER_SHEET = int(os.getenv("MAX_ROWS_PER_SHEET", "5000"))

XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
GOOGLE_FOLDER = "application/vnd.google-apps.folder"

_mistral_client: Optional[Mistral] = None


def _utc_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


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
    client = _get_mistral()
    out: List[List[float]] = []

    clean = [_truncate_for_embedding(t or "") for t in texts]

    for i in range(0, len(clean), MISTRAL_EMBED_BATCH):
        batch = clean[i:i + MISTRAL_EMBED_BATCH]
        t0 = time.time()
        resp = client.embeddings.create(model=MISTRAL_EMBED_MODEL, inputs=batch)
        out.extend([d.embedding for d in resp.data])
        dt = time.time() - t0
        print(f"[ingest][embed] batch={i // MISTRAL_EMBED_BATCH} size={len(batch)} dt={dt:.2f}s")

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


def _normalize_key(text: str) -> str:
    return (text or "").strip().lower().replace(" ", "_")


def _detect_document_type(header: List[str], sheet_name: str, file_name: str) -> str:
    joined = " ".join([(sheet_name or ""), (file_name or ""), *header]).lower()

    rules = [
        (["vlr_custo", "desc_custo", "custo", "despesa", "gasto", "valor"], "planilha de custos de obra"),
        (["fornecedor", "prestador", "parceiro"], "planilha de fornecedores ou parceiros"),
        (["material", "insumo", "produto"], "planilha de materiais ou insumos"),
        (["quantidade", "qtd", "unidade", "saldo"], "planilha quantitativa ou de estoque"),
        (["data", "historico", "histórico", "observacao", "observação"], "planilha de lançamentos e histórico"),
        (["medicao", "medição", "avance", "avanço", "etapa"], "planilha de medição ou avanço de obra"),
    ]

    for keywords, label in rules:
        if any(k in joined for k in keywords):
            return label

    return "planilha de obra"


def _semantic_keywords(header: List[str], sheet_name: str, file_name: str) -> List[str]:
    joined = " ".join([(sheet_name or ""), (file_name or ""), *header]).lower()

    keywords = {
        "obra",
        "engenharia",
        "planilha",
        "arquivo",
        "aba",
        "dados",
        "lançamento",
        "lançamentos",
        "valor",
    }

    mapping = {
        "custo": ["custos", "despesas", "gastos", "financeiro", "orçamento"],
        "vlr_custo": ["custos", "despesas", "gastos", "financeiro", "orçamento"],
        "desc_custo": ["descrição", "tipo de custo", "lançamento", "despesa"],
        "fornecedor": ["fornecedor", "prestador", "empresa", "terceiro"],
        "material": ["material", "insumo", "produto", "item"],
        "quantidade": ["quantidade", "qtd", "saldo"],
        "qtd": ["quantidade", "qtd", "saldo"],
        "data": ["data", "período", "lançamento"],
        "categoria": ["categoria", "grupo", "classificação"],
        "grupo": ["categoria", "grupo", "classificação"],
        "servico": ["serviço", "mão de obra", "execução"],
        "serviço": ["serviço", "mão de obra", "execução"],
        "medicao": ["medição", "avanço", "progresso"],
        "medição": ["medição", "avanço", "progresso"],
    }

    for col in header:
        norm = _normalize_key(col)
        for trigger, adds in mapping.items():
            if trigger in norm:
                keywords.update(adds)

    if "eletrica" in joined or "elétrica" in joined:
        keywords.update(["elétrica", "instalação elétrica"])
    if "hidraulica" in joined or "hidráulica" in joined:
        keywords.update(["hidráulica", "instalação hidráulica"])
    if "ferragem" in joined:
        keywords.update(["ferragem", "aço", "armadura"])
    if "concreto" in joined:
        keywords.update(["concreto", "cimento", "estrutura"])
    if "aluguel" in joined or "locacao" in joined or "locação" in joined:
        keywords.update(["aluguel", "locação", "equipamento"])

    return sorted(k for k in keywords if k)


def _friendly_field_name(col: str) -> str:
    c = _normalize_key(col)

    mapping = {
        "data": "data",
        "desc_custo": "descrição do custo",
        "descricao": "descrição",
        "descrição": "descrição",
        "vlr_custo": "valor do custo",
        "valor": "valor",
        "fornecedor": "fornecedor",
        "categoria": "categoria",
        "grupo": "grupo",
        "qtd": "quantidade",
        "quantidade": "quantidade",
        "material": "material",
        "servico": "serviço",
        "serviço": "serviço",
        "historico": "histórico",
        "histórico": "histórico",
        "observacao": "observação",
        "observação": "observação",
    }

    return mapping.get(c, col.strip())


def _row_to_natural_sentence(header: List[str], row: Dict[str, Any]) -> str:
    parts: List[str] = []

    for col in header:
        val = _fmt(row.get(col))
        if val == "":
            continue
        parts.append(f"{_friendly_field_name(col)} {val}")

    if not parts:
        return ""

    return "; ".join(parts)


def _infer_sheet_summary(header: List[str], rows: List[Dict[str, Any]], doc_type: str) -> str:
    normalized = [_normalize_key(h) for h in header]

    if "desc_custo" in normalized or "vlr_custo" in normalized:
        return "Esta aba contém linhas de planilha relacionadas a custos, despesas ou lançamentos financeiros da obra."

    if "fornecedor" in normalized:
        return "Esta aba contém informações relacionadas a fornecedores, parceiros ou prestadores envolvidos na obra."

    if "material" in normalized or "qtd" in normalized or "quantidade" in normalized:
        return "Esta aba contém itens, materiais, quantidades ou informações operacionais associadas à obra."

    if "historico" in normalized or "histórico" in normalized:
        return "Esta aba contém histórico, anotações ou registros cronológicos da obra."

    return f"Esta aba contém dados de {doc_type}."


def _build_chunk_text(
    *,
    obra_name: Optional[str],
    parent_folder_name: Optional[str],
    file_name: Optional[str],
    sheet_name: str,
    header: List[str],
    rows: List[Dict[str, Any]],
    row_start: int,
    row_end: int,
) -> str:
    doc_type = _detect_document_type(header, sheet_name, file_name or "")
    keywords = _semantic_keywords(header, sheet_name, file_name or "")
    summary = _infer_sheet_summary(header, rows, doc_type)

    lines: List[str] = []
    lines.append("DOCUMENTO PLANILHA DE OBRA")
    lines.append(f"OBRA: {obra_name or 'não informada'}")
    lines.append(f"PASTA: {parent_folder_name or 'não informada'}")
    lines.append(f"ARQUIVO: {file_name or 'não informado'}")
    lines.append(f"ABA: {sheet_name}")
    lines.append(f"TIPO_DOCUMENTO: {doc_type}")
    lines.append(f"INTERVALO_LINHAS: {row_start + 1} até {row_end + 1}")

    if keywords:
        lines.append("PALAVRAS_CHAVE: " + ", ".join(keywords))

    lines.append("")
    lines.append("COLUNAS DISPONÍVEIS:")
    lines.append(" | ".join([h for h in header if h]) or "(sem colunas)")
    lines.append("")
    lines.append("RESUMO SEMÂNTICO:")
    lines.append(summary)
    lines.append("")
    lines.append("LINHAS DA PLANILHA:")

    natural_count = 0
    for idx, row in enumerate(rows, start=row_start + 1):
        sentence = _row_to_natural_sentence(header, row)
        if sentence:
            lines.append(f"Linha {idx}: {sentence}")
            natural_count += 1

    if natural_count == 0:
        lines.append("Nenhuma linha textual relevante encontrada neste bloco.")

    lines.append("")
    lines.append("FORMATO ESTRUTURADO:")

    for idx, row in enumerate(rows, start=row_start + 1):
        parts = []

        for col in header:
            val = _fmt(row.get(col))
            if val != "":
                parts.append(f"{col}={val}")

        for col in header:
            f = row.get(f"__formula__{col}")
            miss = row.get(f"__missing__{col}")
            if f and miss:
                parts.append(f"{col}_FORMULA={f}")

        if parts:
            lines.append(f"L{idx}: " + " ; ".join(parts))

    return "\n".join(lines).strip()


def _sheet_rows_to_chunks_text(
    *,
    obra_name: Optional[str],
    parent_folder_name: Optional[str],
    file_name: Optional[str],
    sheet_name: str,
    header: List[str],
    rows: List[Dict[str, Any]],
    rows_per_chunk: int = SHEET_ROWS_PER_CHUNK,
    overlap_rows: int = 5,
) -> List[Tuple[int, int, str]]:
    """
    chunks: (row_start, row_end, text)
    row_start/end são índices 0-based em relação a rows (sem header).
    overlap_rows cria uma pequena sobreposição entre blocos.
    """
    chunks: List[Tuple[int, int, str]] = []
    if not rows:
        return chunks

    header = (header or [])[:SHEET_MAX_COLS]

    step = max(1, rows_per_chunk - overlap_rows)
    i = 0

    while i < len(rows):
        row_start = i
        row_end = min(i + rows_per_chunk, len(rows)) - 1
        block = rows[row_start:row_end + 1]

        text = _build_chunk_text(
            obra_name=obra_name,
            parent_folder_name=parent_folder_name,
            file_name=file_name,
            sheet_name=sheet_name,
            header=header,
            rows=block,
            row_start=row_start,
            row_end=row_end,
        )

        if text:
            chunks.append((row_start, row_end, text))

        if row_end >= len(rows) - 1:
            break

        i += step

    return chunks

    header = (header or [])[:SHEET_MAX_COLS]

    for i in range(0, len(rows), rows_per_chunk):
        block = rows[i:i + rows_per_chunk]
        row_start = i
        row_end = min(i + rows_per_chunk, len(rows)) - 1

        text = _build_chunk_text(
            obra_name=obra_name,
            parent_folder_name=parent_folder_name,
            file_name=file_name,
            sheet_name=sheet_name,
            header=header,
            rows=block,
            row_start=row_start,
            row_end=row_end,
        )

        if text:
            chunks.append((row_start, row_end, text))

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
    totals = {
        "files": len(docs),
        "points": 0,
        "sheet_tabs": 0,
        "sheet_rows": 0,
        "diffs": 0,
    }

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

        if mime_type == GOOGLE_FOLDER:
            await mark_indexed(file_id)
            indexed += 1
            print(f"[ingest][file] skip folder -> indexed file_id={file_id}")
            continue

        if mime_type != XLSX_MIME:
            await mark_error(file_id, f"unsupported mime for xlsx-only pipeline: {mime_type}")
            errors += 1
            failed.append(file_id)
            print(f"[ingest][file] unsupported mime -> error file_id={file_id} mime={mime_type}")
            continue

        try:
            try:
                delete_by_file_id(qdrant, QDRANT_COLLECTION, file_id)
                print(f"[ingest][qdrant] deleted existing points file_id={file_id}")
            except Exception as e:
                print(f"[ingest][qdrant] delete skipped file_id={file_id} err={type(e).__name__}:{e}")

            t0 = time.time()
            data = drive.download_file_bytes(file_id=file_id)
            dt = time.time() - t0
            print(
                f"[ingest][download] ok file_id={file_id} "
                f"bytes={len(data)} sha256={_sha256_bytes(data)[:12]} dt={dt:.2f}s"
            )

            t0 = time.time()
            extracted = extract_xlsx_structured(
                data,
                max_cols=SHEET_MAX_COLS,
                max_rows_per_sheet=MAX_ROWS_PER_SHEET,
            )
            dt = time.time() - t0

            file_sha256 = extracted.get("file_sha256")
            sheets = extracted.get("sheets") or []

            print(
                f"[ingest][extract] ok file_id={file_id} "
                f"sheets={len(sheets)} file_sha256={str(file_sha256)[:12]} dt={dt:.2f}s"
            )

            if not sheets:
                await mark_error(file_id, "xlsx has no readable sheets/rows")
                errors += 1
                failed.append(file_id)
                print(f"[ingest][file] no sheets -> error file_id={file_id}")
                continue

            obra_ctx = await resolve_obra_context(
                parent_folder_id=doc.get("parent_folder_id"),
                parent_folder_name=doc.get("parent_folder_name"),
            )

            payload_base = _build_payload_base(doc)
            payload_base["file_sha256"] = file_sha256
            payload_base["obra_name"] = obra_ctx.get("obra_name")
            payload_base["obra_folder_id"] = obra_ctx.get("obra_folder_id")

            print(
                f"[ingest][obra] file_id={file_id} "
                f"obra_name={obra_ctx.get('obra_name')} "
                f"obra_folder_id={obra_ctx.get('obra_folder_id')}"
            )

            all_points: List[Dict[str, Any]] = []

            for s in sheets:
                sheet_name = s["sheet"]
                header = s.get("header") or []
                rows = s.get("rows") or []
                schema = s.get("schema")
                sheet_sha1 = s.get("sheet_sha1")

                totals["sheet_tabs"] += 1
                totals["sheet_rows"] += len(rows)

                print(
                    f"[ingest][sheet] name={sheet_name} rows={len(rows)} "
                    f"sha1={str(sheet_sha1)[:10]}"
                )

                previous_snapshot = await get_previous_snapshot(
                    file_id=file_id,
                    sheet=sheet_name,
                    current_sheet_sha1=sheet_sha1,
                )

                if previous_snapshot:
                    print(
                        f"[ingest][sheet] previous_snapshot_sha1="
                        f"{str(previous_snapshot.get('sheet_sha1'))[:10]}"
                    )
                else:
                    print(f"[ingest][sheet] previous_snapshot_sha1=None")

                snapshot_doc = {
                    "file_id": file_id,
                    "file_name": name,
                    "modified_time": doc.get("modified_time"),
                    "parent_folder_id": doc.get("parent_folder_id"),
                    "parent_folder_name": doc.get("parent_folder_name"),
                    "obra_name": obra_ctx.get("obra_name"),
                    "obra_folder_id": obra_ctx.get("obra_folder_id"),
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
                print(
                    f"[ingest][mongo] snapshot upserted "
                    f"file_id={file_id} file_name={name} sheet={sheet_name} rows={len(rows)}"
                )

                if previous_snapshot:
                    diff = diff_rows(
                        previous_rows=previous_snapshot.get("rows") or [],
                        current_rows=rows,
                    )

                    if diff["added_count"] > 0 or diff["removed_count"] > 0:
                        summary = build_diff_summary(
                            file_name=name,
                            sheet=sheet_name,
                            diff=diff,
                        )

                        await insert_sheet_diff(
                            {
                                "file_id": file_id,
                                "file_name": name,
                                "sheet": sheet_name,
                                "previous_sheet_sha1": previous_snapshot.get("sheet_sha1"),
                                "current_sheet_sha1": sheet_sha1,
                                "added_count": diff["added_count"],
                                "removed_count": diff["removed_count"],
                                "added_rows": diff["added_rows"],
                                "removed_rows": diff["removed_rows"],
                                "summary": summary,
                                "modified_time": doc.get("modified_time"),
                                "parent_folder_name": doc.get("parent_folder_name"),
                                "parent_folder_id": doc.get("parent_folder_id"),
                                "obra_name": obra_ctx.get("obra_name"),
                                "obra_folder_id": obra_ctx.get("obra_folder_id"),
                            }
                        )

                        totals["diffs"] += 1

                        print(
                            f"[ingest][diff] file_id={file_id} sheet={sheet_name} "
                            f"added={diff['added_count']} removed={diff['removed_count']}"
                        )
                        print(f"[ingest][diff][summary] {summary}")
                    else:
                        print(f"[ingest][diff] no row-level changes file_id={file_id} sheet={sheet_name}")
                else:
                    print(f"[ingest][diff] no previous snapshot file_id={file_id} sheet={sheet_name}")

                chunks = _sheet_rows_to_chunks_text(
                    obra_name=obra_ctx.get("obra_name"),
                    parent_folder_name=doc.get("parent_folder_name"),
                    file_name=name,
                    sheet_name=sheet_name,
                    header=header,
                    rows=rows,
                    rows_per_chunk=SHEET_ROWS_PER_CHUNK,
                )

                if not chunks:
                    print(f"[ingest][sheet] no chunks produced sheet={sheet_name}")
                    continue

                print(
                    f"[ingest][sheet] chunks_produced={len(chunks)} "
                    f"sheet={sheet_name} sheet_sha1={sheet_sha1}"
                )

                texts = [t for (_rs, _re, t) in chunks]
                vectors = _embed_texts(texts)

                if not vectors:
                    print(f"[ingest][embed] no vectors returned sheet={sheet_name}")
                    continue

                if len(vectors) != len(chunks):
                    raise RuntimeError(
                        f"Quantidade de vetores diferente da quantidade de chunks: "
                        f"chunks={len(chunks)} vectors={len(vectors)}"
                    )

                if not collection_ready:
                    vector_size = len(vectors[0])
                    ensure_collection(qdrant, QDRANT_COLLECTION, vector_size)
                    collection_ready = True
                    print(f"[ingest][qdrant] ensure_collection ok name={QDRANT_COLLECTION} size={vector_size}")

                doc_type = _detect_document_type(header, sheet_name, name or "")
                semantic_keywords = _semantic_keywords(header, sheet_name, name or "")

                for idx, (chunk, vec) in enumerate(zip(chunks, vectors)):
                    row_start, row_end, text = chunk
                    snapshot_ref = f"{file_id}:{sheet_name}:{sheet_sha1}"
                    point_key = f"{file_id}:{sheet_name}:{sheet_sha1}:{idx}"
                    chunk_sha1 = _sha1(text)

                    print(
                        f"[ingest][point] file_id={file_id} sheet={sheet_name} "
                        f"chunk_index={idx} row_start={row_start} row_end={row_end} "
                        f"sheet_sha1={sheet_sha1} chunk_sha1={chunk_sha1[:10]} "
                        f"snapshot_ref={snapshot_ref}"
                    )
                    print(f"[ingest][point][preview] {text[:700]}")

                    payload = dict(payload_base)
                    payload.update(
                        {
                            "doc_kind": "xlsx_sheet",
                            "sheet": sheet_name,
                            "sheet_sha1": sheet_sha1,
                            "row_start": row_start,
                            "row_end": row_end,
                            "chunk_index": idx,
                            "chunk_sha1": chunk_sha1,
                            "snapshot_ref": snapshot_ref,
                            "text": text,
                            "text_preview": text[:400],
                            "doc_type": doc_type,
                            "semantic_keywords": semantic_keywords,
                            "header_columns": header[:SHEET_MAX_COLS],
                        }
                    )

                    all_points.append(
                        {
                            "id": make_point_id(point_key),
                            "vector": vec,
                            "payload": {
                                **payload,
                                "point_key": point_key,
                            },
                        }
                    )

            if not all_points:
                await mark_error(file_id, "no points generated from xlsx (no chunks/rows?)")
                errors += 1
                failed.append(file_id)
                print(f"[ingest][file] no points -> error file_id={file_id}")
                continue

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
        "diffs_created": totals["diffs"],
        "failed": failed[:20],
    }