from __future__ import annotations

import os
import re
import time
import unicodedata
from typing import Dict, Any, Optional, List, Tuple, Set, cast

from mistralai import Mistral
from qdrant_client.http import models as rest

from app.core.mongo import get_db
from app.ingest.qdrant_indexer import get_qdrant

MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")
MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-large-latest")
MISTRAL_EMBED_MODEL = os.getenv("MISTRAL_EMBED_MODEL", "mistral-embed")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "drive_rag")

SEMANTIC_TOP_K = int(os.getenv("SEMANTIC_TOP_K", "12"))
SEMANTIC_FINAL_TOP_K = int(os.getenv("SEMANTIC_FINAL_TOP_K", "8"))
SEMANTIC_SCORE_THRESHOLD = float(os.getenv("SEMANTIC_SCORE_THRESHOLD", "0.40"))
SEMANTIC_CONTEXT_MAX_CHARS = int(os.getenv("SEMANTIC_CONTEXT_MAX_CHARS", "14000"))

_client: Optional[Mistral] = None


def _log(msg: str) -> None:
    print(msg, flush=True)


def _get_mistral() -> Mistral:
    global _client

    if _client is not None:
        return _client

    if not MISTRAL_API_KEY:
        raise RuntimeError("MISTRAL_API_KEY não configurado")

    _client = Mistral(api_key=MISTRAL_API_KEY)
    return _client


def _normalize_for_match(text: str) -> str:
    text = (text or "").lower().strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text


def _build_diff_filter(scope: Dict[str, Optional[str]]) -> Dict[str, Any]:
    flt: Dict[str, Any] = {}

    if scope.get("obra"):
        flt["obra_name"] = scope["obra"]

    if scope.get("folder"):
        flt["parent_folder_name"] = scope["folder"]

    if scope.get("file_name"):
        flt["file_name"] = scope["file_name"]

    return flt


def _build_snapshot_filter(scope: Dict[str, Optional[str]]) -> Dict[str, Any]:
    flt: Dict[str, Any] = {}

    if scope.get("obra"):
        flt["obra_name"] = scope["obra"]

    if scope.get("folder"):
        flt["parent_folder_name"] = scope["folder"]

    if scope.get("file_name"):
        flt["file_name"] = scope["file_name"]

    return flt


async def get_latest_snapshot(scope: Dict[str, Optional[str]]) -> Optional[Dict[str, Any]]:
    db = get_db()
    flt = _build_snapshot_filter(scope)

    return await db["drive_sheet_snapshots"].find_one(
        flt,
        sort=[("indexed_at", -1), ("modified_time", -1), ("created_at", -1)],
    )


async def get_last_diff(scope: Dict[str, Optional[str]]) -> Optional[Dict[str, Any]]:
    db = get_db()
    flt = _build_diff_filter(scope)

    return await db["drive_sheet_diffs"].find_one(
        flt,
        sort=[("created_at", -1), ("modified_time", -1)],
    )


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip()
    if not s:
        return None

    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


async def calculate_total(snapshot: Dict[str, Any]) -> float:
    rows = snapshot.get("rows", [])
    total = 0.0

    for r in rows:
        v = _to_float(r.get("VLR_CUSTO"))
        if v is not None:
            total += v

    return total


def _format_money(v: float) -> str:
    return f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _pick_latest_added_row(diff: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    added = diff.get("added_rows") or []
    if not added:
        return None
    return added[-1]


def _build_qdrant_filter(scope: Dict[str, Optional[str]]) -> Optional[rest.Filter]:
    must: List[rest.FieldCondition] = []

    if scope.get("obra"):
        must.append(
            rest.FieldCondition(
                key="obra_name",
                match=rest.MatchValue(value=scope["obra"]),
            )
        )

    if scope.get("folder"):
        must.append(
            rest.FieldCondition(
                key="parent_folder_name",
                match=rest.MatchValue(value=scope["folder"]),
            )
        )

    if scope.get("file_name"):
        must.append(
            rest.FieldCondition(
                key="name",
                match=rest.MatchValue(value=scope["file_name"]),
            )
        )

    if not must:
        return None

    return rest.Filter(must=must)


def embed_query(text: str) -> List[float]:
    client = _get_mistral()
    resp = client.embeddings.create(
        model=MISTRAL_EMBED_MODEL,
        inputs=[text],
    )
    return resp.data[0].embedding


def _tokenize_for_rerank(text: str) -> List[str]:
    normalized = _normalize_for_match(text)
    raw = re.findall(r"[a-z0-9_]+", normalized)

    stopwords = {
        "a", "o", "e", "de", "da", "do", "das", "dos", "em", "na", "no", "nas", "nos",
        "qual", "quais", "quanto", "quanta", "quero", "me", "mostra", "mostrar", "liste",
        "listar", "tem", "teve", "com", "para", "por", "um", "uma", "os", "as", "que",
        "foi", "sao", "ate", "agora", "obra", "planilha", "arquivo", "dados",
    }

    return [t for t in raw if len(t) >= 3 and t not in stopwords]


def _infer_folder_from_question(question: str) -> Optional[str]:
    q = _normalize_for_match(question)

    custos_terms = [
        "custo", "custos", "gasto", "gastos", "despesa", "despesas",
        "locacao", "aluguel", "diaria",
        "material", "insumo", "servico", "fornecedor",
        "compra", "compras", "maior custo", "total de custos",
    ]
    faturamento_terms = [
        "faturamento", "parcela", "parcelas", "pagamento",
        "prev pgto", "previsao de pagamento",
        "forma pgto", "forma de pagamento", "recebimento",
        "recebimentos", "pago", "pagou", "pagamentos",
    ]

    if any(term in q for term in custos_terms):
        return "custos"

    if any(term in q for term in faturamento_terms):
        return "faturamento"

    return None


def _keyword_overlap_score(question: str, hit: Dict[str, Any]) -> float:
    q_tokens = set(_tokenize_for_rerank(question))
    if not q_tokens:
        return 0.0

    text_real = _normalize_for_match(str(hit.get("text", "") or ""))
    semantic_keywords = _normalize_for_match(
        " ".join([str(x) for x in (hit.get("semantic_keywords") or [])])
    )
    doc_type = _normalize_for_match(str(hit.get("doc_type", "") or ""))
    metadata = _normalize_for_match(" ".join([
        str(hit.get("file_name", "") or ""),
        str(hit.get("sheet", "") or ""),
        str(hit.get("obra_name", "") or ""),
        str(hit.get("folder_name", "") or ""),
    ]))

    strong_terms = {
        "locacao", "aluguel", "diaria",
        "concreto", "cimento", "ferragem", "aco",
        "eletrica", "hidraulica",
        "mao", "servico", "fornecedor",
        "parcela", "pagamento", "recebimento",
    }

    content_matches = 0
    semantic_matches = 0
    metadata_matches = 0
    strong_matches = 0

    for tok in q_tokens:
        in_text = tok in text_real
        in_semantic = tok in semantic_keywords or tok in doc_type
        in_metadata = tok in metadata

        if in_text:
            content_matches += 1
        elif in_semantic:
            semantic_matches += 1
        elif in_metadata:
            metadata_matches += 1

        if tok in strong_terms and in_text:
            strong_matches += 1

    content_bonus = min(0.24, content_matches * 0.05)
    semantic_bonus = min(0.10, semantic_matches * 0.025)
    metadata_bonus = min(0.04, metadata_matches * 0.01)
    strong_bonus = min(0.20, strong_matches * 0.08)

    return content_bonus + semantic_bonus + metadata_bonus + strong_bonus


def _dedupe_hits(hits: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: Set[Tuple[Any, Any, Any, Any, Any]] = set()
    out: List[Dict[str, Any]] = []

    for hit in hits:
        key = (
            hit.get("file_name"),
            hit.get("sheet"),
            hit.get("row_start"),
            hit.get("row_end"),
            hit.get("chunk_index"),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(hit)

    return out


def _rerank_hits(
    question: str,
    scope: Dict[str, Optional[str]],
    hits: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    ranked: List[Dict[str, Any]] = []

    scope_obra = _normalize_for_match(scope.get("obra") or "")
    scope_folder = _normalize_for_match(scope.get("folder") or "")
    inferred_folder = _infer_folder_from_question(question)

    for hit in hits:
        vector_score = float(hit.get("score", 0.0) or 0.0)
        keyword_bonus = _keyword_overlap_score(question, hit)

        obra_bonus = 0.0
        folder_bonus = 0.0

        hit_obra = _normalize_for_match(str(hit.get("obra_name") or ""))
        hit_folder = _normalize_for_match(str(hit.get("folder_name") or ""))

        if scope_obra:
            if hit_obra == scope_obra:
                obra_bonus = 0.20
            else:
                obra_bonus = -0.08

        if scope_folder:
            if hit_folder == scope_folder:
                folder_bonus += 0.08
            else:
                folder_bonus += -0.03
        elif inferred_folder:
            if hit_folder == inferred_folder:
                folder_bonus += 0.10
            else:
                folder_bonus += -0.04

        final_score = vector_score + keyword_bonus + obra_bonus + folder_bonus

        ranked.append(
            {
                **hit,
                "vector_score": vector_score,
                "keyword_bonus": keyword_bonus,
                "obra_bonus": obra_bonus,
                "folder_bonus": folder_bonus,
                "inferred_folder": inferred_folder,
                "final_score": final_score,
            }
        )

    ranked.sort(key=lambda x: x["final_score"], reverse=True)
    return ranked


def _search_qdrant_once(
    *,
    query_vector: List[float],
    q_filter: Optional[rest.Filter],
    limit: int,
    score_threshold: float,
) -> List[Dict[str, Any]]:
    qdrant = get_qdrant()

    try:
        result = qdrant.query_points(
            collection_name=QDRANT_COLLECTION,
            query=query_vector,
            query_filter=q_filter,
            limit=limit,
            with_payload=True,
            score_threshold=score_threshold,
        )
        points = getattr(result, "points", None) or []
    except Exception:
        result = qdrant.search(
            collection_name=QDRANT_COLLECTION,
            query_vector=query_vector,
            query_filter=q_filter,
            limit=limit,
            with_payload=True,
            score_threshold=score_threshold,
        )
        points = result or []

    hits: List[Dict[str, Any]] = []
    for p in points:
        payload = getattr(p, "payload", None) or {}
        text = str(payload.get("text", "")).strip()
        if not text:
            continue

        hits.append(
            {
                "score": float(getattr(p, "score", 0.0) or 0.0),
                "text": text,
                "file_id": payload.get("file_id"),
                "file_name": payload.get("name"),
                "sheet": payload.get("sheet"),
                "obra_name": payload.get("obra_name"),
                "folder_name": payload.get("parent_folder_name"),
                "row_start": payload.get("row_start"),
                "row_end": payload.get("row_end"),
                "chunk_index": payload.get("chunk_index"),
                "doc_type": payload.get("doc_type"),
                "semantic_keywords": payload.get("semantic_keywords") or [],
                "text_preview": payload.get("text_preview"),
            }
        )

    return hits


def search_qdrant(question: str, scope: Dict[str, Optional[str]]) -> List[Dict[str, Any]]:
    query_vector = embed_query(question)

    complete_scope = {
        "obra": scope.get("obra"),
        "folder": scope.get("folder"),
        "file_name": scope.get("file_name"),
    }
    only_obra_scope = {
        "obra": scope.get("obra"),
        "folder": None,
        "file_name": None,
    }
    only_folder_scope = {
        "obra": None,
        "folder": scope.get("folder"),
        "file_name": None,
    }

    filters_to_try: List[Tuple[str, Optional[rest.Filter]]] = [
        ("complete_scope", _build_qdrant_filter(complete_scope)),
        ("only_obra", _build_qdrant_filter(only_obra_scope)),
        ("only_folder", _build_qdrant_filter(only_folder_scope)),
        ("global", None),
    ]

    merged_hits: List[Dict[str, Any]] = []

    _log(
        f"[semantic][search] question={question!r} "
        f"obra={scope.get('obra')} folder={scope.get('folder')} file_name={scope.get('file_name')}"
    )

    for label, q_filter in filters_to_try:
        try:
            hits = _search_qdrant_once(
                query_vector=query_vector,
                q_filter=q_filter,
                limit=SEMANTIC_TOP_K,
                score_threshold=SEMANTIC_SCORE_THRESHOLD,
            )

            _log(
                f"[semantic][search] stage={label} hits={len(hits)} "
                f"threshold={SEMANTIC_SCORE_THRESHOLD}"
            )

            merged_hits.extend(hits)

            if len(merged_hits) >= SEMANTIC_TOP_K:
                break

        except Exception as e:
            _log(f"[semantic][search][error] stage={label} err={type(e).__name__}:{e}")

    merged_hits = _dedupe_hits(merged_hits)
    ranked_hits = _rerank_hits(question, scope, merged_hits)
    final_hits = ranked_hits[:SEMANTIC_FINAL_TOP_K]

    for idx, hit in enumerate(final_hits[:5], start=1):
        _log(
            f"[semantic][top_hit] rank={idx} "
            f"final={hit['final_score']:.4f} "
            f"vector={hit['vector_score']:.4f} "
            f"keyword_bonus={hit['keyword_bonus']:.4f} "
            f"obra_bonus={hit.get('obra_bonus', 0.0):.4f} "
            f"folder_bonus={hit.get('folder_bonus', 0.0):.4f} "
            f"inferred_folder={hit.get('inferred_folder')} "
            f"obra={hit.get('obra_name')} "
            f"folder={hit.get('folder_name')} "
            f"arquivo={hit.get('file_name')} "
            f"aba={hit.get('sheet')} "
            f"linhas={hit.get('row_start')}..{hit.get('row_end')} "
            f"tipo={hit.get('doc_type')}"
        )

    return final_hits


def build_search_debug(question: str, scope: Dict[str, Optional[str]], hits: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "question": question,
        "scope": scope,
        "threshold": SEMANTIC_SCORE_THRESHOLD,
        "top_k": SEMANTIC_TOP_K,
        "final_top_k": SEMANTIC_FINAL_TOP_K,
        "hits": [
            {
                "rank": i + 1,
                "final_score": h.get("final_score"),
                "vector_score": h.get("vector_score"),
                "keyword_bonus": h.get("keyword_bonus"),
                "obra_bonus": h.get("obra_bonus"),
                "folder_bonus": h.get("folder_bonus"),
                "inferred_folder": h.get("inferred_folder"),
                "obra_name": h.get("obra_name"),
                "folder_name": h.get("folder_name"),
                "file_name": h.get("file_name"),
                "sheet": h.get("sheet"),
                "row_start": h.get("row_start"),
                "row_end": h.get("row_end"),
                "doc_type": h.get("doc_type"),
                "semantic_keywords": h.get("semantic_keywords"),
                "text_preview": (h.get("text_preview") or h.get("text") or "")[:900],
            }
            for i, h in enumerate(hits)
        ],
    }


def _build_context_text(hits: List[Dict[str, Any]], max_chars: int = SEMANTIC_CONTEXT_MAX_CHARS) -> str:
    blocks: List[str] = []
    total_chars = 0

    for i, hit in enumerate(hits, start=1):
        block = (
            f"[TRECHO {i}] "
            f"final_score={hit.get('final_score', hit.get('score', 0.0)):.4f} | "
            f"vector_score={hit.get('vector_score', hit.get('score', 0.0)):.4f} | "
            f"obra={hit.get('obra_name')} | "
            f"pasta={hit.get('folder_name')} | "
            f"arquivo={hit.get('file_name')} | "
            f"aba={hit.get('sheet')} | "
            f"linhas={hit.get('row_start')}..{hit.get('row_end')} | "
            f"tipo={hit.get('doc_type')}\n"
            f"{hit.get('text', '')}\n"
        )

        if total_chars + len(block) > max_chars:
            break

        blocks.append(block)
        total_chars += len(block)

    return "\n".join(blocks).strip()


def _is_retryable_llm_error(exc: Exception) -> bool:
    msg = f"{type(exc).__name__}: {exc}".lower()

    retry_terms = [
        "503",
        "429",
        "timeout",
        "timed out",
        "connection",
        "service unavailable",
        "unreachable_backend",
        "rate limit",
        "temporarily unavailable",
    ]

    return any(term in msg for term in retry_terms)


def ask_llm(question: str, context: str) -> str:
    prompt = f"""
Você é um analista de dados da empresa.

Pergunta do usuário:
{question}

Contexto recuperado:
{context}

Regras:
- Responda de forma objetiva e executiva.
- Responda SOMENTE com base no contexto.
- Não invente dados.
- Se houver incerteza, diga claramente.
- Se a pergunta pedir análise, resuma o que os dados mostram.
- Se houver números, preserve os valores corretamente.
- Se o contexto não for suficiente para responder integralmente, diga isso.
""".strip()

    messages = cast(Any, [
        {"role": "system", "content": "Você é um analista de dados da empresa. Responda apenas com base no contexto."},
        {"role": "user", "content": prompt},
    ])

    client = _get_mistral()
    delays = [0.8, 1.5, 3.0]
    last_error: Optional[Exception] = None

    for attempt, delay in enumerate(delays, start=1):
        try:
            _log(f"[semantic][llm] attempt={attempt} model={MISTRAL_MODEL}")

            resp = client.chat.complete(
                model=MISTRAL_MODEL,
                messages=messages,
                temperature=0.1,
            )

            content = str(resp.choices[0].message.content or "").strip()
            _log(f"[semantic][llm] success attempt={attempt}")
            return content

        except Exception as e:
            last_error = e
            retryable = _is_retryable_llm_error(e)

            _log(
                f"[semantic][llm_error] attempt={attempt} "
                f"retryable={retryable} err={type(e).__name__}:{e}"
            )

            if not retryable or attempt == len(delays):
                break

            time.sleep(delay)

    raise RuntimeError(f"LLM unavailable after retries: {type(last_error).__name__}: {last_error}")


async def handle_structured_total(
    question: str,
    scope: Dict[str, Optional[str]],
    plan: Dict[str, Any],
) -> Dict[str, Any]:
    snapshot = await get_latest_snapshot(scope)

    if not snapshot:
        return {
            "answer": (
                f"Ainda não encontrei dados de {scope.get('folder', 'dados').lower()} "
                f"para a {scope.get('obra') or 'obra informada'}. "
                f"Pode ser que essa obra ainda não tenha planilha indexada ou sincronizada."
            ),
            "status": "no_data_for_scope",
        }

    total = await calculate_total(snapshot)

    return {
        "answer": f"O total atual de custos da {scope.get('obra') or 'obra'} é R$ {_format_money(total)}.",
        "status": "ok",
        "data": {
            "total": total,
            "formatted_total": _format_money(total),
            "source": "snapshot",
        },
    }


async def handle_structured_diff(
    question: str,
    scope: Dict[str, Optional[str]],
    plan: Dict[str, Any],
) -> Dict[str, Any]:
    diff = await get_last_diff(scope)

    if not diff:
        return {
            "answer": (
                f"Ainda não encontrei alterações registradas para a {scope.get('obra') or 'obra informada'} "
                f"nesse contexto."
            ),
            "status": "no_diff_found",
        }

    summary = diff.get("summary") or "Alteração identificada."

    return {
        "answer": summary,
        "status": "ok",
        "data": {
            "added_count": diff.get("added_count", 0),
            "removed_count": diff.get("removed_count", 0),
            "source": "diff",
        },
    }


async def handle_structured_last(
    question: str,
    scope: Dict[str, Optional[str]],
    plan: Dict[str, Any],
) -> Dict[str, Any]:
    diff = await get_last_diff(scope)

    if diff:
        row = _pick_latest_added_row(diff)
        if row:
            desc = row.get("DESC_CUSTO", "sem descrição")
            val = row.get("VLR_CUSTO", "sem valor")
            data = row.get("DATA", "sem data")

            return {
                "answer": f"O último lançamento identificado foi {desc}, no valor de {val}, na data {data}.",
                "status": "ok",
                "data": {
                    "descricao": desc,
                    "valor": val,
                    "data": data,
                    "source": "diff_added_row",
                },
            }

    snapshot = await get_latest_snapshot(scope)
    if not snapshot:
        return {
            "answer": (
                f"Ainda não encontrei dados suficientes para identificar o último lançamento "
                f"da {scope.get('obra') or 'obra informada'}."
            ),
            "status": "no_data_for_scope",
        }

    rows = snapshot.get("rows") or []
    if not rows:
        return {
            "answer": "Não encontrei linhas suficientes para identificar o último lançamento.",
            "status": "empty_snapshot",
        }

    row = rows[-1]
    desc = row.get("DESC_CUSTO", "sem descrição")
    val = row.get("VLR_CUSTO", "sem valor")
    data = row.get("DATA", "sem data")

    return {
        "answer": f"O último lançamento disponível foi {desc}, no valor de {val}, na data {data}.",
        "status": "ok",
        "data": {
            "descricao": desc,
            "valor": val,
            "data": data,
            "source": "snapshot_last_row",
        },
    }


async def handle_structured_list_costs(
    question: str,
    scope: Dict[str, Optional[str]],
    plan: Dict[str, Any],
) -> Dict[str, Any]:
    snapshot = await get_latest_snapshot(scope)

    if not snapshot:
        return {
            "answer": (
                f"Ainda não encontrei dados de custos para a {scope.get('obra') or 'obra informada'}."
            ),
            "status": "no_data_for_scope",
        }

    rows = snapshot.get("rows") or []
    if not rows:
        return {
            "answer": "A planilha foi encontrada, mas não há linhas de custos disponíveis.",
            "status": "empty_snapshot",
        }

    lines: List[str] = []
    items: List[Dict[str, Any]] = []

    for r in rows:
        desc = str(r.get("DESC_CUSTO", "")).strip() or "sem descrição"
        val_raw = r.get("VLR_CUSTO")
        val_num = _to_float(val_raw)
        val_fmt = _format_money(val_num) if val_num is not None else str(val_raw)

        lines.append(f"- {desc}: R$ {val_fmt}")
        items.append(
            {
                "descricao": desc,
                "valor": val_num,
                "valor_raw": val_raw,
            }
        )

    answer = (
        f"Os custos identificados para a {scope.get('obra') or 'obra'} são:\n"
        + "\n".join(lines[:50])
    )

    return {
        "answer": answer,
        "status": "ok",
        "data": {
            "items": items,
            "count": len(items),
            "source": "snapshot",
        },
    }


async def handle_structured_max_cost(
    question: str,
    scope: Dict[str, Optional[str]],
    plan: Dict[str, Any],
) -> Dict[str, Any]:
    snapshot = await get_latest_snapshot(scope)

    if not snapshot:
        return {
            "answer": (
                f"Ainda não encontrei dados de custos para a {scope.get('obra') or 'obra informada'}."
            ),
            "status": "no_data_for_scope",
        }

    rows = snapshot.get("rows") or []
    valid_rows: List[Dict[str, Any]] = []

    for r in rows:
        v = _to_float(r.get("VLR_CUSTO"))
        if v is not None:
            valid_rows.append(
                {
                    "DATA": r.get("DATA"),
                    "DESC_CUSTO": r.get("DESC_CUSTO"),
                    "VLR_CUSTO": v,
                }
            )

    if not valid_rows:
        return {
            "answer": "Não encontrei valores válidos para identificar o maior custo.",
            "status": "no_valid_values",
        }

    maior = max(valid_rows, key=lambda x: x["VLR_CUSTO"])

    desc = str(maior.get("DESC_CUSTO") or "sem descrição").strip()
    data = maior.get("DATA", "sem data")
    valor = float(maior["VLR_CUSTO"])

    return {
        "answer": (
            f"O maior custo lançado na {scope.get('obra') or 'obra'} foi "
            f"{desc}, no valor de R$ {_format_money(valor)}, na data {data}."
        ),
        "status": "ok",
        "data": {
            "descricao": desc,
            "valor": valor,
            "formatted_valor": _format_money(valor),
            "data": data,
            "source": "snapshot_max",
        },
    }


async def handle_structured_insights(
    question: str,
    scope: Dict[str, Optional[str]],
    plan: Dict[str, Any],
) -> Dict[str, Any]:
    snapshot = await get_latest_snapshot(scope)

    if not snapshot:
        return {
            "answer": (
                f"Ainda não encontrei dados suficientes para gerar insights da "
                f"{scope.get('obra') or 'obra informada'}."
            ),
            "status": "no_data_for_scope",
        }

    rows = snapshot.get("rows") or []
    valid_rows: List[Dict[str, Any]] = []

    for r in rows:
        v = _to_float(r.get("VLR_CUSTO"))
        if v is not None:
            valid_rows.append(
                {
                    "DATA": r.get("DATA"),
                    "DESC_CUSTO": r.get("DESC_CUSTO"),
                    "VLR_CUSTO": v,
                }
            )

    if not valid_rows:
        return {
            "answer": "Não encontrei valores válidos para gerar insights.",
            "status": "no_valid_values",
        }

    total = sum(r["VLR_CUSTO"] for r in valid_rows)
    maior = max(valid_rows, key=lambda x: x["VLR_CUSTO"])

    freq: Dict[str, int] = {}
    for r in valid_rows:
        desc = str(r.get("DESC_CUSTO", "sem descrição")).strip()
        freq[desc] = freq.get(desc, 0) + 1

    recorrentes = [k for k, v in freq.items() if v > 1]

    insights = [
        f"Total atual: R$ {_format_money(total)}.",
        f"Maior custo identificado: {maior['DESC_CUSTO']} (R$ {_format_money(maior['VLR_CUSTO'])}).",
    ]

    if recorrentes:
        insights.append(
            "Custos recorrentes identificados: " + ", ".join(recorrentes[:10]) + "."
        )

    return {
        "answer": "Insights automáticos:\n- " + "\n- ".join(insights),
        "status": "ok",
        "data": {
            "total": total,
            "maior_custo": maior,
            "recorrentes": recorrentes,
            "source": "snapshot_insights",
        },
    }


async def handle_semantic_rag(
    question: str,
    scope: Dict[str, Optional[str]],
    plan: Dict[str, Any],
) -> Dict[str, Any]:
    try:
        hits = search_qdrant(question, scope)
    except Exception as e:
        return {
            "answer": f"Não consegui consultar o contexto vetorial agora: {type(e).__name__}: {e}",
            "status": "vector_error",
        }

    if not hits:
        return {
            "answer": (
                f"Ainda não encontrei dados suficientes para a {scope.get('obra') or 'obra informada'} "
                f"nesse contexto. Tente informar a obra, a pasta ou o nome do arquivo."
            ),
            "status": "insufficient_retrieval",
            "data": {
                "contexts_found": 0,
                "hits": [],
                "search_debug": build_search_debug(question, scope, []),
            },
        }

    context_text = _build_context_text(hits)

    if not context_text.strip():
        return {
            "answer": (
                f"Encontrei registros vetoriais, mas sem contexto textual suficiente para responder "
                f"com segurança sobre a {scope.get('obra') or 'obra informada'}."
            ),
            "status": "insufficient_retrieval",
            "data": {
                "contexts_found": len(hits),
                "hits": hits,
                "search_debug": build_search_debug(question, scope, hits),
            },
        }

    try:
        answer = ask_llm(question, context_text)
        llm_status = "ok"
        llm_error = None
    except Exception as e:
        llm_status = "fallback"
        llm_error = f"{type(e).__name__}: {e}"

        top = hits[0] if hits else {}
        preview = (top.get("text") or "")[:1200]

        answer = (
            "Encontrei contexto relevante para responder, mas o gerador de resposta ficou indisponível no momento. "
            "Segue o trecho mais relevante recuperado:\n\n"
            f"{preview}"
        )

    return {
        "answer": answer,
        "status": "ok" if llm_status == "ok" else "partial_fallback",
        "data": {
            "contexts_found": len(hits),
            "hits": [
                {
                    "final_score": h.get("final_score"),
                    "vector_score": h.get("vector_score"),
                    "keyword_bonus": h.get("keyword_bonus"),
                    "obra_bonus": h.get("obra_bonus"),
                    "folder_bonus": h.get("folder_bonus"),
                    "inferred_folder": h.get("inferred_folder"),
                    "file_name": h.get("file_name"),
                    "sheet": h.get("sheet"),
                    "obra_name": h.get("obra_name"),
                    "folder_name": h.get("folder_name"),
                    "row_start": h.get("row_start"),
                    "row_end": h.get("row_end"),
                    "doc_type": h.get("doc_type"),
                }
                for h in hits
            ],
            "search_debug": build_search_debug(question, scope, hits),
            "source": "qdrant_semantic",
            "llm_status": llm_status,
            "llm_error": llm_error,
        },
    }

def _extract_lookup_term(question: str, scope: Dict[str, Optional[str]]) -> Optional[str]:
    q = _normalize_for_match(question)

    obra = _normalize_for_match(scope.get("obra") or "")
    folder = _normalize_for_match(scope.get("folder") or "")
    file_name = _normalize_for_match(scope.get("file_name") or "")

    # remove contexto conhecido do escopo
    for piece in [obra, folder, file_name]:
        if piece:
            q = q.replace(piece, " ")

    # remove padrões tipo "obra xxx" que sobrarem
    q = re.sub(r"\bobra\b", " ", q)

    fillers = {
        "teve", "tem", "existe", "existiu", "ha", "houve",
        "algum", "alguma",
        "custo", "custos", "gasto", "gastos", "despesa", "despesas",
        "com", "na", "no", "da", "do", "de", "em", "para", "por",
        "arquivo", "planilha", "pasta", "aba",
        "qual", "quais", "mostrar", "mostre", "liste", "listar",
    }

    tokens = re.findall(r"[a-z0-9_]+", q)
    tokens = [t for t in tokens if t not in fillers and len(t) >= 3]

    if not tokens:
        return None

    priority_terms = [
        "peao",
        "diaria",
        "container",
        "locacao",
        "aluguel",
        "pagamento",
        "parcela",
        "recebimento",
        "concreto",
        "cimento",
        "ferragem",
        "material",
        "fornecedor",
        "servico",
    ]

    for term in priority_terms:
        if term in tokens:
            return term

    return tokens[-1]


def _expand_lookup_terms(term: str) -> List[str]:
    base = _normalize_for_match(term)

    synonyms = {
        "aluguel": ["aluguel", "locacao"],
        "locacao": ["locacao", "aluguel"],
        "diaria": ["diaria", "peao"],
        "peao": ["peao", "diaria"],
        "container": ["container"],
        "pagamento": ["pagamento", "parcela", "recebimento"],
        "parcela": ["parcela", "pagamento"],
        "recebimento": ["recebimento", "pagamento", "parcela"],
        "servico": ["servico", "mao", "peao", "diaria"],
        "material": ["material", "insumo"],
    }

    return synonyms.get(base, [base])


async def handle_structured_lookup_cost(
    question: str,
    scope: Dict[str, Optional[str]],
    plan: Dict[str, Any],
) -> Dict[str, Any]:
    snapshot = await get_latest_snapshot(scope)

    if not snapshot:
        return {
            "answer": (
                f"Ainda não encontrei dados de custos para a {scope.get('obra') or 'obra informada'}."
            ),
            "status": "no_data_for_scope",
        }

    rows = snapshot.get("rows") or []
    if not rows:
        return {
            "answer": "A planilha foi encontrada, mas não há linhas de custos disponíveis.",
            "status": "empty_snapshot",
        }

    lookup_term = _extract_lookup_term(question, scope)
    if not lookup_term:
        return {
            "answer": "Não consegui identificar qual custo específico você quer buscar.",
            "status": "clarify",
            "data": {
                "source": "snapshot_lookup",
            },
        }

    terms = _expand_lookup_terms(lookup_term)
    matches: List[Dict[str, Any]] = []

    for r in rows:
        desc_raw = str(r.get("DESC_CUSTO", "")).strip()
        desc_norm = _normalize_for_match(desc_raw)

        if any(term in desc_norm for term in terms):
            val_raw = r.get("VLR_CUSTO")
            val_num = _to_float(val_raw)
            val_fmt = _format_money(val_num) if val_num is not None else str(val_raw)

            matches.append(
                {
                    "descricao": desc_raw or "sem descrição",
                    "valor": val_num,
                    "valor_raw": val_raw,
                    "valor_fmt": val_fmt,
                    "data": r.get("DATA"),
                }
            )

    if not matches:
        return {
            "answer": (
                f"Não encontrei custos relacionados a '{lookup_term}' "
                f"na {scope.get('obra') or 'obra informada'}."
            ),
            "status": "ok",
            "data": {
                "lookup_term": lookup_term,
                "expanded_terms": terms,
                "matches": [],
                "count": 0,
                "source": "snapshot_lookup",
            },
        }

    linhas = []
    for m in matches[:20]:
        if m.get("data"):
            linhas.append(f"- {m['descricao']}: R$ {m['valor_fmt']} ({m['data']})")
        else:
            linhas.append(f"- {m['descricao']}: R$ {m['valor_fmt']}")

    answer = (
        f"Sim. Encontrei {len(matches)} lançamento(s) relacionados a '{lookup_term}' "
        f"na {scope.get('obra') or 'obra'}:\n" + "\n".join(linhas)
    )

    return {
        "answer": answer,
        "status": "ok",
        "data": {
            "lookup_term": lookup_term,
            "expanded_terms": terms,
            "matches": matches,
            "count": len(matches),
            "source": "snapshot_lookup",
        },
    }



async def handle_clarify(
    question: str,
    scope: Dict[str, Optional[str]],
    plan: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "answer": (
            "Não consegui identificar claramente o contexto da sua pergunta. "
            "Informe, por exemplo, a obra e o tipo de informação desejada. "
            "Ex.: 'qual o total de custos da obra A?'"
        ),
        "status": "clarify",
    }