from __future__ import annotations

import os
from typing import Dict, Any, Optional, List, cast

from mistralai import Mistral
from qdrant_client.http import models as rest

from app.core.mongo import get_db
from app.ingest.qdrant_indexer import get_qdrant


MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")
MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-large-latest")
MISTRAL_EMBED_MODEL = os.getenv("MISTRAL_EMBED_MODEL", "mistral-embed")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "drive_rag")

client = Mistral(api_key=MISTRAL_API_KEY)


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
        sort=[("indexed_at", -1), ("modified_time", -1), ("created_at", -1)]
    )


async def get_last_diff(scope: Dict[str, Optional[str]]) -> Optional[Dict[str, Any]]:
    db = get_db()
    flt = _build_diff_filter(scope)

    return await db["drive_sheet_diffs"].find_one(
        flt,
        sort=[("created_at", -1), ("modified_time", -1)]
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
    resp = client.embeddings.create(
        model=MISTRAL_EMBED_MODEL,
        inputs=[text],
    )
    return resp.data[0].embedding


def search_qdrant(question: str, scope: Dict[str, Optional[str]]) -> List[str]:
    qdrant = get_qdrant()
    query_vector = embed_query(question)

    filters_to_try: List[Optional[rest.Filter]] = [
        _build_qdrant_filter(scope),
        _build_qdrant_filter({
            "obra": scope.get("obra"),
            "folder": None,
            "file_name": None,
        }),
        None,
    ]

    for q_filter in filters_to_try:
        results = qdrant.query_points(
            collection_name=QDRANT_COLLECTION,
            query=query_vector,
            query_filter=q_filter,
            limit=5,
        )

        texts: List[str] = []
        points = getattr(results, "points", None) or []

        for p in points:
            payload = getattr(p, "payload", None) or {}
            txt = payload.get("text", "")
            if txt:
                texts.append(txt)

        if texts:
            return texts

    return []


def ask_llm(question: str, context: str) -> str:
    prompt = f"""
Você é um analista de dados da empresa.

Pergunta do usuário:
{question}

Dados disponíveis:
{context}

Regras:
- Responda de forma objetiva e executiva.
- Não invente dados que não estejam no contexto.
- Se houver incerteza, diga claramente.
- Se a pergunta pedir análise, resuma o que os dados mostram.
- Se a pergunta pedir explicação, responda com base apenas no contexto.
"""

    messages = cast(Any, [
        {"role": "system", "content": "Você é um analista de dados da empresa."},
        {"role": "user", "content": prompt},
    ])

    resp = client.chat.complete(
        model=MISTRAL_MODEL,
        messages=messages,
    )

    return resp.choices[0].message.content


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
        "answer": f"O total atual de custos da {scope.get('obra') or 'obra'} é {_format_money(total)}.",
        "status": "ok",
        "data": {
            "total": total,
            "formatted_total": _format_money(total),
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
        items.append({
            "descricao": desc,
            "valor": val_num,
            "valor_raw": val_raw,
        })

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
            valid_rows.append({
                "DATA": r.get("DATA"),
                "DESC_CUSTO": r.get("DESC_CUSTO"),
                "VLR_CUSTO": v,
            })

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
            valid_rows.append({
                "DATA": r.get("DATA"),
                "DESC_CUSTO": r.get("DESC_CUSTO"),
                "VLR_CUSTO": v,
            })

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
        },
    }


async def handle_semantic_rag(
    question: str,
    scope: Dict[str, Optional[str]],
    plan: Dict[str, Any],
) -> Dict[str, Any]:
    try:
        contexts = search_qdrant(question, scope)
    except Exception as e:
        return {
            "answer": f"Não consegui consultar o contexto vetorial agora: {type(e).__name__}: {e}",
            "status": "vector_error",
        }

    context_text = "\n\n".join(contexts)

    if not context_text.strip():
        return {
            "answer": (
                f"Ainda não encontrei dados suficientes para a {scope.get('obra') or 'obra informada'} "
                f"nesse contexto."
            ),
            "status": "insufficient_retrieval",
        }

    answer = ask_llm(question, context_text)

    return {
        "answer": answer,
        "status": "ok",
        "data": {
            "contexts_found": len(contexts),
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