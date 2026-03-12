from __future__ import annotations

import os
import re
from typing import Dict, Any, Optional, List, Tuple, cast

from mistralai import Mistral

from app.core.mongo import get_db
from app.ingest.qdrant_indexer import get_qdrant


MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")
MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-large-latest")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "drive_rag")

client = Mistral(api_key=MISTRAL_API_KEY)


# =========================================================
# INTENT DETECTION
# =========================================================

def detect_intent(question: str) -> str:
    q = question.lower()

    if "mudou" in q or "alteração" in q or "alteracao" in q or "mudanca" in q:
        return "diff"

    if "total" in q or "soma" in q:
        return "total"

    if "último" in q or "ultimo" in q or "última" in q or "ultima" in q:
        return "last"

    return "rag"


# =========================================================
# ENTITY EXTRACTION
# =========================================================

def extract_entity(question: str) -> Dict[str, Optional[str]]:
    """
    Extrai contexto operacional da pergunta.
    Ex:
    'Na OBRA F, o que mudou na planilha de custos?'
      -> obra='OBRA F', folder='CUSTOS'
    """
    q = question.upper()

    scope = {
        "obra": None,
        "folder": None,
        "file_name": None,
    }

    # ---------------------------------------------
    # OBRA
    # ---------------------------------------------
    # Pega padrões como:
    # OBRA F
    # OBRA A
    # OBRA 12
    m = re.search(r"\bOBRA\s+([A-Z0-9_-]+)\b", q)
    if m:
        scope["obra"] = f"OBRA {m.group(1)}"

    # ---------------------------------------------
    # FOLDER / TIPO
    # ---------------------------------------------
    if "CUSTO" in q or "CUSTOS" in q:
        scope["folder"] = "CUSTOS"

    elif "FATURAMENTO" in q:
        scope["folder"] = "FATURAMENTO"

    elif "ORCAMENTO" in q or "ORÇAMENTO" in q:
        scope["folder"] = "ORCAMENTO"

    # ---------------------------------------------
    # FILE NAME
    # ---------------------------------------------
    if "CUSTOS.XLSX" in q or "PLANILHA DE CUSTOS" in q:
        scope["file_name"] = "custos.xlsx"

    elif "FATURAMENTO.XLSX" in q or "PLANILHA DE FATURAMENTO" in q:
        scope["file_name"] = "faturamento.xlsx"

    return scope


# =========================================================
# FILTER BUILDERS
# =========================================================

def _build_diff_filter(scope: Dict[str, Optional[str]]) -> Dict[str, Any]:
    flt: Dict[str, Any] = {}

    if scope.get("folder"):
        flt["parent_folder_name"] = scope["folder"]

    if scope.get("file_name"):
        flt["file_name"] = scope["file_name"]

    # no futuro: quando salvar "obra_name" no snapshot/diff, usar aqui
    if scope.get("obra"):
        # fallback temporário: tenta achar no file_name ou parent path futuro
        # hoje teu diff ainda não tem obra_name explícito
        pass

    return flt


def _build_snapshot_filter(scope: Dict[str, Optional[str]]) -> Dict[str, Any]:
    flt: Dict[str, Any] = {}

    if scope.get("folder"):
        flt["parent_folder_name"] = scope["folder"]

    if scope.get("file_name"):
        flt["file_name"] = scope["file_name"]

    if scope.get("obra"):
        # quando teu snapshot passar a salvar obra_name, filtra aqui
        pass

    return flt


# =========================================================
# SNAPSHOT / DIFF QUERIES
# =========================================================

async def get_latest_snapshot(scope: Dict[str, Optional[str]]) -> Optional[Dict[str, Any]]:
    db = get_db()
    flt = _build_snapshot_filter(scope)

    return await db["drive_sheet_snapshots"].find_one(
        flt,
        sort=[("indexed_at", -1)]
    )


async def get_last_diff(scope: Dict[str, Optional[str]]) -> Optional[Dict[str, Any]]:
    db = get_db()
    flt = _build_diff_filter(scope)

    return await db["drive_sheet_diffs"].find_one(
        flt,
        sort=[("created_at", -1)]
    )


async def calculate_total(snapshot: Dict[str, Any]) -> float:
    rows = snapshot.get("rows", [])
    total = 0.0

    for r in rows:
        v = r.get("VLR_CUSTO")

        try:
            total += float(v)
        except Exception:
            pass

    return total


def _format_money(v: float) -> str:
    return f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


# =========================================================
# LAST ENTRY HELPERS
# =========================================================

def _pick_latest_added_row(diff: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    added = diff.get("added_rows") or []
    if not added:
        return None

    # tenta usar a última linha do array
    return added[-1]


# =========================================================
# QDRANT SEARCH
# =========================================================

def search_qdrant(question: str, scope: Dict[str, Optional[str]]) -> List[str]:
    """
    Primeira versão simples:
    - busca vetorial textual
    - sem filtro de payload ainda
    """
    qdrant = get_qdrant()

    # IMPORTANTE:
    # teu qdrant_client pode não suportar query_text direto dependendo da versão.
    # Se isso já estiver funcionando no teu ambiente, mantém.
    # Caso contrário, depois a gente adapta para embedding da pergunta + search por vector.
    results = qdrant.search(
        collection_name=QDRANT_COLLECTION,
        query_vector=None,
        query_text=question,
        limit=3,
    )

    texts = []

    for r in results:
        payload = r.payload or {}
        txt = payload.get("text", "")
        if txt:
            texts.append(txt)

    return texts


# =========================================================
# LLM RESPONSE
# =========================================================

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
"""

    messages = cast(Any, [
        {"role": "system", "content": "Você é um analista de dados."},
        {"role": "user", "content": prompt},
    ])

    resp = client.chat.complete(
        model=MISTRAL_MODEL,
        messages=messages,
    )

    return resp.choices[0].message.content


# =========================================================
# MAIN SERVICE
# =========================================================

async def query_ai(question: str) -> Dict[str, Any]:
    intent = detect_intent(question)
    scope = extract_entity(question)

    # -----------------------------------------------------
    # DIFF
    # -----------------------------------------------------
    if intent == "diff":
        diff = await get_last_diff(scope)

        if not diff:
            return {
                "answer": "Nenhuma alteração encontrada para esse contexto.",
                "intent": intent,
                "scope": scope,
            }

        summary = diff.get("summary") or "Alteração identificada."

        return {
            "answer": summary,
            "intent": intent,
            "scope": scope,
        }

    # -----------------------------------------------------
    # TOTAL
    # -----------------------------------------------------
    if intent == "total":
        snapshot = await get_latest_snapshot(scope)

        if not snapshot:
            return {
                "answer": "Nenhum dado encontrado para esse contexto.",
                "intent": intent,
                "scope": scope,
            }

        total = await calculate_total(snapshot)

        return {
            "answer": f"O total atual de custos é {_format_money(total)}.",
            "intent": intent,
            "scope": scope,
        }

    # -----------------------------------------------------
    # LAST
    # -----------------------------------------------------
    if intent == "last":
        diff = await get_last_diff(scope)

        if not diff:
            return {
                "answer": "Nenhuma alteração recente encontrada para esse contexto.",
                "intent": intent,
                "scope": scope,
            }

        row = _pick_latest_added_row(diff)

        if not row:
            return {
                "answer": "Não foi possível identificar um último lançamento novo.",
                "intent": intent,
                "scope": scope,
            }

        desc = row.get("DESC_CUSTO", "sem descrição")
        val = row.get("VLR_CUSTO", "sem valor")
        data = row.get("DATA", "sem data")

        return {
            "answer": f"O último lançamento identificado foi {desc}, no valor de {val}, na data {data}.",
            "intent": intent,
            "scope": scope,
        }

    # -----------------------------------------------------
    # RAG FALLBACK
    # -----------------------------------------------------
    contexts = search_qdrant(question, scope)
    context_text = "\n\n".join(contexts)

    if not context_text.strip():
        return {
            "answer": "Não encontrei contexto suficiente para responder com segurança.",
            "intent": intent,
            "scope": scope,
        }

    answer = ask_llm(question, context_text)

    return {
        "answer": answer,
        "intent": intent,
        "scope": scope,
    }