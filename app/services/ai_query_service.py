from __future__ import annotations

from typing import Dict, Any, Optional, List

from app.core.mongo import get_db
from app.ingest.qdrant_indexer import get_qdrant

from mistralai import Mistral
import os


MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")
MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-large-latest")

client = Mistral(api_key=MISTRAL_API_KEY)


# =========================================================
# INTENT DETECTION
# =========================================================

def detect_intent(question: str) -> str:
    q = question.lower()

    if "mudou" in q or "alteração" in q or "mudanca" in q:
        return "diff"

    if "total" in q or "soma" in q:
        return "total"

    if "último" in q or "ultimo" in q:
        return "last"

    return "rag"


# =========================================================
# SNAPSHOT QUERIES
# =========================================================

async def get_latest_snapshot() -> Optional[Dict[str, Any]]:
    db = get_db()

    return await db["drive_sheet_snapshots"].find_one(
        {},
        sort=[("indexed_at", -1)]
    )


async def calculate_total(snapshot: Dict[str, Any]) -> float:
    rows = snapshot.get("rows", [])

    total = 0.0

    for r in rows:
        v = r.get("VLR_CUSTO")

        try:
            total += float(v)
        except:
            pass

    return total


async def get_last_diff() -> Optional[Dict[str, Any]]:
    db = get_db()

    return await db["drive_sheet_diffs"].find_one(
        {},
        sort=[("created_at", -1)]
    )


# =========================================================
# QDRANT SEARCH
# =========================================================

def search_qdrant(question: str) -> List[str]:
    qdrant = get_qdrant()

    results = qdrant.search(
        collection_name="drive_rag",
        query_vector=None,
        query_text=question,
        limit=3,
    )

    texts = []

    for r in results:
        payload = r.payload or {}
        texts.append(payload.get("text", ""))

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

Responda de forma clara e objetiva.
"""

    resp = client.chat.complete(
        model=MISTRAL_MODEL,
        messages=[
            {"role": "system", "content": "Você é um analista de dados."},
            {"role": "user", "content": prompt},
        ],
    )

    return resp.choices[0].message.content


# =========================================================
# MAIN SERVICE
# =========================================================

async def query_ai(question: str) -> Dict[str, Any]:

    intent = detect_intent(question)

    # -----------------------------------------------------
    # DIFF
    # -----------------------------------------------------

    if intent == "diff":

        diff = await get_last_diff()

        if not diff:
            return {"answer": "Nenhuma alteração encontrada."}

        summary = diff.get("summary")

        return {
            "answer": summary,
            "intent": intent,
        }

    # -----------------------------------------------------
    # TOTAL
    # -----------------------------------------------------

    if intent == "total":

        snapshot = await get_latest_snapshot()

        if not snapshot:
            return {"answer": "Nenhum dado encontrado."}

        total = await calculate_total(snapshot)

        return {
            "answer": f"O total atual de custos é {total:.2f}.",
            "intent": intent,
        }

    # -----------------------------------------------------
    # RAG FALLBACK
    # -----------------------------------------------------

    contexts = search_qdrant(question)

    context_text = "\n\n".join(contexts)

    answer = ask_llm(question, context_text)

    return {
        "answer": answer,
        "intent": intent,
    }