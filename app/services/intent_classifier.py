from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

from mistralai import Mistral

from app.services.query_entities import normalize_text

MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")
MISTRAL_INTENT_MODEL = os.getenv("MISTRAL_INTENT_MODEL", "mistral-large-latest")

VALID_ROUTES = {
    "structured_total",
    "structured_diff",
    "structured_last",
    "structured_list_costs",
    "structured_insights",
    "structured_max_cost",
    "semantic_rag",
    "clarify",
}

_client: Optional[Mistral] = None


def _log(msg: str) -> None:
    print(msg, flush=True)


def _get_client() -> Mistral:
    global _client

    if _client is not None:
        return _client

    if not MISTRAL_API_KEY:
        raise RuntimeError("MISTRAL_API_KEY não configurado")

    _client = Mistral(api_key=MISTRAL_API_KEY)
    return _client


def _fallback_route(question: str) -> Dict[str, Any]:
    q = normalize_text(question)

    if any(term in q for term in [
        "mudou",
        "alterou",
        "diferença",
        "diferenca",
        "comparado",
        "comparar",
        "antes e depois",
        "o que mudou",
    ]):
        return {
            "route": "structured_diff",
            "confidence": 0.70,
            "reason": "fallback_diff_keywords",
            "needs_scope": False,
            "classifier": "fallback",
        }

    if any(term in q for term in [
        "ultimo",
        "último",
        "mais recente",
        "ultima",
        "última atualização",
        "último lançamento",
        "ultimo lançamento",
    ]):
        return {
            "route": "structured_last",
            "confidence": 0.68,
            "reason": "fallback_last_keywords",
            "needs_scope": False,
            "classifier": "fallback",
        }

    if any(term in q for term in [
        "maior custo",
        "maior gasto",
        "maior despesa",
        "custo mais alto",
        "gasto mais alto",
        "despesa mais alta",
    ]):
        return {
            "route": "structured_max_cost",
            "confidence": 0.72,
            "reason": "fallback_max_keywords",
            "needs_scope": False,
            "classifier": "fallback",
        }

    if any(term in q for term in [
        "listar custos",
        "liste os custos",
        "quais custos",
        "quais são os custos",
        "quais sao os custos",
        "quais gastos",
        "quais despesas",
        "mostre os custos",
    ]):
        return {
            "route": "structured_list_costs",
            "confidence": 0.70,
            "reason": "fallback_list_keywords",
            "needs_scope": False,
            "classifier": "fallback",
        }

    if any(term in q for term in [
        "insight",
        "insights",
        "análise",
        "analise",
        "resumo",
        "resuma",
        "o que os dados mostram",
        "interpreta",
        "interpretar",
    ]):
        return {
            "route": "structured_insights",
            "confidence": 0.67,
            "reason": "fallback_insights_keywords",
            "needs_scope": False,
            "classifier": "fallback",
        }

    if any(term in q for term in [
        "valor",
        "total",
        "soma",
        "quanto deu",
        "quanto já deu",
        "quanto ja deu",
        "custo total",
        "total de custos",
        "valor acumulado",
        "ate agora",
        "até agora",
        "valor total",
    ]):
        return {
            "route": "structured_total",
            "confidence": 0.72,
            "reason": "fallback_total_keywords",
            "needs_scope": False,
            "classifier": "fallback",
        }

    if len(q.strip()) < 8:
        return {
            "route": "clarify",
            "confidence": 0.80,
            "reason": "fallback_too_short",
            "needs_scope": True,
            "classifier": "fallback",
        }

    return {
        "route": "semantic_rag",
        "confidence": 0.60,
        "reason": "fallback_semantic_default",
        "needs_scope": False,
        "classifier": "fallback",
    }


def _build_prompt(question: str, entities: Dict[str, Any]) -> str:
    return f"""
Você é um classificador de intenções para um agente corporativo que responde perguntas sobre obras, planilhas, custos, alterações e documentos.

Classifique a pergunta do usuário em EXATAMENTE uma rota:

- structured_total
- structured_diff
- structured_last
- structured_list_costs
- structured_insights
- structured_max_cost
- semantic_rag
- clarify

Definições:
- structured_total: soma, acumulado, total, valor total, custo total.
- structured_diff: mudanças, diferenças, o que mudou, comparação antes/depois.
- structured_last: último lançamento, última atualização, item mais recente.
- structured_list_costs: listar custos, mostrar custos, quais despesas/gastos.
- structured_insights: análise gerencial, resumo analítico, principais padrões.
- structured_max_cost: maior custo/gasto/despesa.
- semantic_rag: pergunta aberta, contextual, explicativa, documental, sem formato estruturado claro.
- clarify: pergunta vaga, ambígua, incompleta, sem escopo mínimo.

Critérios:
- O usuário pode escrever de forma torta, incompleta ou informal.
- Priorize semantic_rag quando a pergunta não for claramente matemática/estruturada.
- Use clarify quando a pergunta estiver curta ou ambígua demais para agir com segurança.
- Responda SOMENTE com JSON válido.
- Não use markdown.
- Não use bloco de código.

Pergunta:
{question}

Entidades extraídas:
{json.dumps(entities, ensure_ascii=False)}

Responda SOMENTE JSON válido no formato:
{{
  "route": "semantic_rag",
  "confidence": 0.93,
  "reason": "motivo curto",
  "needs_scope": false
}}
""".strip()


def _extract_json_object(text: str) -> Dict[str, Any]:
    text = (text or "").strip()

    if not text:
        raise ValueError("Resposta vazia da Mistral")

    # remove code fences
    if text.startswith("```"):
        text = text.strip()

        if text.startswith("```json"):
            text = text[len("```json"):].strip()
        elif text.startswith("```JSON"):
            text = text[len("```JSON"):].strip()
        elif text.startswith("```"):
            text = text[3:].strip()

        if text.endswith("```"):
            text = text[:-3].strip()

    start = text.find("{")
    end = text.rfind("}")

    if start == -1 or end == -1 or end <= start:
        raise ValueError(f"Resposta sem JSON válido: {text[:300]}")

    candidate = text[start:end + 1]
    return json.loads(candidate)


def classify_route_with_mistral(question: str, entities: Dict[str, Any]) -> Dict[str, Any]:
    try:
        client = _get_client()
        prompt = _build_prompt(question, entities)

        resp = client.chat.complete(
            model=MISTRAL_INTENT_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Você é um classificador de intenções. "
                        "Responda somente JSON válido, sem markdown, sem explicações extras."
                    ),
                },
                {
                    "role": "user",
                    "content": prompt,
                },
            ],
            temperature=0.0,
        )

        content = (resp.choices[0].message.content or "").strip()
        _log(f"[intent][raw] {content[:500]}")

        data = _extract_json_object(content)

        route = str(data.get("route", "")).strip()
        confidence = float(data.get("confidence", 0.0))
        reason = str(data.get("reason", "")).strip() or "mistral_classification"
        needs_scope = bool(data.get("needs_scope", False))

        if route not in VALID_ROUTES:
            raise ValueError(f"Rota inválida retornada pela LLM: {route}")

        result = {
            "route": route,
            "confidence": max(0.0, min(confidence, 1.0)),
            "reason": reason,
            "needs_scope": needs_scope,
            "classifier": "mistral",
        }

        _log(
            f"[intent][parsed] route={result['route']} "
            f"confidence={result['confidence']} "
            f"needs_scope={result['needs_scope']} "
            f"reason={result['reason']}"
        )

        return result

    except Exception as e:
        fb = _fallback_route(question)
        fb["classifier_error"] = f"{type(e).__name__}: {e}"
        _log(f"[intent][fallback] err={fb['classifier_error']} route={fb['route']}")
        return fb