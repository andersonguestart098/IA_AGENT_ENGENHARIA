from __future__ import annotations

from typing import Dict, Any, Optional

from app.services.query_entities import normalize_text


VALID_ROUTES = {
    "structured_total",
    "structured_diff",
    "structured_last",
    "structured_list_costs",
    "structured_insights",
    "semantic_rag",
    "clarify",
}


def _contains_any(text: str, terms: list[str]) -> bool:
    return any(term in text for term in terms)


def classify_route(question: str, entities: Dict[str, Optional[str]]) -> Dict[str, Any]:
    """
    Classifica a rota inicial da pergunta.
    Não executa nada. Apenas decide a melhor rota provável.

    Retorno:
    {
        "route": "...",
        "confidence": 0.0-1.0,
        "reason": "...",
    }
    """
    q = normalize_text(question)

    # -----------------------------------------------------
    # Guardrails básicos
    # -----------------------------------------------------
    if not q.strip():
        return {
            "route": "clarify",
            "confidence": 0.99,
            "reason": "empty_question",
        }

    # -----------------------------------------------------
    # INSIGHTS
    # -----------------------------------------------------
    if _contains_any(q, [
        "insight", "insights", "analise", "análise", "resumo", "pontos principais",
        "o que chama atencao", "o que chama atenção", "o que voce percebe", "o que você percebe"
    ]):
        return {
            "route": "structured_insights",
            "confidence": 0.94,
            "reason": "insight_request",
        }

    # -----------------------------------------------------
    # DIFF / MUDANÇA
    # -----------------------------------------------------
    if _contains_any(q, [
        "mudou", "mudanca", "mudança", "alteracao", "alteração", "houve alteracao",
        "houve alteração", "o que mudou", "teve mudanca", "teve mudança"
    ]):
        return {
            "route": "structured_diff",
            "confidence": 0.96,
            "reason": "change_detection_question",
        }

    # -----------------------------------------------------
    # LAST / ÚLTIMO LANÇAMENTO
    # -----------------------------------------------------
    if _contains_any(q, [
        "ultimo", "último", "ultima", "última", "mais recente", "ultimo lancamento",
        "último lançamento", "ultimo registro", "último registro", "mais novo"
    ]):
        return {
            "route": "structured_last",
            "confidence": 0.94,
            "reason": "latest_record_question",
        }

    # -----------------------------------------------------
    # TOTAL / AGREGAÇÃO
    # -----------------------------------------------------
    if _contains_any(q, [
        "total",
        "soma",
        "quanto",
        "quanto deu",
        "quanto ja deu",
        "quanto já deu",
        "quanto custou",
        "valor total",
        "valor dos custos",
        "valor do custo",
        "custo total",
        "total de custos",
        "gasto atual",
        "ja gastou",
        "já gastou",
        "valor acumulado",
        "ate agora",
        "até agora",
        "ate o momento",
        "até o momento",
    ]):
        return {
            "route": "structured_total",
            "confidence": 0.90,
            "reason": "aggregate_numeric_question",
        }

    # -----------------------------------------------------
    # LISTAGEM DE CUSTOS
    # -----------------------------------------------------
    if _contains_any(q, [
        "quais custos",
        "quais sao os custos",
        "quais são os custos",
        "listar custos",
        "liste os custos",
        "mostre os custos",
        "quais gastos",
        "quais despesas",
    ]):
        return {
            "route": "structured_list_costs",
            "confidence": 0.88,
            "reason": "cost_list_question",
        }

    # -----------------------------------------------------
    # Sem obra e sem domínio => clarificação
    # -----------------------------------------------------
    if not entities.get("obra") and not entities.get("folder"):
        return {
            "route": "clarify",
            "confidence": 0.75,
            "reason": "missing_scope",
        }

    # -----------------------------------------------------
    # Fallback semântico
    # -----------------------------------------------------
    return {
        "route": "semantic_rag",
        "confidence": 0.60,
        "reason": "fallback_semantic_search",
    }