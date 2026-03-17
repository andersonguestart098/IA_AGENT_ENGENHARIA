from __future__ import annotations

from typing import Dict, Any, Optional

from app.core.mongo import get_db
from app.services.query_entities import normalize_text


def _build_snapshot_filter(scope: Dict[str, Optional[str]]) -> Dict[str, Any]:
    flt: Dict[str, Any] = {}

    if scope.get("obra"):
        flt["obra_name"] = scope["obra"]

    if scope.get("folder"):
        flt["parent_folder_name"] = scope["folder"]

    if scope.get("file_name"):
        flt["file_name"] = scope["file_name"]

    return flt


def _build_diff_filter(scope: Dict[str, Optional[str]]) -> Dict[str, Any]:
    flt: Dict[str, Any] = {}

    if scope.get("obra"):
        flt["obra_name"] = scope["obra"]

    if scope.get("folder"):
        flt["parent_folder_name"] = scope["folder"]

    if scope.get("file_name"):
        flt["file_name"] = scope["file_name"]

    return flt


async def has_snapshot_for_scope(scope: Dict[str, Optional[str]]) -> bool:
    db = get_db()
    flt = _build_snapshot_filter(scope)
    doc = await db["drive_sheet_snapshots"].find_one(flt)
    return doc is not None


async def has_diff_for_scope(scope: Dict[str, Optional[str]]) -> bool:
    db = get_db()
    flt = _build_diff_filter(scope)
    doc = await db["drive_sheet_diffs"].find_one(flt)
    return doc is not None


def _looks_like_numeric_aggregate(question: str) -> bool:
    q = normalize_text(question)
    return any(term in q for term in [
        "valor",
        "total",
        "soma",
        "quanto",
        "quanto deu",
        "quanto ja deu",
        "quanto já deu",
        "custo total",
        "total de custos",
        "valor dos custos",
        "valor acumulado",
        "ate agora",
        "até agora",
        "ate o momento",
        "até o momento",
    ])


def _looks_like_cost_listing(question: str) -> bool:
    q = normalize_text(question)
    return any(term in q for term in [
        "quais custos",
        "quais sao os custos",
        "quais são os custos",
        "listar custos",
        "liste os custos",
        "mostre os custos",
        "quais gastos",
        "quais despesas",
    ])


def _looks_like_max_cost(question: str) -> bool:
    q = normalize_text(question)
    return any(term in q for term in [
        "maior custo",
        "maior gasto",
        "maior despesa",
        "custo mais alto",
        "gasto mais alto",
        "despesa mais alta",
        "qual foi o maior custo",
        "qual o maior custo",
        "qual foi a maior despesa",
        "qual a maior despesa",
        "qual o custo mais alto",
        "qual o gasto mais alto",
    ])


def _looks_like_last(question: str) -> bool:
    q = normalize_text(question)
    return any(term in q for term in [
        "ultimo",
        "último",
        "ultima",
        "última",
        "mais recente",
        "último lançamento",
        "ultimo lançamento",
        "última atualização",
        "ultima atualização",
    ])


def _looks_like_diff(question: str) -> bool:
    q = normalize_text(question)
    return any(term in q for term in [
        "mudou",
        "alterou",
        "diferença",
        "diferenca",
        "comparar",
        "comparado",
        "antes e depois",
        "o que mudou",
    ])


def _looks_like_cost_lookup(question: str) -> bool:
    q = normalize_text(question)

    lookup_terms = [
        "teve",
        "tem",
        "existe",
        "existiu",
        "ha",
        "há",
        "houve",
        "algum",
        "alguma",
    ]

    list_terms = [
        "quais custos",
        "quais sao os custos",
        "quais são os custos",
        "listar custos",
        "liste os custos",
        "mostre os custos",
        "quais gastos",
        "quais despesas",
    ]

    if any(term in q for term in list_terms):
        return False

    return any(term in q for term in lookup_terms)


def _looks_like_cost_domain(question: str) -> bool:
    q = normalize_text(question)
    return any(term in q for term in [
        "custo",
        "custos",
        "gasto",
        "gastos",
        "despesa",
        "despesas",
        "locacao",
        "locação",
        "aluguel",
        "diaria",
        "diária",
        "container",
        "peao",
        "peão",
        "material",
        "servico",
        "serviço",
        "fornecedor",
    ])


async def apply_route_policy(
    question: str,
    entities: Dict[str, Optional[str]],
    route_plan: Dict[str, Any],
) -> Dict[str, Any]:
    route = route_plan.get("route", "clarify")
    confidence = float(route_plan.get("confidence", 0.0) or 0.0)
    reason = route_plan.get("reason", "")
    needs_scope = bool(route_plan.get("needs_scope", False))

    has_snapshot = await has_snapshot_for_scope(entities)
    has_diff = await has_diff_for_scope(entities)

    has_min_scope = bool(
        entities.get("obra") or entities.get("folder") or entities.get("file_name")
    )

    # 1) Sem escopo mínimo e modelo já acha que precisa escopo
    if not has_min_scope and needs_scope:
        return {
            "route": "clarify",
            "confidence": 0.98,
            "reason": "policy_missing_scope_from_classifier",
            "policy_adjusted": route != "clarify",
            "has_snapshot": has_snapshot,
            "has_diff": has_diff,
        }

    # 2) Confiança muito baixa
    if confidence < 0.45 and not has_min_scope:
        return {
            "route": "clarify",
            "confidence": 0.96,
            "reason": "policy_low_confidence_and_missing_scope",
            "policy_adjusted": True,
            "has_snapshot": has_snapshot,
            "has_diff": has_diff,
        }

    # 3) Overrides fortes por heurística + dado disponível
    if _looks_like_max_cost(question) and has_snapshot:
        return {
            "route": "structured_max_cost",
            "confidence": max(confidence, 0.97),
            "reason": "policy_max_cost_with_snapshot",
            "policy_adjusted": route != "structured_max_cost",
            "has_snapshot": has_snapshot,
            "has_diff": has_diff,
        }

    if _looks_like_numeric_aggregate(question) and has_snapshot:
        return {
            "route": "structured_total",
            "confidence": max(confidence, 0.97),
            "reason": "policy_numeric_aggregate_with_snapshot",
            "policy_adjusted": route != "structured_total",
            "has_snapshot": has_snapshot,
            "has_diff": has_diff,
        }

    if _looks_like_cost_lookup(question) and _looks_like_cost_domain(question) and has_snapshot:
        return {
            "route": "structured_lookup_cost",
            "confidence": max(confidence, 0.96),
            "reason": "policy_specific_cost_lookup_with_snapshot",
            "policy_adjusted": route != "structured_lookup_cost",
            "has_snapshot": has_snapshot,
            "has_diff": has_diff,
        }

    if _looks_like_cost_listing(question) and has_snapshot:
        return {
            "route": "structured_list_costs",
            "confidence": max(confidence, 0.95),
            "reason": "policy_cost_list_with_snapshot",
            "policy_adjusted": route != "structured_list_costs",
            "has_snapshot": has_snapshot,
            "has_diff": has_diff,
        }

    if _looks_like_last(question) and has_snapshot:
        return {
            "route": "structured_last",
            "confidence": max(confidence, 0.94),
            "reason": "policy_last_with_snapshot",
            "policy_adjusted": route != "structured_last",
            "has_snapshot": has_snapshot,
            "has_diff": has_diff,
        }

    if _looks_like_diff(question) and has_diff:
        return {
            "route": "structured_diff",
            "confidence": max(confidence, 0.94),
            "reason": "policy_diff_with_diff_data",
            "policy_adjusted": route != "structured_diff",
            "has_snapshot": has_snapshot,
            "has_diff": has_diff,
        }

    # 4) Se rota estruturada pede snapshot, mas não existe snapshot -> tenta semantic
    if route in {
        "structured_total",
        "structured_last",
        "structured_list_costs",
        "structured_lookup_cost",
        "structured_insights",
        "structured_max_cost",
    } and not has_snapshot:
        if has_min_scope:
            return {
                "route": "semantic_rag",
                "confidence": max(confidence, 0.80),
                "reason": "policy_no_snapshot_fallback_to_semantic",
                "policy_adjusted": True,
                "has_snapshot": has_snapshot,
                "has_diff": has_diff,
            }

        return {
            "route": "clarify",
            "confidence": 0.95,
            "reason": "policy_no_snapshot_and_missing_scope",
            "policy_adjusted": True,
            "has_snapshot": has_snapshot,
            "has_diff": has_diff,
        }

    # 5) Se pediu diff mas não há diff, tenta semantic
    if route == "structured_diff" and not has_diff:
        if has_min_scope:
            return {
                "route": "semantic_rag",
                "confidence": max(confidence, 0.78),
                "reason": "policy_no_diff_fallback_to_semantic",
                "policy_adjusted": True,
                "has_snapshot": has_snapshot,
                "has_diff": has_diff,
            }

        return {
            "route": "clarify",
            "confidence": 0.95,
            "reason": "policy_no_diff_and_missing_scope",
            "policy_adjusted": True,
            "has_snapshot": has_snapshot,
            "has_diff": has_diff,
        }

    # 6) Se semantic e não há escopo nenhum e baixa confiança, clarifica
    if route == "semantic_rag" and not has_min_scope and confidence < 0.60:
        return {
            "route": "clarify",
            "confidence": 0.93,
            "reason": "policy_semantic_low_confidence_missing_scope",
            "policy_adjusted": True,
            "has_snapshot": has_snapshot,
            "has_diff": has_diff,
        }

    # 7) Mantém rota original
    return {
        "route": route,
        "confidence": confidence,
        "reason": reason,
        "policy_adjusted": False,
        "has_snapshot": has_snapshot,
        "has_diff": has_diff,
    }