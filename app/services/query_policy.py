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


async def apply_route_policy(
    question: str,
    entities: Dict[str, Optional[str]],
    route_plan: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Corrige rota baseada em:
    - existência de snapshot
    - existência de diff
    - tipo real da pergunta
    - escopo disponível
    """
    route = route_plan["route"]
    confidence = route_plan.get("confidence", 0.0)
    reason = route_plan.get("reason", "")

    has_snapshot = await has_snapshot_for_scope(entities)
    has_diff = await has_diff_for_scope(entities)

    # -----------------------------------------------------
    # Se não tem obra/folder suficiente, clarifica
    # -----------------------------------------------------
    if not entities.get("obra") and not entities.get("folder"):
        return {
            "route": "clarify",
            "confidence": 0.98,
            "reason": "policy_missing_scope",
            "policy_adjusted": True,
            "has_snapshot": has_snapshot,
            "has_diff": has_diff,
        }

    # -----------------------------------------------------
    # Se parece maior custo e existe snapshot, prioriza structured_max_cost
    # -----------------------------------------------------
    if _looks_like_max_cost(question) and has_snapshot:
        return {
            "route": "structured_max_cost",
            "confidence": max(confidence, 0.97),
            "reason": "policy_max_cost_with_snapshot",
            "policy_adjusted": route != "structured_max_cost",
            "has_snapshot": has_snapshot,
            "has_diff": has_diff,
        }

    # -----------------------------------------------------
    # Se parece total e existe snapshot, prioriza structured_total
    # -----------------------------------------------------
    if _looks_like_numeric_aggregate(question) and has_snapshot:
        return {
            "route": "structured_total",
            "confidence": max(confidence, 0.97),
            "reason": "policy_numeric_aggregate_with_snapshot",
            "policy_adjusted": route != "structured_total",
            "has_snapshot": has_snapshot,
            "has_diff": has_diff,
        }

    # -----------------------------------------------------
    # Se parece listagem de custos e existe snapshot, prioriza estruturado
    # -----------------------------------------------------
    if _looks_like_cost_listing(question) and has_snapshot:
        return {
            "route": "structured_list_costs",
            "confidence": max(confidence, 0.95),
            "reason": "policy_cost_list_with_snapshot",
            "policy_adjusted": route != "structured_list_costs",
            "has_snapshot": has_snapshot,
            "has_diff": has_diff,
        }

    # -----------------------------------------------------
    # Se pediu mudança, mantém diff
    # -----------------------------------------------------
    if route == "structured_diff":
        return {
            "route": "structured_diff",
            "confidence": confidence,
            "reason": reason or "policy_keep_diff_route",
            "policy_adjusted": False,
            "has_snapshot": has_snapshot,
            "has_diff": has_diff,
        }

    return {
        "route": route,
        "confidence": confidence,
        "reason": reason,
        "policy_adjusted": False,
        "has_snapshot": has_snapshot,
        "has_diff": has_diff,
    }