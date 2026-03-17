from __future__ import annotations

from typing import Dict, Any

from app.services.query_entities import extract_entities
from app.services.intent_classifier import classify_route_with_mistral
from app.services.query_policy import apply_route_policy
from app.services.query_handlers import (
    handle_structured_total,
    handle_structured_diff,
    handle_structured_last,
    handle_structured_list_costs,
    handle_structured_insights,
    handle_structured_max_cost,
    handle_semantic_rag,
    handle_clarify,
)


async def query_ai(question: str) -> Dict[str, Any]:
    question = (question or "").strip()

    if not question:
        return {
            "answer": "A pergunta veio vazia. Informe a obra e o tipo de informação desejada.",
            "status": "clarify",
            "scope": {
                "obra": None,
                "folder": None,
                "file_name": None,
                "period": None,
            },
            "route": "clarify",
            "route_confidence": 1.0,
            "route_reason": "empty_question",
            "policy_adjusted": False,
            "metadata": {},
        }

    entities = extract_entities(question)

    initial_plan = classify_route_with_mistral(question, entities)
    final_plan = await apply_route_policy(question, entities, initial_plan)

    route = final_plan.get("route", "clarify")

    print(f"[ai][question] {question}")
    print(f"[ai][entities] {entities}")
    print(f"[ai][plan.initial] {initial_plan}")
    print(f"[ai][plan.final] {final_plan}")

    if route == "structured_total":
        result = await handle_structured_total(question, entities, final_plan)

    elif route == "structured_diff":
        result = await handle_structured_diff(question, entities, final_plan)

    elif route == "structured_last":
        result = await handle_structured_last(question, entities, final_plan)

    elif route == "structured_list_costs":
        result = await handle_structured_list_costs(question, entities, final_plan)

    elif route == "structured_insights":
        result = await handle_structured_insights(question, entities, final_plan)

    elif route == "structured_max_cost":
        result = await handle_structured_max_cost(question, entities, final_plan)

    elif route == "semantic_rag":
        result = await handle_semantic_rag(question, entities, final_plan)

    else:
        result = await handle_clarify(question, entities, final_plan)

    return {
        "answer": result.get("answer", "Não foi possível gerar uma resposta."),
        "status": result.get("status", "ok"),
        "scope": entities,
        "route": route,
        "route_confidence": final_plan.get("confidence"),
        "route_reason": final_plan.get("reason"),
        "policy_adjusted": final_plan.get("policy_adjusted", False),
        "metadata": {
            **result.get("data", {}),
            "classifier": initial_plan.get("classifier"),
            "classifier_error": initial_plan.get("classifier_error"),
            "needs_scope": initial_plan.get("needs_scope"),
            "has_snapshot": final_plan.get("has_snapshot"),
            "has_diff": final_plan.get("has_diff"),
        },
    }