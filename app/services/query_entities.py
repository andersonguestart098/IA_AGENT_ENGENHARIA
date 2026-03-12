from __future__ import annotations

import re
import unicodedata
from typing import Dict, Optional


def _strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )


def normalize_text(text: str) -> str:
    text = text or ""
    text = text.strip()
    text = _strip_accents(text)
    text = re.sub(r"\s+", " ", text)
    return text.lower()


def extract_entities(question: str) -> Dict[str, Optional[str]]:
    """
    Extrai contexto operacional da pergunta.
    Retorna:
    {
        "obra": "OBRA A" | None,
        "folder": "CUSTOS" | "FATURAMENTO" | "ORCAMENTO" | None,
        "file_name": "custos.xlsx" | "faturamento.xlsx" | None,
        "period": None | "current"
    }
    """
    q_raw = question or ""
    q_norm = normalize_text(q_raw)
    q_upper = q_raw.upper()

    entities: Dict[str, Optional[str]] = {
        "obra": None,
        "folder": None,
        "file_name": None,
        "period": None,
    }

    # -----------------------------------------------------
    # OBRA
    # Ex.: "obra a", "obra f", "na obra 12", "obra alpha"
    # -----------------------------------------------------
    obra_match = re.search(r"\bobra\s+([a-zA-Z0-9_-]+)\b", q_norm)
    if obra_match:
        obra_token = obra_match.group(1).upper()
        entities["obra"] = f"OBRA {obra_token}"

    # -----------------------------------------------------
    # FOLDER / DOMÍNIO
    # -----------------------------------------------------
    if any(t in q_norm for t in ["custo", "custos", "gasto", "gastos", "despesa", "despesas"]):
        entities["folder"] = "CUSTOS"

    elif any(t in q_norm for t in ["faturamento", "faturado", "receita", "venda", "vendas"]):
        entities["folder"] = "FATURAMENTO"

    elif any(t in q_norm for t in ["orcamento", "orçamento", "previsto", "previsao", "previsão"]):
        entities["folder"] = "ORCAMENTO"

    # -----------------------------------------------------
    # FILE NAME
    # -----------------------------------------------------
    if "CUSTOS.XLSX" in q_upper or "PLANILHA DE CUSTOS" in q_upper:
        entities["file_name"] = "custos.xlsx"
        if not entities["folder"]:
            entities["folder"] = "CUSTOS"

    elif "FATURAMENTO.XLSX" in q_upper or "PLANILHA DE FATURAMENTO" in q_upper:
        entities["file_name"] = "faturamento.xlsx"
        if not entities["folder"]:
            entities["folder"] = "FATURAMENTO"

    elif "ORCAMENTO.XLSX" in q_upper or "ORÇAMENTO.XLSX" in q_upper or "PLANILHA DE ORCAMENTO" in q_upper:
        entities["file_name"] = "orcamento.xlsx"
        if not entities["folder"]:
            entities["folder"] = "ORCAMENTO"

    # -----------------------------------------------------
    # PERÍODO
    # -----------------------------------------------------
    if any(t in q_norm for t in [
        "ate agora",
        "até agora",
        "ate o momento",
        "até o momento",
        "atualmente",
        "hoje",
        "ate hoje",
        "até hoje",
        "ja",
        "já",
    ]):
        entities["period"] = "current"

    return entities