from __future__ import annotations

from typing import Any, Dict, Optional

from app.core.mongo import get_db


async def get_drive_file_by_id(file_id: str) -> Optional[Dict[str, Any]]:
    db = get_db()
    return await db["drive_files"].find_one({"file_id": file_id})


async def resolve_obra_context(
    *,
    parent_folder_id: Optional[str],
    parent_folder_name: Optional[str],
    max_depth: int = 5,
) -> Dict[str, Optional[str]]:
    """
    Resolve o contexto da obra subindo a hierarquia das pastas no Mongo.

    Cenário esperado:
      arquivo -> CUSTOS/FATURAMENTO -> OBRA X

    Retorna:
      {
        "obra_name": "OBRA F",
        "obra_folder_id": "...",
        "direct_parent_name": "CUSTOS",
        "direct_parent_id": "..."
      }
    """
    result = {
        "obra_name": None,
        "obra_folder_id": None,
        "direct_parent_name": parent_folder_name,
        "direct_parent_id": parent_folder_id,
    }

    if not parent_folder_id:
        return result

    current_id = parent_folder_id
    current_name = parent_folder_name
    depth = 0

    while current_id and depth < max_depth:
        current = await get_drive_file_by_id(current_id)
        if not current:
            break

        current_name = current.get("name") or current_name

        # Regra principal:
        # se a pasta atual NÃO for uma pasta funcional (CUSTOS/FATURAMENTO/etc),
        # tratamos ela como obra.
        upper_name = (current_name or "").strip().upper()

        functional_folders = {
            "CUSTOS",
            "FATURAMENTO",
            "ORCAMENTO",
            "ORÇAMENTO",
            "MEDIÇÕES",
            "MEDICOES",
            "COMPRAS",
            "CONTRATOS",
            "DOCUMENTOS",
            "FINANCEIRO",
        }

        if upper_name and upper_name not in functional_folders:
            result["obra_name"] = current_name
            result["obra_folder_id"] = current.get("file_id")
            return result

        current_id = current.get("parent_folder_id")
        current_name = current.get("parent_folder_name")
        depth += 1

    return result