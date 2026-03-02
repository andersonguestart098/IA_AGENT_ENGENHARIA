from typing import Dict, Any, List


def rows_to_structured(sheet_name: str, values: List[List[Any]]):
    if not values:
        return None

    header = values[0]
    rows = []

    for row in values[1:]:
        obj = {}
        for i, col in enumerate(header):
            if i < len(row):
                obj[str(col)] = row[i]
            else:
                obj[str(col)] = None
        rows.append(obj)

    return {
        "sheet": sheet_name,
        "header": header,
        "rows": rows,
        "row_count": len(rows),
    }