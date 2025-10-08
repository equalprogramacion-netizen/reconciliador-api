from __future__ import annotations
import json
from typing import Any, Dict

def _safe_json(r) -> Dict[str, Any]:
    """
    Devuelve un dict “seguro” desde una Response (httpx/requests).
    Si .json() falla, intenta con .text; si no, {}.
    """
    try:
        return r.json()
    except Exception:
        try:
            t = getattr(r, "text", "") or ""
            t = t.strip()
            if not t:
                return {}
            return json.loads(t)
        except Exception:
            return {}
