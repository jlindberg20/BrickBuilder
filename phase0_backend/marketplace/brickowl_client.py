# phase0_backend/marketplace/brickowl_client.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Any
import os
import time
import random
import requests


BRICKOWL_BASE = "https://api.brickowl.com/v1"


# ---------- Exceptions ----------
class BrickOwlError(RuntimeError):
    pass


# ---------- HTTP helpers ----------
def _get_api_key() -> str:
    key = os.getenv("BRICKOWL_API_KEY", "").strip()
    if not key:
        raise BrickOwlError("Missing BRICKOWL_API_KEY in environment")
    return key


def _rate_sleep(attempt: int) -> None:
    """Exponential backoff with jitter, capped to 2s."""
    time.sleep(min(2.0, 0.2 * (2 ** attempt) + random.random() * 0.2))


def _bo_get(endpoint: str, params: Dict[str, object], timeout: int = 20) -> Dict[str, Any]:
    """
    GET wrapper with:
    - key injection
    - clear handling of 401/403 (often catalog permission issues)
    - retry on 429/5xx
    - tolerant JSON parsing (wraps non-dict responses as {'_raw': ...})
    """
    key = _get_api_key()
    q = {"key": key, **{k: v for (k, v) in params.items() if v is not None}}
    url = f"{BRICKOWL_BASE}/{endpoint}"
    attempt = 0
    while True:
        resp = requests.get(url, params=q, timeout=timeout)
        # Permission/auth issues come back as 401/403
        if resp.status_code in (401, 403):
            try:
                body = resp.json()
            except Exception:
                body = resp.text
            hint = " (Catalog API likely not enabled for this key)" if endpoint.startswith("catalog/") else ""
            raise BrickOwlError(f"{resp.status_code} on {endpoint}{hint}: {body!r}")

        # Retry on rate limit / transient server errors
        if resp.status_code == 429 or resp.status_code >= 500:
            attempt += 1
            if attempt > 6:
                resp.raise_for_status()
            _rate_sleep(attempt)
            continue

        # Other HTTP errors
        resp.raise_for_status()

        # Parse JSON; tolerate non-JSON
        try:
            data = resp.json()
        except Exception:
            return {"_raw": resp.text}

        # Some endpoints may return top-level list/str; wrap to avoid .get crashes
        if not isinstance(data, dict):
            return {"_raw": data}

        return data


# ---------- Data models ----------
@dataclass
class BOIdCandidate:
    boid: str
    confidence: float
    reason: str  # e.g., 'design_id'


@dataclass
class BOItem:
    boid: str
    name: str
    category: Optional[str]
    url: Optional[str]
    colors: Optional[List[Dict]]  # shape varies; keep as best-effort list


# ---------- Client ----------
class BrickOwlClient:
    """
    Minimal client for the endpoints we need:
    - catalog/id_lookup (mapping external IDs -> BOIDs)
    - catalog/lookup (details by BOID)
    - catalog/availability (optional)
    - catalog/search (optional)
    """

    def __init__(self, country: str = "US"):
        self.country = country

    # ---- Core lookups ----
    def id_lookup(self, part_num: str, id_type: str = "design_id", type_: str = "Part") -> List[BOIdCandidate]:
        """
        BrickOwl returns {"boids": [...]}
          - Often a list of strings like ["771344-79", "771344", ...]
          - Occasionally dicts with {boid, id, id_type}
        Normalize to BOIdCandidate with strong confidence for the requested id_type.
        """
        data = _bo_get("catalog/id_lookup", {"id": part_num, "type": type_, "id_type": id_type})
        if "_raw" in data:
            # Non-standard/empty payload; bubble up a clear error
            raise BrickOwlError(f"id_lookup unexpected payload for {part_num}/{id_type}: {data['_raw']!r}")

        raw = data.get("boids") or []
        out: List[BOIdCandidate] = []
        for entry in raw:
            if isinstance(entry, str):
                boid = entry.strip()
            elif isinstance(entry, dict):
                boid = str(entry.get("boid", "")).strip()
            else:
                continue
            if not boid:
                continue
            out.append(BOIdCandidate(boid=boid, confidence=1.0, reason=id_type))
        return out

    def lookup(self, boid: str) -> Optional[BOItem]:
        det = _bo_get("catalog/lookup", {"boid": boid})
        if "_raw" in det:
            # Some BOIDs may return minimal payloads; treat as missing rather than crashing
            raise BrickOwlError(f"lookup unexpected payload for {boid}: {det['_raw']!r}")

        name = (det.get("name") or "").strip()
        url = (det.get("url") or "").strip() or (det.get("product_url") or "").strip() or None
        category = (det.get("category_name") or det.get("category") or "").strip() or None
        colors = det.get("colors") if isinstance(det.get("colors"), list) else None
        return BOItem(boid=boid, name=name, category=category, url=url, colors=colors)

    # ---- Optional endpoints ----
    def availability(self, boid: str, country: Optional[str] = None) -> Dict[str, Any]:
        ctry = country or self.country
        return _bo_get("catalog/availability", {"boid": boid, "country": ctry})

    def search(self, query: str, type_: str = "Part", page: int = 1, per_page: int = 10) -> Dict[str, Any]:
        return _bo_get("catalog/search", {"query": query, "type": type_, "page": page, "per_page": per_page})

    # ---- Convenience: resolve a BOID from a typical LEGO number ----
    def resolve_boid(self, part_num: str) -> Optional[str]:
        """
        Strategy:
          1) Prefer design_id mapping (best for LEGO numbers like 3001, 3023, etc.).
          2) If multiple BOIDs are returned, prefer the base BOID without a dash.
          3) If only dashed BOIDs exist, strip the dash suffix and try the base.
          4) If base lookup fails later, callers can retry with a full dashed BOID.
        """
        # 1) Primary route: design_id
        try:
            cands = self.id_lookup(part_num, id_type="design_id", type_="Part")
            if cands:
                # Prefer a base (no '-') if present
                base = [c for c in cands if "-" not in c.boid]
                if base:
                    return base[0].boid
                # Otherwise strip the first '-' suffix
                return cands[0].boid.split("-", 1)[0]
        except BrickOwlError:
            # If design_id fails due to permissions/shape, we just return None
            pass
        return None
