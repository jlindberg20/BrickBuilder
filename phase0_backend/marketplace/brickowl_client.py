# phase0_backend/marketplace/brickowl_client.py

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional
import os, time, random, requests

BRICKOWL_BASE = "https://api.brickowl.com/v1"


def _get_api_key() -> str:
    key = os.getenv("BRICKOWL_API_KEY", "").strip()
    if not key:
        raise RuntimeError("Missing BRICKOWL_API_KEY in environment")
    return key


def _rate_sleep(attempt: int) -> None:
    # Simple exponential backoff with jitter
    time.sleep(min(2.0, 0.2 * (2 ** attempt) + random.random() * 0.2))


def _bo_get(endpoint: str, params: Dict[str, object], timeout: int = 20) -> Dict:
    key = _get_api_key()
    q = {"key": key, **{k: v for (k, v) in params.items() if v is not None}}
    url = f"{BRICKOWL_BASE}/{endpoint}"
    attempt = 0
    while True:
        resp = requests.get(url, params=q, timeout=timeout)
        if resp.status_code == 429 or resp.status_code >= 500:
            attempt += 1
            if attempt > 6:
                resp.raise_for_status()
            _rate_sleep(attempt)
            continue
        resp.raise_for_status()
        return resp.json()


@dataclass
class BOIdCandidate:
    boid: str
    confidence: float
    reason: str


@dataclass
class BOItem:
    boid: str
    name: str
    category: Optional[str]
    url: Optional[str]
    colors: Optional[List[Dict]]  # best-effort; BrickOwl color facets vary


class BrickOwlClient:
    """
    Minimal live client used in crosswalk and merge steps.
    """

    def __init__(self, country: str = "US"):
        self.country = country

    def id_lookup(self, part_num: str, id_type: str = "item_no", type_: str = "Part") -> List[BOIdCandidate]:
        # https://api.brickowl.com/v1/catalog/id_lookup?id=...&type=Part&id_type=item_no
        data = _bo_get("catalog/id_lookup", {"id": part_num, "type": type_, "id_type": id_type})
        out: List[BOIdCandidate] = []
        for row in data.get("boids", []) or []:
            boid = str(row.get("boid", "")).strip()
            reason = row.get("id_type") or id_type
            conf = 1.0 if (row.get("id") or "").strip().lower() == str(part_num).strip().lower() else 0.8
            if boid:
                out.append(BOIdCandidate(boid=boid, confidence=conf, reason=reason))
        return out

    def lookup(self, boid: str) -> Optional[BOItem]:
        det = _bo_get("catalog/lookup", {"boid": boid})
        name = (det.get("name") or "").strip()
        url = (det.get("url") or "").strip() or (det.get("product_url") or "").strip() or None
        category = (det.get("category_name") or det.get("category") or "").strip() or None
        colors = det.get("colors") if isinstance(det.get("colors"), list) else None
        return BOItem(boid=boid, name=name, category=category, url=url, colors=colors)

    def availability(self, boid: str, country: Optional[str] = None) -> Dict:
        # Optional; use later to snapshot price/quantity
        ctry = country or self.country
        return _bo_get("catalog/availability", {"boid": boid, "country": ctry})

    def search(self, query: str, type_: str = "Part", page: int = 1, per_page: int = 10) -> dict:
        # https://api.brickowl.com/v1/catalog/search?query=...&type=Part
        return _bo_get("catalog/search", {
            "query": query,
            "type": type_,
            "page": page,
            "per_page": per_page
        })

    def resolve_boid(self, part_num: str) -> Optional[str]:
        """
        Try several identifier types, then fall back to search.
        Returns BOID or None.
        """
        for id_type in ("design_id", "bricklink_id", "item_no", "ldraw_id"):
            try:
                cands = self.id_lookup(part_num, id_type=id_type, type_="Part")
                if cands:
                    print(f"[debug] {part_num} matched via {id_type}: {cands}")
                    return cands[0].boid
            except Exception as e:
                print(f"[warn] id_lookup failed ({id_type}): {e}")

        try:
            s = self.search(query=part_num, type_="Part", page=1, per_page=10)
            items = s.get("items") or s.get("results") or []
            for it in items:
                boid = str(it.get("boid") or "").strip()
                if boid:
                    print(f"[debug] {part_num} found via search: {it}")
                    return boid
        except Exception as e:
            print(f"[warn] search failed: {e}")
        return None

