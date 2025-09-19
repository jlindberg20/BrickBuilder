from __future__ import annotations
from typing import Optional, Dict, Any, Set

from .util import load_yaml, get_secret, build_request_from_template, http_json, now_iso

class BrickOwlClient:
    def __init__(self, templates_path: str, timeout: float = 5.0):
        self.tpls = load_yaml(templates_path)["templates"]
        self.api_key = get_secret("BRICKOWL_API_KEY")
        self.timeout = timeout

    def _call(self, tpl_key: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        tpl = self.tpls[tpl_key]
        method, url, headers, query, auth = build_request_from_template(tpl, params, self.api_key)
        try:
            data, _ = http_json(method, url, headers, query, auth, timeout=self.timeout)
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    def fetch_part_snapshot(self, *, boid: str, country: str = "US") -> Optional[Dict[str, Any]]:
        if not boid or not self.api_key:
            return None

        prices = []
        sellers_count = None
        in_stock = None

        # 1) Try aggregated availability (preferred)
        avail = self._call("bo:catalog_availability:1", {"boid": boid, "country": country})
        if avail:
            # The exact shape can vary by access; use defensive extraction.
            # Common patterns: cheapest_* fields or arrays with offers.
            # We'll look for min/avg/max-ish hints and a sellers/available count.
            for k in ("cheapest_usd", "cheapest_gbp", "cheapest_eur"):
                if k in avail and isinstance(avail[k], (int, float)):
                    prices.append({"market":"bo","currency":k.split("_")[1].upper(),"amount":float(avail[k]),"updated_at":now_iso(),"amount_type":"min"})

            if "lowest_price" in avail and isinstance(avail["lowest_price"], (int,float)):
                prices.append({"market":"bo","currency":"USD","amount":float(avail["lowest_price"]), "updated_at": now_iso(), "amount_type":"min"})

            # seller/availability clues
            if "sellers" in avail and isinstance(avail["sellers"], int):
                sellers_count = avail["sellers"]
            if "available" in avail and isinstance(avail["available"], int):
                sellers_count = max(sellers_count or 0, avail["available"])
            if sellers_count is not None:
                in_stock = sellers_count > 0

        # 2) Fallback: inventory listing → count unique stores
        if sellers_count is None or in_stock is None:
            inv = self._call("bo:catalog_inventory:1", {"boid": boid})
            if inv:
                # inventory may be a dict with a 'lots' array; count unique store ids if present
                lots = inv.get("lots") if isinstance(inv.get("lots"), list) else []
                stores: Set[str] = set()
                for lot in lots:
                    sid = lot.get("store_id") or lot.get("store") or lot.get("seller_id")
                    if sid:
                        stores.add(str(sid))
                    # capture visible prices if present
                    p = lot.get("price") or lot.get("unit_price")
                    c = lot.get("currency") or "USD"
                    try:
                        if p is not None:
                            prices.append({"market":"bo","currency":str(c).upper(),"amount":float(p),"updated_at":now_iso(),"amount_type":"offer"})
                    except Exception:
                        pass
                sellers_count = len(stores) if stores else (len(lots) if lots else None)
                in_stock = (sellers_count or 0) > 0

        if not prices and sellers_count is None and in_stock is None:
            return None

        # Build normalized payload for our schema’s commerce.*
        payload: Dict[str, Any] = {}
        if prices:
            # filter out any None and keep first ~10 entries to avoid bloat
            payload["prices"] = [p for p in prices[:10] if p and p.get("amount") is not None]
        if sellers_count is not None or in_stock is not None:
            payload["availability"] = {
                "bo": {
                    "in_stock": bool(in_stock),
                    "sellers": sellers_count,
                    "last_seen": now_iso()
                }
            }
        return payload
