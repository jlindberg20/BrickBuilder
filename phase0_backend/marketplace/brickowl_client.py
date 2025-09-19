from __future__ import annotations
from typing import Optional, Dict, Any

from .util import load_yaml, get_secret, build_request_from_template, http_json, now_iso

class BrickOwlClient:
    def __init__(self, templates_path: str):
        self.tpls = load_yaml(templates_path)["templates"]
        self.api_key = get_secret("BRICKOWL_API_KEY")

    def fetch_part_snapshot(self, *, boid: str) -> Optional[Dict[str, Any]]:
        if not boid or not self.api_key:
            return None
        tpl = self.tpls["bo:part_prices:1"]
        params = {"part_id": boid, "condition": "all"}
        method, url, headers, query, auth = build_request_from_template(tpl, params, self.api_key)

        try:
            data, _ = http_json(method, url, headers, query, auth)
        except Exception:
            return None

        # Normalize conservatively; adjust parsing once BO response shape is confirmed.
        price_min = None
        price_max = None
        avg_price = None
        num_sellers = None

        if isinstance(data, dict):
            # try common keys if present
            price_min = data.get("min_price") or data.get("price_min")
            price_max = data.get("max_price") or data.get("price_max")
            avg_price = data.get("avg_price")
            num_sellers = data.get("num_sellers") or data.get("sellers")

        return {
            "prices": [
                # store avg as the normalized price point when available
                {"market": "bo", "currency": "USD", "amount": avg_price, "updated_at": now_iso()} if avg_price is not None else None,
                # optionally store min/max as additional entries (same schema allows extra properties)
                {"market": "bo", "currency": "USD", "amount": price_min, "updated_at": now_iso(), "amount_type": "min"} if price_min is not None else None,
                {"market": "bo", "currency": "USD", "amount": price_max, "updated_at": now_iso(), "amount_type": "max"} if price_max is not None else None
            ],
            "availability": {
                "bo": {
                    "in_stock": (num_sellers or 0) > 0,
                    "sellers": num_sellers,
                    "last_seen": now_iso()
                }
            }
        }
