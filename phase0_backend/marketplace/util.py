from __future__ import annotations
import datetime as dt
import os
from typing import Dict, Any, Tuple, Optional

import requests
import yaml

try:
    from dotenv import load_dotenv
    load_dotenv()  # loads .env into process env
except Exception:
    # If python-dotenv is not installed, env-only mode still works
    pass

def now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def get_secret(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.environ.get(name, default)

def build_request_from_template(tpl: Dict[str, Any], params: Dict[str, Any], api_key: Optional[str]):
    method = tpl.get("method", "GET").upper()
    url_template = tpl["url"]

    # path fill
    path_params = {k: v for k, v in params.items() if "{" + k + "}" in url_template}
    url = url_template.format(**path_params)

    # query params are everything else not consumed in the path
    query = {k: v for k, v in params.items() if "{" + k + "}" not in url_template and v is not None}

    # auth
    auth_cfg = (tpl.get("auth") or {})
    headers: Dict[str, str] = {}
    auth_obj = None

    if auth_cfg.get("type") == "api_key":
        loc = auth_cfg.get("location", "query")
        name = auth_cfg.get("name", "key")
        prefix = auth_cfg.get("prefix", "")
        if api_key:
            if loc == "query":
                query[name] = api_key
            elif loc == "header":
                headers[name] = f"{prefix}{api_key}".strip()

    # extensible to oauth1/2 later if needed
    return method, url, headers, query, auth_obj

def http_json(method: str, url: str, headers: Dict[str, str], params: Dict[str, Any], auth):
    resp = requests.request(method, url, headers=headers, params=params, auth=auth, timeout=15)
    resp.raise_for_status()
    return resp.json(), resp
