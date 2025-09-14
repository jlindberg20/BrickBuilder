# phase0_backend/marketplace/bricklink_client.py
from __future__ import annotations
from pathlib import Path
from typing import Dict, Iterable, Optional, List
import csv
import json

from ..utils.normalization import stem_bl, tokens

# Expected local files (any subset is OK; missing files are handled gracefully):
# data/raw/bricklink/parts.csv               (part_num,name,category_id,category_name)
# data/raw/bricklink/colors.csv              (color_id,color_name,rgb_hex?)
# data/raw/bricklink/part_colors.csv         (part_num,color_id)  # optional
# data/raw/bricklink/aliases.csv             (part_num,alias)     # optional
# data/raw/bricklink/urls.csv                (part_num,catalog_url)  # optional
# data/raw/bricklink/parts.jsonl             (one record per line; optional alt source)
#
# We normalize to a minimal record shape to keep the merger simple.

Row = Dict[str, object]

def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))

def _read_jsonl_rows(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    out = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out

class BrickLinkClient:
    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)

        # Load auxiliary tables (best-effort)
        self._parts_csv = _read_csv_rows(self.data_dir / "parts.csv")
        self._parts_jsonl = _read_jsonl_rows(self.data_dir / "parts.jsonl")
        self._colors = {r.get("color_id"): r for r in _read_csv_rows(self.data_dir / "colors.csv")}
        self._part_colors = _read_csv_rows(self.data_dir / "part_colors.csv")
        self._aliases = _read_csv_rows(self.data_dir / "aliases.csv")
        self._urls = {r.get("part_num"): r.get("catalog_url") for r in _read_csv_rows(self.data_dir / "urls.csv")}

        # Build quick color index per part if available
        self._colors_by_part = {}
        if self._part_colors:
            for r in self._part_colors:
                pn = (r.get("part_num") or "").strip()
                cid = (r.get("color_id") or "").strip()
                if pn and cid:
                    self._colors_by_part.setdefault(pn, set()).add(cid)

        # Build alias index
        self._aliases_map = {}
        for r in self._aliases:
            pn = (r.get("part_num") or "").strip()
            alias = (r.get("alias") or "").strip()
            if pn and alias:
                self._aliases_map.setdefault(pn, set()).add(alias)

        # Unify “parts” inputs into a single list of dicts with consistent keys
        self._parts = []
        if self._parts_csv:
            for r in self._parts_csv:
                self._parts.append({
                    "part_num": (r.get("part_num") or "").strip(),
                    "name": (r.get("name") or "").strip(),
                    "category_id": r.get("category_id"),
                    "category_name": r.get("category_name"),
                })
        if self._parts_jsonl:
            # JSONL may already be normalized; fill gaps only
            for r in self._parts_jsonl:
                pn = (r.get("part_num") or "").strip()
                if not pn:
                    continue
                self._parts.append({
                    "part_num": pn,
                    "name": (r.get("name") or "").strip(),
                    "category_id": r.get("category_id"),
                    "category_name": r.get("category_name"),
                })

    def _colors_for_part(self, pn: str):
        out = []
        for cid in sorted(self._colors_by_part.get(pn, []), key=lambda x: int(x) if str(x).isdigit() else 1e9):
            c = self._colors.get(cid) or {}
            out.append({
                "id": cid,
                "name": c.get("color_name"),
                "rgb_hex": c.get("rgb_hex"),
            })
        return out

    def _url_for_part(self, pn: str) -> Optional[str]:
        # Prefer provided urls.csv; fallback to a deterministic BL catalog URL
        url = self._urls.get(pn)
        if url:
            return url
        # BrickLink catalog pattern for parts (P=)
        return f"https://www.bricklink.com/v2/catalog/catalogitem.page?P={pn}"

    def iter_parts(self) -> Iterable[Row]:
        """
        Yield normalized BrickLink records:
        {
          "bl_part_num": str,
          "name": str,
          "category": {"id": ..., "name": ...},
          "colors": [{"id": "...", "name": "...", "rgb_hex": "..."}],
          "aliases": ["..."],
          "urls": {"catalog": "..."},
          "norm_key": str,
          "name_tokens": ["...", ...]
        }
        """
        seen = set()
        for r in self._parts:
            pn = (r.get("part_num") or "").strip()
            if not pn or pn in seen:
                continue
            seen.add(pn)
            yield {
                "bl_part_num": pn,
                "name": (r.get("name") or "").strip(),
                "category": {
                    "id": r.get("category_id"),
                    "name": r.get("category_name"),
                },
                "colors": self._colors_for_part(pn),
                "aliases": sorted(self._aliases_map.get(pn, [])),
                "urls": {
                    "catalog": self._url_for_part(pn),
                },
                "norm_key": stem_bl(pn),
                "name_tokens": tokens(r.get("name") or ""),
            }

    def lookup_part(self, bl_part_num: str) -> Optional[Row]:
        key = bl_part_num.strip()
        for rec in self.iter_parts():
            if rec["bl_part_num"] == key:
                return rec
        return None
