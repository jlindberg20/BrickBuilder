import json
import re
from pathlib import Path

def _stem(s: str) -> str:
    s = s.strip().lower()
    if s.endswith(".dat") or s.endswith(".ldr"):
        s = s.rsplit(".", 1)[0]
    return s

def _maybe_digits_stem(s: str) -> str | None:
    """If filename stem is purely digits (e.g., '3001'), return it as a likely RB part_num."""
    m = re.fullmatch(r"\d+", s)
    return m.group(0) if m else None

def load_ldraw_to_rb_map(crosswalk_path: str) -> dict[str, str]:
    """
    Build a mapping { ldraw_stem -> rb_part_num } from a tolerant read of the crosswalk JSONL.
    We look for various possible shapes used in earlier runs.
    """
    p = Path(crosswalk_path)
    if not p.exists():
        return {}

    mapping: dict[str, str] = {}
    with p.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue

            # Try to extract rb_part_num
            rb_part_num = None
            src = obj.get("source_ids") or {}
            if isinstance(src, dict):
                rb = src.get("rb") or {}
                if isinstance(rb, dict):
                    rb_part_num = rb.get("part_num")
            if not rb_part_num:
                rb_part_num = obj.get("rb_part_num") or (obj.get("rebrickable") or {}).get("part_num")

            if not rb_part_num:
                continue

            # Collect possible ldraw identifiers
            ldraw_ids = set()
            # 1) source_ids.ldraw could be list or dict
            ldraw_src = src.get("ldraw")
            if isinstance(ldraw_src, list):
                ldraw_ids.update([str(x) for x in ldraw_src])
            elif isinstance(ldraw_src, dict):
                for key in ("ids", "files", "id", "file", "ldraw_ids"):
                    v = ldraw_src.get(key)
                    if isinstance(v, list):
                        ldraw_ids.update([str(x) for x in v])
                    elif isinstance(v, str):
                        ldraw_ids.add(v)
            # 2) flat keys
            for k in ("ldraw", "ldraw_id", "ldraw_ids"):
                v = obj.get(k)
                if isinstance(v, list):
                    ldraw_ids.update([str(x) for x in v])
                elif isinstance(v, str):
                    ldraw_ids.add(v)

            # Normalize and record
            for raw in ldraw_ids:
                s = _stem(raw)
                if s:
                    mapping.setdefault(s, rb_part_num)

    return mapping

def resolve_rb_part_num(subfile: str, mapping: dict[str,str]) -> str | None:
    """
    Resolve an LDraw subfile name to an RB part_num using the mapping.
    Falls back to numeric stems (e.g., 3001.dat -> '3001') when sensible.
    """
    s = _stem(subfile)
    if not s:
        return None
    if s in mapping:
        return mapping[s]
    guess = _maybe_digits_stem(s)
    if guess:
        return guess
    return None
