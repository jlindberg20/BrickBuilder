from __future__ import annotations
import json
import re
from pathlib import Path
from typing import Dict, Iterable

# Inputs / outputs (paths align to your repo tree)
# Rebrickable canonical JSONL is here:
#   data/processed/rebrickable/parts_unified.jsonl
# LDraw index produced in the previous step is here:
#   data/raw/ldraw/_index/ldraw_index.json
# Output will be written alongside as:
#   data/processed/rebrickable/parts_with_ldraw.jsonl

RB_IN = Path("data/processed/rebrickable/parts_unified.jsonl")
LDRAW_INDEX_JSON = Path("data/raw/ldraw/_index/ldraw_index.json")
RB_OUT = Path("data/processed/rebrickable/parts_with_ldraw.jsonl")

# --- Helpers -----------------------------------------------------------------

def jsonl_reader(path: Path) -> Iterable[dict]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def jsonl_writer(path: Path):
    class _W:
        def __init__(self, p: Path):
            self.f = open(p, "w", encoding="utf-8")
        def write(self, obj: dict):
            self.f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        def close(self):
            self.f.close()
    return _W(path)

def load_ldraw_index() -> Dict[str, dict]:
    with open(LDRAW_INDEX_JSON, "r", encoding="utf-8") as f:
        idx = json.load(f)
    # keys are already lowercase stems; normalize just in case
    return {k.lower(): v for k, v in idx.items()}

# Normalization of RB part ids (e.g., "14769pr1235" -> base "14769")
_SUFFIX_RX = re.compile(r'(c\d+|h\d+|pr\d+|ps\d+|pat\d+|cpat\d+|d\d+)$', re.IGNORECASE)

def base_core(part_num: str) -> str:
    s = part_num.strip().lower()
    return _SUFFIX_RX.sub("", s)

def alt_forms(s: str):
    # try simple forms; our index keys are stems without extension
    yield s
    yield s.replace('-', '_')
    yield s.replace('_', '-')
    yield re.sub(r'[^a-z0-9]', '', s)

def try_exact_lookup(part_num: str, ldraw_idx: Dict[str, dict]):
    for form in alt_forms(part_num.lower()):
        if form in ldraw_idx:
            return ldraw_idx[form], 1.0, "exact_or_alt"
    return None, 0.0, None

def try_parent_geometry(rb_obj: dict, ldraw_idx: Dict[str, dict]):
    # Use RB relationships to find a base/parent part that likely carries geometry
    for rel in rb_obj.get("relationships", []):
        if rel.get("rel_type") in ("P", "M"):  # Printed/Modified from parent
            parent_id = str(rel.get("parent")).lower()
            m, conf, reason = try_exact_lookup(parent_id, ldraw_idx)
            if m:
                return m, 0.90, "parent_geometry"
    # Fallback to stripping common suffixes to find the base core id
    core = base_core(rb_obj.get("source_ids", {}).get("rb", {}).get("part_num", ""))
    if core and core != rb_obj["source_ids"]["rb"]["part_num"].lower():
        m, conf, reason = try_exact_lookup(core, ldraw_idx)
        if m:
            return m, 0.85, "base_core"
    return None, 0.0, None

def attach_ldraw_fields(obj: dict, match: dict | None, reason: str | None, conf: float | None):
    geom = obj.setdefault("geometry", {}).setdefault("ldraw", {})
    if match:
        geom.update({
            "file": match["path"],
            "status": "matched",
            "confidence": conf,
            "match_reason": reason,
            "source": "official" if "/unofficial/" not in match["path"].replace("\\", "/") else "unofficial",
            "scale": {"unit": "LDU", "to_mm": 0.4}
        })
    else:
        geom.update({
            "file": None,
            "status": "unmatched",
            "reason": "no_match"
        })

# --- Main --------------------------------------------------------------------

def main():
    if not RB_IN.exists():
        raise SystemExit(f"Missing input JSONL: {RB_IN.resolve()}")
    if not LDRAW_INDEX_JSON.exists():
        raise SystemExit(f"Missing LDraw index: {LDRAW_INDEX_JSON.resolve()}")

    ldraw_idx = load_ldraw_index()
    writer = jsonl_writer(RB_OUT)

    total = matched = parent_used = base_used = exact_used = 0

    for obj in jsonl_reader(RB_IN):
        total += 1
        rb_id = obj.get("source_ids", {}).get("rb", {}).get("part_num", "")
        # Pass 1: exact or alt
        m, conf, reason = try_exact_lookup(rb_id, ldraw_idx)
        if not m:
            # Pass 2: parent/base
            m, conf, reason = try_parent_geometry(obj, ldraw_idx)

        if m:
            matched += 1
            if reason == "parent_geometry":
                parent_used += 1
            elif reason == "base_core":
                base_used += 1
            elif reason == "exact_or_alt":
                exact_used += 1
            attach_ldraw_fields(obj, m, reason, conf)
        else:
            attach_ldraw_fields(obj, None, None, None)

        writer.write(obj)

    writer.close()
    print(f"Processed: {total}")
    print(f"Matched:   {matched}")
    print(f"  exact_or_alt:   {exact_used}")
    print(f"  parent_geometry:{parent_used}")
    print(f"  base_core:      {base_used}")
    print(f"Output:    {RB_OUT.as_posix()}")

if __name__ == "__main__":
    main()
