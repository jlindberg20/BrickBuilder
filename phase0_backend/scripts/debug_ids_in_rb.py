from __future__ import annotations
import json, re, itertools
from pathlib import Path

p = Path("data/processed/rebrickable/parts_with_mesh_mm.jsonl")

def first_n(n=30):
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            rec = json.loads(line)
            if rec.get("type") != "part": continue
            yield rec

def dig(d, path):
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur: return None
        cur = cur[k]
    return cur

CANDIDATES = [
  # Likely RB design-id style fields
  ["source_ids","rb","part_num"],
  ["rb","part_num"],
  ["rebrickable","part_num"],
  ["part_num"],
  ["design_id"],

  # LDraw paths we often see
  ["source_ids","ldraw","part_id"],
  ["source_ids","ldraw","ids"],
  ["ldraw_id"],
  ["ldraw","id"],
  ["ldraw","ids"],

  # BrickLink paths we often see
  ["source_ids","bl","part_num"],
  ["bricklink","part_num"],
  ["bl","part_num"],

  # Misc possible nestings
  ["ids","rebrickable","part_num"],
  ["ids","ldraw","part_id"],
  ["ids","bricklink","part_num"],
]

def normalize(v):
    if v is None: return None
    if isinstance(v, (int,)): return str(v)
    if isinstance(v, str): return v.strip()
    if isinstance(v, (list, tuple)):
        return [normalize(x) for x in v if normalize(x)]
    return None

def looks_like_design(v):
    return isinstance(v, str) and re.match(r"^[0-9A-Za-z]+$", v) and not re.match(r"^0{2,}\d+$", v)

def looks_like_ldraw(v):
    return isinstance(v, str) and re.match(r"^[0-9A-Za-z_\-\.]+$", v)

def looks_like_bl(v):
    return isinstance(v, str) and re.match(r"^[0-9A-Za-z\-]+$", v)

print("Probing first ~30 part records for candidate ID fields…\n")

count = 0
for rec in first_n(30):
    name = rec.get("name")
    print("—", name)
    found_any = False
    for path in CANDIDATES:
        raw = normalize(dig(rec, path))
        if not raw: continue
        if isinstance(raw, list):
            vals = raw
        else:
            vals = [raw]
        # Heuristic classify & show a few:
        for v in vals[:4]:
            tag = ""
            if looks_like_design(v): tag = "design?"
            if looks_like_ldraw(v):  tag = tag or "ldraw?"
            if looks_like_bl(v):     tag = tag or "bricklink?"
            print("   ", ".".join(path), "=", repr(v), f"({tag})")
            found_any = True
    if not found_any:
        print("    (no candidate fields hit)")
    count += 1
    print()
    if count >= 10:
        break
