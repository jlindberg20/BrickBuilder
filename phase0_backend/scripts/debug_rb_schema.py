# phase0_backend/scripts/debug_rb_schema.py
from __future__ import annotations
import json, argparse
from pathlib import Path
from collections import Counter
import re

CANDIDATE_PATHS = [
    # Most likely:
    ["source_ids","rb","part_num"],
    # Other common shapes we’ve seen:
    ["rb","part_num"],
    ["rebrickable","part_num"],
    ["rb_part_num"],
    ["part_num"],
    ["design_id"],
    ["ids","rebrickable","part_num"],
]

DIGITISH = re.compile(r"^[0-9A-Za-z]+$")  # allow 3001, 6558c01 etc.

def get_path(d, path):
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur: return None
        cur = cur[k]
    return cur

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rb-jsonl", required=True)
    ap.add_argument("--sample", type=int, default=200)
    args = ap.parse_args()

    p = Path(args.rb_jsonl)
    seen = Counter()
    examples = {tuple(path): None for path in CANDIDATE_PATHS}
    bad_examples = []
    total = 0

    with p.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            rec = json.loads(line)
            if rec.get("type") != "part":
                continue
            total += 1
            # probe candidate paths
            hit = False
            for path in CANDIDATE_PATHS:
                v = get_path(rec, path)
                if isinstance(v, (str, int)):
                    s = str(v).strip()
                    if DIGITISH.match(s):
                        seen[tuple(path)] += 1
                        if examples[tuple(path)] is None:
                            examples[tuple(path)] = s
                        hit = True
                        break
            if not hit and len(bad_examples) < 10:
                bad_examples.append({
                    "id": rec.get("id"),
                    "name": rec.get("name"),
                    "keys_top": sorted(list(rec.keys())),
                })
            if total >= args.sample:
                break

    print(f"Scanned {total} part records")
    print("Hits by candidate path:")
    for path, cnt in seen.most_common():
        print(f"  {'.'.join(path)}: {cnt} (e.g. {examples[path]!r})")

    if not seen:
        print("\nNo candidate path matched. First few unmatched examples:")
        for ex in bad_examples:
            print(ex)

if __name__ == "__main__":
    main()
