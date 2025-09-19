from __future__ import annotations
import argparse, json, sys
from pathlib import Path

# Allow import from phase0_backend/*
THIS = Path(__file__).resolve()
PKG_ROOT = THIS.parents[1]
if str(PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(PKG_ROOT))

from marketplace.brickowl_client import BrickOwlClient

def iter_records(jsonl_path: Path):
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-jsonl", required=True)
    ap.add_argument("--templates", required=True)
    args = ap.parse_args()

    in_path = Path(args.in_jsonl)
    tpl_path = Path(args.templates)

    client = BrickOwlClient(str(tpl_path))
    print(f"[info] templates: {tpl_path}")
    print(f"[info] BRICKOWL key detected: {'yes' if client.api_key else 'no'}")

    # Find first record with a BrickOwl boid
    chosen = None
    for rec in iter_records(in_path):
        boid = (rec.get("marketplaces") or {}).get("brickowl", {}).get("boid")
        if boid:
            chosen = (rec.get("id"), boid)
            break

    if not chosen:
        print("[warn] No record with marketplaces.brickowl.boid found.")
        sys.exit(2)

    rec_id, boid = chosen
    print(f"[info] testing rec_id={rec_id} boid={boid}")

    snap = client.fetch_part_snapshot(boid=boid)
    if not snap:
        print("[warn] No snapshot returned (API may require additional access or different endpoint).")
        sys.exit(0)

    # Only print normalized commerce snippet (prices + availability)
    out = {
        "id": rec_id,
        "boid": boid,
        "commerce": {
            "prices": snap.get("prices"),
            "availability": snap.get("availability"),
        }
    }
    print(json.dumps(out, indent=2))
    return 0

if __name__ == "__main__":
    sys.exit(main())
