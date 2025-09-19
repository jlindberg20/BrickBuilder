from __future__ import annotations
import argparse, json, sys
from pathlib import Path
from typing import Dict, Any, List

# import from phase0_backend/*
THIS = Path(__file__).resolve()
PKG_ROOT = THIS.parents[1]
if str(PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(PKG_ROOT))

from marketplace.brickowl_client import BrickOwlClient

def iter_records(p: Path):
    with p.open('r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                yield json.loads(line)

def merge_commerce(rec: Dict[str, Any], add_prices: List[Dict[str,Any]]|None, add_avail: Dict[str,Any]|None):
    if not add_prices and not add_avail:
        return rec
    com = rec.get('commerce') or {}
    # merge prices
    if add_prices:
        base = com.get('prices') or []
        merged = [p for p in base if p and p.get('market')]
        for p in add_prices:
            if not p:
                continue
            key = (p.get('market'), p.get('currency'), p.get('amount'), p.get('amount_type'))
            if not any((q.get('market'), q.get('currency'), q.get('amount'), q.get('amount_type')) == key for q in merged):
                merged.append(p)
        com['prices'] = merged
    # merge availability
    if add_avail:
        base_av = com.get('availability') or {}
        for k,v in add_avail.items():
            base_av[k] = v
        com['availability'] = base_av
    rec['commerce'] = com
    return rec

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--in-jsonl', required=True)
    ap.add_argument('--out-jsonl', required=True)
    ap.add_argument('--templates', required=True)
    ap.add_argument('--limit', type=int, default=200)
    ap.add_argument('--bo-live', action='store_true', help='Attempt live BrickOwl Catalog calls')
    ap.add_argument('--bo-timeout', type=float, default=3.0, help='HTTP timeout seconds for BrickOwl calls')
    args = ap.parse_args()

    try:
        from tqdm import tqdm  # progress bar for console/PowerShell
    except Exception:
        tqdm = None

    inp = Path(args.in_jsonl)
    outp = Path(args.out_jsonl)

    bo_client = BrickOwlClient(args.templates, timeout=args.bo_timeout) if args.bo_live else None

    processed = 0
    enriched = 0

    # First pass to count (for nicer progress bar); fall back if large file
    try:
        total_est = sum(1 for _ in inp.open('r', encoding='utf-8'))
        if args.limit:
            total_est = min(total_est, args.limit)
    except Exception:
        total_est = args.limit or None

    progress_iter = iter_records(inp)
    if tqdm:
        progress_iter = tqdm(progress_iter, total=total_est, unit='rec', dynamic_ncols=True)

    with outp.open('w', encoding='utf-8', newline='') as fout:
        for rec in progress_iter:
            if args.limit and processed >= args.limit:
                break
            processed += 1

            add_prices = None
            add_avail = None

            boid = (rec.get('marketplaces') or {}).get('brickowl', {}).get('boid')

            if bo_client and boid:
                snap = bo_client.fetch_part_snapshot(boid=boid)
                if snap:
                    add_prices = [p for p in (snap.get('prices') or []) if p and p.get('amount') is not None]
                    add_avail = snap.get('availability')
                    if add_prices or add_avail:
                        enriched += 1

            rec = merge_commerce(rec, add_prices, add_avail)

            # IMPORTANT: write a REAL newline, not a literal '\n'
            fout.write(json.dumps(rec, ensure_ascii=False))
            fout.write('\n')

            if tqdm:
                # update status in the bar
                progress_iter.set_postfix_str(f"enriched={enriched}")

    print(json.dumps({
        'processed': processed,
        'enriched': enriched,
        'out_file': str(outp)
    }, indent=2))

if __name__ == '__main__':
    sys.exit(main())
