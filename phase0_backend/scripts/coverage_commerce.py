from __future__ import annotations
import argparse, json
from pathlib import Path
from collections import Counter, defaultdict

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--in-jsonl', required=True)
    ap.add_argument('--out-json', required=True)
    args = ap.parse_args()

    total = 0
    has_commerce = 0
    has_prices = 0
    has_avail = 0
    price_by_market = Counter()
    avail_by_market = Counter()

    p = Path(args.in_jsonl)
    with p.open('r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            total += 1
            try:
                rec = json.loads(line)
            except Exception:
                continue

            c = rec.get('commerce') or {}
            if c:
                has_commerce += 1

            prices = c.get('prices') or []
            if prices:
                has_prices += 1
                for it in prices:
                    m = (it or {}).get('market')
                    if m:
                        price_by_market[m] += 1

            avail = c.get('availability') or {}
            if avail:
                has_avail += 1
                for m in avail.keys():
                    avail_by_market[m] += 1

    out = {
        "total": total,
        "coverage": {
            "has_commerce": has_commerce / total if total else 0.0,
            "has_prices":   has_prices   / total if total else 0.0,
            "has_availability": has_avail / total if total else 0.0
        },
        "by_market": {
            "prices": dict(price_by_market),
            "availability": dict(avail_by_market)
        }
    }

    Path(args.out_json).write_text(json.dumps(out, indent=2), encoding='utf-8')
    print(json.dumps(out, indent=2))

if __name__ == '__main__':
    main()
