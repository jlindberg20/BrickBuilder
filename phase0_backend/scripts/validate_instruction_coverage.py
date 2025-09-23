#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, json, sys
from pathlib import Path
from collections import Counter

def norm(d,*ks):
    cur=d
    for k in ks:
        if not isinstance(cur, dict): return None
        cur=cur.get(k)
    return cur

def bucket(n):
    if n is None: return "unknown"
    for a,b in [(0,50),(51,100),(101,200),(201,400),(401,800)]:
        if a <= n <= b: return f"{a}-{b}"
    return "801+"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inn", required=True, help="models JSONL produced by harvester (with pages)")
    ap.add_argument("--top", type=int, default=15, help="Top-N by piece_count with verified pages")
    ap.add_argument("--progress_every", type=int, default=250, help="Emit a progress tick every N lines to stderr")
    args = ap.parse_args()

    p = Path(args.inn)
    if not p.exists():
        print(f"ERROR: not found: {p}", file=sys.stderr); return 2

    # First pass: count total for accurate progress
    total = sum(1 for _ in p.open("r", encoding="utf-8-sig"))

    with_pdf = with_pages = 0
    bom_bins = Counter()
    tops = []

    # Second pass: parse + progress
    i = 0
    import json as _json
    with p.open("r", encoding="utf-8-sig") as f:
        for ln in f:
            if not ln.strip(): 
                continue
            i += 1
            if args.progress_every and (i % args.progress_every == 0 or i == total):
                # Machine-parsable progress line to stderr
                sys.stderr.write(f"PROGRESS {i} {total}\n")
                sys.stderr.flush()
            rec = _json.loads(ln)
            links = (norm(rec,"instructions","links") or [])
            if any((isinstance(l,dict) and ((l.get("kind")=="pdf") or str(l.get("url","")).lower().endswith(".pdf"))) for l in links):
                with_pdf += 1
            pages = (norm(rec,"instructions","pages") or {})
            if (pages.get("count") or 0) > 0:
                with_pages += 1
                bom = rec.get("bom") or []
                bom_bins[bucket(len(bom))] += 1
                piece_count = norm(rec,"metadata","piece_count") or norm(rec,"meta","piece_count") or 0
                try:
                    piece_count = int(piece_count)
                except Exception:
                    piece_count = 0
                set_num = norm(rec,"source_ids","rb","set_num") or rec.get("id")
                name = rec.get("name") or norm(rec,"metadata","name")
                tops.append((piece_count, set_num, name, pages.get("count")))
