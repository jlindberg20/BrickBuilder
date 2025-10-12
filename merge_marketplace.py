import argparse, json, os, sys, time, shutil
from datetime import datetime

def load_marketplace(market_path):
    by_rb = {}
    by_bl = {}
    total = 0
    with open(market_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            total += 1
            o = json.loads(line)
            rb = o.get("rb_part_num")
            bl = o.get("bricklink") or {}
            bl_no = (bl.get("part_no") or "").strip()
            if rb:
                by_rb[str(rb)] = bl
            if bl_no:
                by_bl[bl_no] = bl
    return by_rb, by_bl, total

def count_lines(path):
    n = 0
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for _ in f: n += 1
    return n

def show_progress(done, total, label="Merging"):
    pct = (done/total*100.0) if total else 100.0
    bar_len = 30
    filled = int(bar_len * pct/100.0)
    bar = "#" * filled + "-" * (bar_len - filled)
    msg = f"\r{label} [{bar}] {done}/{total} ({pct:5.1f}%)"
    sys.stdout.write(msg)
    sys.stdout.flush()

def merge(master_path, market_path, backup_dir):
    # 1) Backup master
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    bak_name = f"master_parts.BAK_{ts}.jsonl"
    bak_path = os.path.join(backup_dir, bak_name)
    shutil.copy2(master_path, bak_path)

    # 2) Load marketplace maps
    by_rb, by_bl, market_total = load_marketplace(market_path)
    print(f"Loaded marketplace rows: {market_total}  (rb_map={len(by_rb)}  bl_map={len(by_bl)})")

    # 3) First pass: count master lines for progress
    total_master = count_lines(master_path)
    print(f"Master lines to process: {total_master}")

    # 4) Merge
    tmp_out = master_path + ".tmp"
    matched = 0
    unchanged = 0
    written = 0
    start = time.time()
    with open(master_path, "r", encoding="utf-8", errors="replace") as fin, \
         open(tmp_out,    "w", encoding="utf-8", newline="\n") as fout:
        i = 0
        for line in fin:
            if not line.strip():
                continue
            i += 1
            o = json.loads(line)

            # RB part number from master
            rb = None
            sid = o.get("source_ids") or {}
            rb_node = sid.get("rb") or {}
            rb = rb_node.get("part_num")
            if not rb:
                # sometimes master may carry 'rb_part_num'
                rb = o.get("rb_part_num")

            # BL id from master (fallback)
            bl_id = None
            mp = o.get("marketplaces") or {}
            bl_mp = mp.get("bricklink") or {}
            bl_id = bl_mp.get("part_id")

            bl_data = None
            if rb and str(rb) in by_rb:
                bl_data = by_rb[str(rb)]
            elif bl_id and str(bl_id) in by_bl:
                bl_data = by_bl[str(bl_id)]

            if bl_data is not None:
                o["bricklink"] = bl_data
                matched += 1
            else:
                unchanged += 1

            fout.write(json.dumps(o, ensure_ascii=False, separators=(",",":")))
            fout.write("\n")
            written += 1

            # progress
            if i % 200 == 0 or i == total_master:
                show_progress(i, total_master)

    # finalize progress line
    show_progress(total_master, total_master)
    sys.stdout.write("\n")

    # 5) Atomic replace
    os.replace(tmp_out, master_path)
    dur = time.time() - start
    print(f"Backup  -> {bak_path}")
    print(f"Merged  -> {master_path}")
    print(f"Matched -> {matched}   Unchanged -> {unchanged}   Written -> {written}   Time -> {dur:0.1f}s")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True, help="Path to root master_parts.jsonl")
    ap.add_argument("--market", required=True, help="Path to master_parts.marketplace.jsonl")
    ap.add_argument("--backup-dir", required=True, help="Directory to store backup")
    args = ap.parse_args()

    # basic checks
    for p in [args.master, args.market]:
        if not os.path.exists(p):
            print(f"ERROR: Not found -> {p}")
            sys.exit(1)
    if not os.path.isdir(args.backup_dir):
        os.makedirs(args.backup_dir, exist_ok=True)

    merge(args.master, args.market, args.backup_dir)

if __name__ == "__main__":
    main()
