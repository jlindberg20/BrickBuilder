# phase0_backend/scripts/build_crosswalk_bo.py
from __future__ import annotations
import argparse, json, csv, time, os, re
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple, Set, List

from dotenv import load_dotenv
load_dotenv()

try:
    from tqdm import tqdm
except Exception:
    tqdm = None

from phase0_backend.marketplace.brickowl_client import BrickOwlClient, BrickOwlError

RE_DESIGN = re.compile(r"^[0-9A-Za-z]+$")          # 3001, 3023, 6558c01
RE_LDRAW  = re.compile(r"^[0-9A-Za-z_\-\.]+$")     # 3001.dat, x123c01, s\*.dat, etc.
RE_BL     = re.compile(r"^[0-9A-Za-z\-]+$")        # 3001, 6558c01, u1234-*

CANDIDATE_PATHS = {
    "design": [
        ["source_ids","rb","part_num"],
        ["rb","part_num"],
        ["rebrickable","part_num"],
        ["part_num"],
        ["design_id"],
        ["ids","rebrickable","part_num"],
    ],
    "ldraw": [
        ["source_ids","ldraw","part_id"],
        ["source_ids","ldraw","ids"],
        ["ldraw_id"],
        ["ldraw","id"],
        ["ldraw","ids"],
        ["ids","ldraw","part_id"],
    ],
    "bricklink": [
        ["source_ids","bl","part_num"],
        ["bricklink","part_num"],
        ["bl","part_num"],
        ["ids","bricklink","part_num"],
    ],
}

def dig(d, path):
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur: return None
        cur = cur[k]
    return cur

def norm(v):
    if v is None: return None
    if isinstance(v, (int,)): return str(v)
    if isinstance(v, str): return v.strip()
    if isinstance(v, (list, tuple)):
        out = []
        for x in v:
            sx = norm(x)
            if sx: out.append(sx)
        return out
    return None

def collect_ids(rec: Dict) -> Dict[str, List[Tuple[str, str]]]:
    """
    Returns dict: kind -> list of (value, source_hint)
    kind ∈ {"design","ldraw","bricklink"}
    """
    out = {"design":[], "ldraw":[], "bricklink":[]}
    for kind, paths in CANDIDATE_PATHS.items():
        for path in paths:
            v = norm(dig(rec, path))
            if not v: continue
            if isinstance(v, list):
                vals = v
            else:
                vals = [v]
            for val in vals:
                if kind == "design" and RE_DESIGN.match(val) and not re.match(r"^0{2,}\d+$", val):
                    out["design"].append((val, ".".join(path)))
                elif kind == "ldraw" and RE_LDRAW.match(val):
                    out["ldraw"].append((val, ".".join(path)))
                elif kind == "bricklink" and RE_BL.match(val):
                    out["bricklink"].append((val, ".".join(path)))
    return out

def iter_rb_parts(jsonl_path: Path, limit: Optional[int]) -> Iterable[Dict]:
    n = 0
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            rec = json.loads(line)
            if rec.get("type") != "part": continue
            ids = collect_ids(rec)
            if not any(ids.values()):  # nothing usable
                continue
            name = rec.get("name") or ""
            yield {"name": name, "ids": ids}
            n += 1
            if limit and n >= limit: break

def load_checkpoint(checkpoint_path: Path) -> Set[str]:
    done: Set[str] = set()
    if checkpoint_path.exists():
        with checkpoint_path.open("r", encoding="utf-8") as f:
            for line in f:
                p = line.strip()
                if p: done.add(p)
    return done

def append_checkpoint(checkpoint_path: Path, key: str) -> None:
    with checkpoint_path.open("a", encoding="utf-8") as f:
        f.write(key + "\n")

def open_output_writer(out_csv: Path):
    is_new = not out_csv.exists()
    f = out_csv.open("a", encoding="utf-8", newline="")
    w = csv.DictWriter(f, fieldnames=["rb_part_num","rb_name","bo_part_num","source","confidence","error","source_hint"])
    if is_new: w.writeheader()
    return w, f

def rate_gate(last_time: list, rpm: float):
    min_interval = 60.0 / max(1.0, rpm)
    now = time.time()
    if last_time[0] is not None:
        elapsed = now - last_time[0]
        if elapsed < min_interval: time.sleep(min_interval - elapsed)
    last_time[0] = time.time()

def main():
    ap = argparse.ArgumentParser(description="RB↔BrickOwl crosswalk (BOID-only, multi-ID, resumable).")
    ap.add_argument("--rb-jsonl", required=True)
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--mode", choices=["live","dry"], default="live")
    ap.add_argument("--checkpoint", default="data/processed/crosswalk/rb_bo_checkpoint.txt")
    ap.add_argument("--rpm", type=float, default=150.0)
    ap.add_argument("--flush-every", type=int, default=200)
    args = ap.parse_args()

    rb_path = Path(args.rb_jsonl)
    out_csv = Path(args.out_csv); out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_json = Path(args.out_json); out_json.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path = Path(args.checkpoint); checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    # Build working set once so progress/ETA is accurate
    usable = list(iter_rb_parts(rb_path, args.limit))
    total_est = len(usable)

    already_done = load_checkpoint(checkpoint_path)
    writer, fh = open_output_writer(out_csv)

    bo = BrickOwlClient() if args.mode == "live" else None
    last_time = [None]
    processed = mapped = errors = 0
    buffer_since_flush = 0

    iterable = usable
    if tqdm: iterable = tqdm(usable, total=total_est, unit="part", smoothing=0.1, dynamic_ncols=True)

    def process_one(name: str, ids: Dict[str, List[Tuple[str,str]]]):
        # We’ll key the checkpoint by a stable string signature: first usable ID found in our order.
        order = ["design","bricklink","ldraw"]
        # Try in this order
        for kind in order:
            for value, hint in ids.get(kind, []):
                try:
                    rate_gate(last_time, args.rpm)
                    if kind == "design":
                        boid = bo.resolve_boid(value)
                        if boid: return value, hint, boid, "api", 1.0, None
                    elif kind == "bricklink":
                        # we don't have a direct resolve for BL in the BO client, but id_lookup can use the id_type
                        from phase0_backend.marketplace.brickowl_client import _bo_get
                        data = _bo_get("catalog/id_lookup", {"id": value, "type": "Part", "id_type": "bricklink_id"})
                        raw = data.get("boids") or []
                        if raw:
                            boid = raw[0] if isinstance(raw[0], str) else (raw[0].get("boid") if isinstance(raw[0], dict) else None)
                            if boid:
                                if isinstance(boid, str) and "-" in boid:
                                    boid = boid.split("-",1)[0]
                                return value, hint, boid, "api", 1.0, None
                    elif kind == "ldraw":
                        from phase0_backend.marketplace.brickowl_client import _bo_get
                        data = _bo_get("catalog/id_lookup", {"id": value, "type": "Part", "id_type": "ldraw_id"})
                        raw = data.get("boids") or []
                        if raw:
                            boid = raw[0] if isinstance(raw[0], str) else (raw[0].get("boid") if isinstance(raw[0], dict) else None)
                            if boid:
                                if isinstance(boid, str) and "-" in boid:
                                    boid = boid.split("-",1)[0]
                                return value, hint, boid, "api", 1.0, None
                except BrickOwlError as e:
                    # Permission/shape errors; continue trying other ids
                    return value, hint, None, None, None, str(e)[:300]
                except Exception as e:
                    return value, hint, None, None, None, f"unexpected:{e!r}"[:300]
        return None, None, None, None, None, None

    try:
        for rec in iterable:
            name = rec["name"]; ids = rec["ids"]

            # choose a stable checkpoint key: prefer the first design id, else first BL, else first LDraw
            key = (ids.get("design") or ids.get("bricklink") or ids.get("ldraw") or [("NA","")])[0][0]
            if key in already_done:
                processed += 1
                if tqdm: iterable.set_postfix_str(f"skipped={len(already_done)} mapped={mapped} err={errors}")
                continue

            rb_value, hint, boid, source, conf, err = process_one(name, ids)

            writer.writerow({
                "rb_part_num": rb_value or key,
                "rb_name": name,
                "bo_part_num": boid,
                "source": source,
                "confidence": conf,
                "error": err,
                "source_hint": hint,
            })
            append_checkpoint(checkpoint_path, key)

            if boid: mapped += 1
            if err: errors += 1

            processed += 1
            buffer_since_flush += 1
            if tqdm: iterable.set_postfix_str(f"mapped={mapped} err={errors}")
            if buffer_since_flush >= args.flush_every:
                fh.flush(); os.fsync(fh.fileno()); buffer_since_flush = 0

        fh.flush(); os.fsync(fh.fileno())
    finally:
        fh.close()

    stats = {
        "total": processed,
        "mapped": mapped,
        "errors": errors,
        "coverage_pct": (100.0 * mapped / processed) if processed else 0.0,
        "rpm_used": args.rpm,
        "checkpoint": str(checkpoint_path),
    }
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)
    print(json.dumps(stats, indent=2))
