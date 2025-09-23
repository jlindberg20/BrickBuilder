#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Deterministic Instruction Harvester:
- Reads models JSONL with instructions.links.
- Finds first viable PDF URL, downloads with retries + backoff (cached by SHA256).
- Renders to data/instructions/<set_num>/page-0001.png... at fixed DPI.
- Augments each record with instructions.pages { image_dir, count, images }.
- Writes one-line JSONL, preserving all other fields unchanged.
- Logs failures to data/scratch/instructions_failures.csv

Idempotent & resumable:
- If a set has a manifest with matching pdf_sha, rendering is skipped.
- URL→SHA map ensures we never re-download the same content.

Usage:
  python -u phase0_backend/scripts/harvest_instructions.py \
    --inn master_models.json \
    --out data/processed/models/master_models.with_pages.SMOKE.jsonl \
    --include-set-nums 10233-1,10244-1 \
    --dpi 200
"""
import argparse, hashlib, io, json, os, re, sys, time
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict
import urllib.request
import urllib.error
from datetime import datetime, UTC


try:
    import fitz  # PyMuPDF
except Exception as e:
    print("ERROR: PyMuPDF (pymupdf) not installed.", file=sys.stderr)
    raise

PDF_EXT_RX = re.compile(r"\.pdf(\?.*)?$", re.IGNORECASE)

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def load_urlmap(urlmap_path: Path) -> Dict[str, str]:
    if urlmap_path.exists():
        try:
            return json.loads(urlmap_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_urlmap(urlmap_path: Path, urlmap: Dict[str, str]) -> None:
    urlmap_path.parent.mkdir(parents=True, exist_ok=True)
    urlmap_path.write_text(json.dumps(urlmap, ensure_ascii=False, indent=2), encoding="utf-8")

def polite_sleep(ms: int):
    if ms > 0:
        time.sleep(ms / 1000.0)

def download_pdf_cached(url: str, cache_dir: Path, urlmap_path: Path, retries=3, backoff=0.8, sleep_ms=250) -> Optional[Path]:
    """Download URL if not cached; return path to cached PDF. Returns None on failure."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    urlmap = load_urlmap(urlmap_path)
    if url in urlmap:
        sha = urlmap[url]
        pdf_path = cache_dir / f"{sha}.pdf"
        if pdf_path.exists() and pdf_path.stat().st_size > 0:
            return pdf_path
        # stale mapping; fall through to re-download

    attempt = 0
    last_err = None
    while attempt <= retries:
        try:
            polite_sleep(sleep_ms)
            with urllib.request.urlopen(url, timeout=30) as resp:
                if resp.status != 200 or ("application/pdf" not in resp.headers.get("Content-Type","").lower() and not PDF_EXT_RX.search(url)):
                    # Some LEGO CDN serves octet-stream; we still accept as PDF if URL ends with .pdf
                    pass
                data = resp.read()
                if not data or len(data) < 1000:
                    raise RuntimeError(f"Suspiciously small PDF payload: {len(data)} bytes")
                sha = sha256_bytes(data)
                pdf_path = cache_dir / f"{sha}.pdf"
                if not pdf_path.exists():
                    pdf_path.write_bytes(data)
                urlmap[url] = sha
                save_urlmap(urlmap_path, urlmap)
                return pdf_path
        except Exception as e:
            last_err = e
            if attempt == retries:
                break
            # exponential backoff (bounded)
            delay = max(0.5, (backoff ** attempt) * 2.0)
            time.sleep(delay)
            attempt += 1
    # failed
    sys.stderr.write(f"[WARN] Download failed for URL: {url} :: {last_err}\n")
    return None

def find_set_num(rec: dict) -> Optional[str]:
    # Prefer explicit RB id if present
    rb = ((rec.get("source_ids") or {}).get("rb") or {})
    if isinstance(rb, dict):
        sn = rb.get("set_num")
        if sn:
            return sn
    # Fallback: parse from id like "model:rb:<set_num>"
    rid = rec.get("id") or ""
    m = re.match(r"^model:rb:([A-Za-z0-9\-]+)$", rid)
    if m:
        return m.group(1)
    return None

def choose_pdf_urls(rec: dict) -> List[str]:
    links = (((rec.get("instructions") or {}).get("links")) or [])
    urls = []
    for lk in links:
        try:
            if (lk.get("kind") == "pdf") or PDF_EXT_RX.search(lk.get("url","")):
                urls.append(lk["url"])
        except Exception:
            continue
    return urls

def render_pdf_to_pngs(pdf_path: Path, out_dir: Path, dpi: int) -> List[str]:
    """
    Returns list of image filenames (relative to out_dir), like ["page-0001.png", ...]
    Idempotent: if manifest exists and matches pdf_sha, reuse existing files.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    data = pdf_path.read_bytes()
    pdf_sha = sha256_bytes(data)
    manifest_path = out_dir / "manifest.json"
    if manifest_path.exists():
        try:
            m = json.loads(manifest_path.read_text(encoding="utf-8"))
            if m.get("pdf_sha") == pdf_sha and m.get("dpi") == dpi:
                # Reuse image list from manifest if files still exist
                imgs = m.get("images") or []
                if imgs and all((out_dir / f).exists() for f in imgs):
                    return imgs
        except Exception:
            pass

    # Render afresh
    doc = fitz.open(pdf_path.as_posix())
    images = []
    for i in range(len(doc)):
        page = doc.load_page(i)
        # Create a transformation matrix corresponding roughly to given DPI
        # PyMuPDF: zoom = dpi / 72
        zoom = dpi / 72.0
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        fname = f"page-{i+1:04d}.png"
        fpath = out_dir / fname
        pix.save(fpath.as_posix())
        images.append(fname)
    doc.close()

    # Write manifest
    manifest = {
        "pdf_sha": pdf_sha,
        "dpi": dpi,
        "images": images,
        "generated_at": datetime.now(UTC).isoformat(),
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return images

def append_failure(fail_csv: Path, set_num: str, url: str, err: str, exhausted: bool):
    fail_csv.parent.mkdir(parents=True, exist_ok=True)
    header_needed = not fail_csv.exists()
    with fail_csv.open("a", encoding="utf-8") as f:
        if header_needed:
            f.write("timestamp,set_num,url,error,retry_budget_exhausted\n")
        ts = datetime.now(UTC).isoformat()
        # Escape commas in error by replacing newlines/commas
        err_clean = (err or "").replace("\n", " ").replace(",", ";")
        f.write(f"{ts},{set_num or ''},{url or ''},{err_clean},{'1' if exhausted else '0'}\n")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inn", type=str, default="master_models.json", help="Input models JSONL")
    ap.add_argument("--out", type=str, required=False, help="Output JSONL (timestamped default)")
    ap.add_argument("--include-set-nums", type=str, default="", help="Comma-separated list to include only these set_nums")
    ap.add_argument("--start-after", type=str, default="", help="Resume after this set_num (exclusive)")
    ap.add_argument("--limit", type=int, default=0, help="Max number of models to process")
    ap.add_argument("--dpi", type=int, default=200, help="Render DPI")
    ap.add_argument("--sleep-ms", type=int, default=250, help="Per-item polite delay (ms)")
    ap.add_argument("--retries", type=int, default=3, help="Download retries")
    ap.add_argument("--backoff", type=float, default=0.8, help="Exponential backoff factor")
    args = ap.parse_args()

    in_path = Path(args.inn)
    if not in_path.exists():
        # fallback to latest with_pdfs.*.jsonl
        proc = Path("data/processed/models")
        candidates = sorted(proc.glob("master_models_from_sets.with_pdfs*.jsonl"), key=lambda p: p.stat().st_mtime)
        if not candidates:
            print(f"ERROR: Input not found: {in_path} and no with_pdfs.*.jsonl in {proc}", file=sys.stderr)
            sys.exit(2)
        in_path = candidates[-1]
        print(f"[info] Using fallback input: {in_path}", file=sys.stderr)

    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    out_path = Path(args.out) if args.out else Path(f"data/processed/models/master_models.with_pages.{ts}.jsonl")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    cache_dir = Path("data/cache/instructions")
    urlmap_path = cache_dir / "urlmap.json"
    fail_csv = Path("data/scratch/instructions_failures.csv")
    base_img_dir = Path("data/instructions")

    include = set([s.strip() for s in args.include_set_nums.split(",") if s.strip()]) if args.include_set_nums else None
    start_after = args.start_after.strip()
    after_flag = bool(start_after)
    passed_sentinel = (not after_flag)

    count_in = count_out = 0
    done = 0
    with in_path.open("r", encoding="utf-8") as fin, out_path.open("w", encoding="utf-8") as fout:
        for line in fin:
            if not line.strip():
                continue
            rec = json.loads(line)
            count_in += 1
            set_num = find_set_num(rec)
            if after_flag and set_num == start_after:
                passed_sentinel = True
                # skip the sentinel row itself
                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
                continue
            if after_flag and not passed_sentinel:
                # before sentinel: pass-through unchanged
                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
                continue
            if include and (set_num not in include):
                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
                continue
            if args.limit and done >= args.limit:
                # pass-through remainder unchanged
                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
                continue

            polite_sleep(args.sleep_ms)

            # Default: pass-through unchanged
            out_rec = rec

            pdf_urls = choose_pdf_urls(rec)
            if set_num and pdf_urls:
                # Try first viable URL then fall back to others if needed
                images_list = None
                last_err = None
                for url in pdf_urls:
                    pdf_path = download_pdf_cached(url, cache_dir, urlmap_path, retries=args.retries, backoff=args.backoff, sleep_ms=args.sleep_ms)
                    if not pdf_path:
                        last_err = f"download_failed:{url}"
                        append_failure(fail_csv, set_num, url, last_err, exhausted=True)
                        continue
                    try:
                        out_dir = base_img_dir / set_num
                        images = render_pdf_to_pngs(pdf_path, out_dir, dpi=args.dpi)
                        if images:
                            images_list = images
                            # Augment record without mutating original nested structure more than necessary
                            instr = (out_rec.get("instructions") or {})
                            instr_links = instr.get("links")
                            pages_obj = {
                                "image_dir": str(out_dir).replace("\\","/"),
                                "count": len(images),
                                "images": images
                            }
                            instr["pages"] = pages_obj
                            # Preserve links as-is
                            if instr_links is not None:
                                instr["links"] = instr_links
                            out_rec["instructions"] = instr
                            break
                        else:
                            last_err = "render_empty_images"
                            append_failure(fail_csv, set_num, url, last_err, exhausted=False)
                    except Exception as e:
                        last_err = f"render_failed:{e}"
                        append_failure(fail_csv, set_num, url, last_err, exhausted=False)
                        continue

                if images_list is None and last_err:
                    # could not render any URL; pass-through unchanged
                    pass

            fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
            count_out += 1
            if set_num and (pdf_urls or []):
                done += 1
                print(f"[{done}] {set_num}  -> pages {'OK' if (out_rec.get('instructions',{}).get('pages')) else 'SKIP'}", file=sys.stderr)

    print(json.dumps({
        "input_file": str(in_path),
        "output_file": str(out_path),
        "records_in": count_in,
        "records_out": count_out,
        "augmented": done
    }, ensure_ascii=False, indent=2))
    return 0

if __name__ == "__main__":
    sys.exit(main())
