#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Deterministic Instruction Harvester (BrickBuilder Phase-0)

- Reads models JSONL (one model per line).
- For each model, finds the first viable PDF in instructions.links, downloads via cache, renders page PNGs,
  and augments the record with instructions.pages { image_dir, count, images }.
- Non-destructive: instructions.links and all other fields are preserved exactly as-is.
- Idempotent & resumable: per-URL SHA256 cache, per-set manifest with pdf_sha+dpi, URL map to avoid refetch.
- Robust: custom User-Agent, short per-URL timeout, retries/backoff, per-model deadline to avoid stalls.
- Graceful Ctrl+C: writes current record unchanged, flushes, exits cleanly.
- Writes a compact failure ledger CSV for any download/render issues.

Requirements (already suggested earlier):
  pip install pymupdf pillow

Usage examples:

  # Two-known-good sets (smoke)
  python -u phase0_backend/scripts/harvest_instructions.py \
    --inn master_models.jsonl \
    --out data/processed/models/master_models.with_pages.SMOKE.jsonl \
    --include-set-nums 10233-1,10244-1 \
    --dpi 200

  # Bounded batch with timeouts and retries
  python -u phase0_backend/scripts/harvest_instructions.py \
    --inn master_models.jsonl \
    --out data/processed/models/master_models.with_pages.BATCH1.jsonl \
    --include-set-nums  <comma,separated,set_nums> \
    --dpi 200 --per-url-timeout 12 --per-model-deadline 25 --retries 2 --sleep-ms 250

This script prints progress lines to STDERR like:
  [17] 10233-1  -> pages OK
Wrap with a PowerShell Write-Progress if you want a bar.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import sys
import time
import socket
import urllib.request
import urllib.error
from datetime import datetime, UTC
from pathlib import Path
from typing import Optional, List, Dict

# ---------- Rendering backend ----------
try:
    import fitz  # PyMuPDF
except Exception as e:
    print("ERROR: PyMuPDF (pymupdf) is required. pip install pymupdf", file=sys.stderr)
    raise

PDF_EXT_RX = re.compile(r"\.pdf(\?.*)?$", re.IGNORECASE)


# ---------- Small utils ----------

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def polite_sleep(ms: int):
    if ms and ms > 0:
        time.sleep(ms / 1000.0)


def posix_path(p: Path) -> str:
    return str(p).replace("\\", "/")


# ---------- URL cache ----------

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


def download_pdf_cached(
    url: str,
    cache_dir: Path,
    urlmap_path: Path,
    retries: int = 3,
    backoff: float = 0.8,
    sleep_ms: int = 250,
    timeout_sec: int = 12,
) -> Optional[Path]:
    """
    Download a URL if not already cached, store as data/cache/instructions/<sha>.pdf.
    Returns the cached file path or None after retry budget exhausted.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    urlmap = load_urlmap(urlmap_path)

    # Use cache if mapping exists and file present
    if url in urlmap:
        sha = urlmap[url]
        pdf_path = cache_dir / f"{sha}.pdf"
        if pdf_path.exists() and pdf_path.stat().st_size > 0:
            return pdf_path
        # stale mapping; fall through

    # Prepare request with headers
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "BrickBuilder/1.0 (instruction-harvester) Python-urllib",
            "Accept": "application/pdf,*/*;q=0.8",
            "Connection": "close",
        },
        method="GET",
    )

    attempt = 0
    last_err = None
    while attempt <= retries:
        try:
            polite_sleep(sleep_ms)
            with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
                data = resp.read()
                # Some endpoints return octet-stream; we accept as long as payload looks non-trivial
                if not data or len(data) < 1000:
                    raise RuntimeError(f"suspicious payload {len(data)} bytes")
                sha = sha256_bytes(data)
                pdf_path = cache_dir / f"{sha}.pdf"
                if not pdf_path.exists():
                    pdf_path.write_bytes(data)
                # Update mapping
                urlmap[url] = sha
                save_urlmap(urlmap_path, urlmap)
                return pdf_path
        except (urllib.error.HTTPError, urllib.error.URLError, socket.timeout, TimeoutError) as e:
            last_err = e
            if attempt == retries:
                break
            delay = max(0.5, (backoff ** attempt) * 2.0)
            time.sleep(delay)
            attempt += 1
            continue
        except Exception as e:
            last_err = e
            break

    sys.stderr.write(f"[WARN] Download failed: {url} :: {last_err}\n")
    return None


# ---------- Rendering ----------

def render_pdf_to_pngs(pdf_path: Path, out_dir: Path, dpi: int) -> List[str]:
    """
    Render PDF to PNG images in out_dir. Idempotent via manifest:
    if pdf_sha+dpi match, reuse images listed in manifest.json.
    Returns the list of image filenames (relative).
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    data = pdf_path.read_bytes()
    pdf_sha = sha256_bytes(data)

    manifest_path = out_dir / "manifest.json"
    if manifest_path.exists():
        try:
            m = json.loads(manifest_path.read_text(encoding="utf-8"))
            if m.get("pdf_sha") == pdf_sha and m.get("dpi") == dpi:
                imgs = m.get("images") or []
                if imgs and all((out_dir / f).exists() for f in imgs):
                    return imgs
        except Exception:
            pass

    # Fresh render
    doc = fitz.open(pdf_path.as_posix())
    images: List[str] = []
    zoom = dpi / 72.0  # PyMuPDF: 72 DPI base
    mat = fitz.Matrix(zoom, zoom)

    for i in range(len(doc)):
        page = doc.load_page(i)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        fname = f"page-{i+1:04d}.png"
        fpath = out_dir / fname
        pix.save(fpath.as_posix())
        images.append(fname)

    doc.close()

    manifest = {
        "pdf_sha": pdf_sha,
        "dpi": dpi,
        "images": images,
        "generated_at": datetime.now(UTC).isoformat(),
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return images


# ---------- Failures ----------

def append_failure(fail_csv: Path, set_num: str | None, url: str | None, err: str | None, exhausted: bool):
    fail_csv.parent.mkdir(parents=True, exist_ok=True)
    header_needed = not fail_csv.exists()
    with fail_csv.open("a", encoding="utf-8") as f:
        if header_needed:
            f.write("timestamp,set_num,url,error,retry_budget_exhausted\n")
        ts = datetime.now(UTC).isoformat()
        err_clean = (err or "").replace("\n", " ").replace(",", ";")
        f.write(f"{ts},{set_num or ''},{url or ''},{err_clean},{'1' if exhausted else '0'}\n")


# ---------- Record helpers ----------

def find_set_num(rec: dict) -> Optional[str]:
    rb = ((rec.get("source_ids") or {}).get("rb") or {})
    if isinstance(rb, dict):
        sn = rb.get("set_num")
        if sn:
            return sn
    rid = rec.get("id") or ""
    m = re.match(r"^model:rb:([A-Za-z0-9\-]+)$", rid)
    if m:
        return m.group(1)
    return None


def choose_pdf_urls(rec: dict) -> List[str]:
    links = (((rec.get("instructions") or {}).get("links")) or [])
    urls: List[str] = []
    if isinstance(links, list):
        for lk in links:
            try:
                url = lk.get("url", "")
                if (lk.get("kind") == "pdf") or PDF_EXT_RX.search(url or ""):
                    urls.append(url)
            except Exception:
                continue
    return urls


# ---------- Main ----------

def resolve_input_path(user_path: Path) -> Path:
    # 1) If exact path provided and exists, use it
    if user_path and user_path.exists():
        return user_path
    # 2) Try common root names
    for candidate in [Path("master_models.jsonl"), Path("master_models.json")]:
        if candidate.exists():
            return candidate
    # 3) Fallback to latest processed with_pdfs
    proc = Path("data/processed/models")
    candidates = sorted(proc.glob("master_models_from_sets.with_pdfs*.jsonl"),
                        key=lambda p: p.stat().st_mtime)
    if candidates:
        print(f"[info] Using fallback input: {candidates[-1]}", file=sys.stderr)
        return candidates[-1]
    raise FileNotFoundError("No input models file found. Tried master_models.jsonl, master_models.json, and processed with_pdfs*.jsonl")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inn", type=str, default="master_models.json", help="Input models JSONL (root .jsonl/.json auto-detected)")
    ap.add_argument("--out", type=str, help="Output JSONL (default: timestamped in data/processed/models/)")
    ap.add_argument("--include-set-nums", type=str, default="", help="Comma-separated set_nums to include (others pass-through)")
    ap.add_argument("--start-after", type=str, default="", help="Resume after this set_num (exclusive)")
    ap.add_argument("--limit", type=int, default=0, help="Max number of models to *attempt* augmenting (others pass-through)")
    ap.add_argument("--dpi", type=int, default=200, help="Render DPI")
    ap.add_argument("--sleep-ms", type=int, default=250, help="Polite delay between items (ms)")
    ap.add_argument("--retries", type=int, default=3, help="Download retries")
    ap.add_argument("--backoff", type=float, default=0.8, help="Exponential backoff factor")
    ap.add_argument("--per-url-timeout", type=int, default=12, help="Per-URL timeout seconds")
    ap.add_argument("--per-model-deadline", type=int, default=25, help="Max seconds to spend on one model before skipping")
    args = ap.parse_args()

    in_path = resolve_input_path(Path(args.inn))
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    out_path = Path(args.out) if args.out else Path(f"data/processed/models/master_models.with_pages.{ts}.jsonl")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    cache_dir = Path("data/cache/instructions")
    urlmap_path = cache_dir / "urlmap.json"
    fail_csv = Path("data/scratch/instructions_failures.csv")
    base_img_dir = Path("data/instructions")

    include = set(s.strip() for s in args.include_set_nums.split(",") if s.strip()) if args.include_set_nums else None
    start_after = args.start_after.strip()
    gate_until = bool(start_after)
    passed_sentinel = not gate_until

    count_in = count_out = augmented = 0

    # Use utf-8-sig to tolerate BOM at start of file
    with in_path.open("r", encoding="utf-8-sig") as fin, out_path.open("w", encoding="utf-8") as fout:
        try:
            for raw in fin:
                if not raw.strip():
                    continue
                rec = json.loads(raw)
                count_in += 1

                # Default: pass-through unchanged
                out_rec = rec

                set_num = find_set_num(rec)

                # Resume gating
                if gate_until:
                    if set_num == start_after:
                        passed_sentinel = True
                        fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                        count_out += 1
                        continue
                    if not passed_sentinel:
                        fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                        count_out += 1
                        continue

                # Include filtering
                if include and (set_num not in include):
                    fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                    count_out += 1
                    continue

                # Limit of attempts (we still pass-through remainder)
                if args.limit and augmented >= args.limit:
                    fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                    count_out += 1
                    continue

                polite_sleep(args.sleep_ms)

                pdf_urls = choose_pdf_urls(rec)
                if set_num and pdf_urls:
                    images_list = None
                    last_err = None
                    model_start = time.time()

                    for url in pdf_urls:
                        # Enforce per-model deadline
                        if (time.time() - model_start) > max(1, args.per_model_deadline):
                            append_failure(fail_csv, set_num, url, "per_model_deadline_exceeded", exhausted=True)
                            break

                        pdf_path = download_pdf_cached(
                            url,
                            cache_dir,
                            urlmap_path,
                            retries=args.retries,
                            backoff=args.backoff,
                            sleep_ms=args.sleep_ms,
                            timeout_sec=max(3, args.per_url_timeout),
                        )

                        if not pdf_path:
                            last_err = f"download_failed:{url}"
                            append_failure(fail_csv, set_num, url, last_err, exhausted=True)
                            continue

                        try:
                            out_dir = base_img_dir / set_num
                            images = render_pdf_to_pngs(pdf_path, out_dir, dpi=args.dpi)
                            if images:
                                images_list = images
                                # Augment record non-destructively
                                instr = (out_rec.get("instructions") or {})
                                links_copy = instr.get("links")
                                pages_obj = {
                                    "image_dir": posix_path(out_dir),
                                    "count": len(images),
                                    "images": images,
                                }
                                instr["pages"] = pages_obj
                                if links_copy is not None:
                                    instr["links"] = links_copy
                                out_rec["instructions"] = instr
                                break
                            else:
                                last_err = "render_empty_images"
                                append_failure(fail_csv, set_num, url, last_err, exhausted=False)
                        except Exception as e:
                            last_err = f"render_failed:{e}"
                            append_failure(fail_csv, set_num, url, last_err, exhausted=False)
                            continue

                    # Progress line for wrappers (stderr)
                    print(
                        f"[{augmented + 1}] {set_num}  -> pages {'OK' if images_list else 'SKIP'}",
                        file=sys.stderr,
                    )

                    if images_list:
                        augmented += 1

                # Write out (either augmented or pass-through)
                fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                count_out += 1

        except KeyboardInterrupt:
            # Write nothing extra here; current record was already written as pass-through above.
            print("\n[info] Interrupted by user. Partial file is consistent and usable.", file=sys.stderr)
            # fall through to footer

    # Footer summary (stdout)
    print(
        json.dumps(
            {
                "input_file": str(in_path),
                "output_file": str(out_path),
                "records_in": count_in,
                "records_out": count_out,
                "augmented": augmented,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
