# phase0_backend/scripts/reb_qc_parts.py

import json
import re
import random
import argparse
from pathlib import Path
from collections import Counter

import pandas as pd

PROCESSED = Path("data/processed/rebrickable")
PREVIEW = PROCESSED / "parts_preview.csv"
JSONL = PROCESSED / "parts_unified.jsonl"

HEX_RE = re.compile(r"^[0-9A-Fa-f]{6}$")


def qc_preview():
    print("=== Preview CSV QC ===")
    df = pd.read_csv(PREVIEW)
    print(f"Rows: {len(df):,}")
    print("Columns:", df.columns.tolist())
    print("\nSample rows:")
    print(df.head(10).to_string(index=False))
    print("\nCategory counts (top 15):")
    print(df["category"].value_counts().head(15))


def _normalize_relationships(rel):
    """
    Accept either:
      - list[...]                          (legacy)
      - {"items": [...], "by_type": {...}} (v3)
    Returns (items_list, by_type_dict)
    """
    if isinstance(rel, dict):
        items = rel.get("items", []) or []
        by_type = rel.get("by_type", {}) or {}
        return items, by_type
    elif isinstance(rel, list):
        return rel, {}
    else:
        return [], {}


def qc_jsonl_full(
    sample_size=5,
    seed=None,
    require_colors=False,
    exclude_category=None,
    sample_type="part",  # one of: "any", "part", "minifig"
):
    print("\n=== JSONL Full-Scan QC ===")

    # Metrics counters
    total = parts = figs = 0
    categories = Counter()
    rel_types = Counter()
    rel_counts = []
    color_counts = []
    element_counts = []
    set_usage_counts = []
    ldraw_yes = 0

    # Color variant-level metrics
    total_color_variants = 0
    variants_with_rgb = 0
    variants_with_name = 0
    variants_hex_valid = 0

    # Random reservoir
    rng = random.Random(seed)
    samples = []
    n_seen = 0

    # Track one very colorful part to show
    best_colorful_part = None
    best_colorful_count = -1
    best_colorful_variants_snippet = []

    with open(JSONL, "r", encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            total += 1
            t = rec.get("type")

            # Normalize relationships to a standard shape
            items, by_type = _normalize_relationships(rec.get("relationships"))
            for k, v in by_type.items():
                try:
                    rel_types[k] += int(v or 0)
                except Exception:
                    pass

            if t == "part":
                parts += 1
                stats = rec.get("stats", {}) or {}
                color_count = int(stats.get("color_count", 0) or 0)
                element_count = int(stats.get("element_count", 0) or 0)
                set_usage = int(stats.get("set_usage_count", 0) or 0)

                color_counts.append(color_count)
                element_counts.append(element_count)
                set_usage_counts.append(set_usage)
                rel_counts.append(len(items))

                cat = (rec.get("category") or {}).get("name")
                if cat:
                    categories[cat] += 1

                if rec.get("geometry", {}).get("ldraw", {}).get("available"):
                    ldraw_yes += 1

                variants = (rec.get("color_compatibility") or {}).get("variants", []) or []
                if variants:
                    total_color_variants += len(variants)
                    for v in variants:
                        name = v.get("name")
                        rgb = v.get("rgb")
                        if name:
                            variants_with_name += 1
                        if rgb:
                            variants_with_rgb += 1
                            if isinstance(rgb, str) and HEX_RE.match(rgb):
                                variants_hex_valid += 1

                    if len(variants) > best_colorful_count:
                        best_colorful_count = len(variants)
                        best_colorful_part = rec
                        best_colorful_variants_snippet = variants[:8]

            elif t == "minifig":
                figs += 1
                set_usage = int((rec.get("stats") or {}).get("set_usage_count", 0) or 0)
                set_usage_counts.append(set_usage)

            # ---------- Reservoir sampling (randomized) ----------
            eligible = True

            # Type filter
            if sample_type != "any" and t != sample_type:
                eligible = False

            # Exclude category filter (applies only to parts)
            if eligible and exclude_category and t == "part":
                cat = (rec.get("category") or {}).get("name") or ""
                if cat.strip().lower() == exclude_category.strip().lower():
                    eligible = False

            # Require color variants (applies only to parts)
            if eligible and require_colors and t == "part":
                variants = (rec.get("color_compatibility") or {}).get("variants", []) or []
                if len(variants) == 0:
                    eligible = False

            if eligible:
                n_seen += 1
                if len(samples) < sample_size:
                    samples.append(rec)
                else:
                    j = rng.randint(0, n_seen - 1)
                    if j < sample_size:
                        samples[j] = rec

    def avg(lst):
        return (sum(lst) / len(lst)) if lst else 0.0

    print(f"Total records: {total:,}")
    print(f"  Parts:      {parts:,}")
    print(f"  Minifigs:   {figs:,}")
    print(f"Avg colors per part:     {avg(color_counts):.2f}")
    print(f"Avg elements per part:   {avg(element_counts):.2f}")
    print(f"Avg set usage per record:{avg(set_usage_counts):.2f}")
    print(f"Avg relationships/part:  {avg(rel_counts):.2f}")
    if parts:
        pct_ldraw = (ldraw_yes / parts) * 100 if parts else 0.0
        print(f"LDraw presence (naive filename match): {ldraw_yes:,} / {parts:,} ({pct_ldraw:.1f}%)")

    if total_color_variants:
        pct_rgb = 100.0 * variants_with_rgb / total_color_variants
        pct_hex_ok = 100.0 * variants_hex_valid / total_color_variants
        pct_named = 100.0 * variants_with_name / total_color_variants
        print("\nColor variant quality:")
        print(f"  Variants total:       {total_color_variants:,}")
        print(f"  With rgb:             {variants_with_rgb:,} ({pct_rgb:.1f}%)")
        print(f"  With valid hex:       {variants_hex_valid:,} ({pct_hex_ok:.1f}%)")
        print(f"  With name:            {variants_with_name:,} ({pct_named:.1f}%)")
    else:
        print("\nColor variant quality: no variants found")

    print("\nTop 10 categories:")
    for cat, ct in categories.most_common(10):
        print(f"  {cat}: {ct:,}")

    print("\nRelationship types (top 10):")
    for k, v in rel_types.most_common(10):
        print(f"  {k}: {v:,}")

    print(
        f"\n=== Random sample (size={sample_size}, seed={seed}, "
        f"type={sample_type}, require_colors={require_colors}, "
        f"exclude_category={exclude_category}) ==="
    )
    for rec in samples:
        s = json.dumps(rec, indent=2)
        print(s[:1200])

    if best_colorful_part:
        print("\n=== Example part with many colors ===")
        print(json.dumps({
            "id": best_colorful_part["id"],
            "name": best_colorful_part.get("name"),
            "category": (best_colorful_part.get("category") or {}).get("name"),
            "color_count": (best_colorful_part.get("stats") or {}).get("color_count"),
            "first_variants": best_colorful_variants_snippet
        }, indent=2))
    else:
        print("\n(No part with color variants found.)")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--samples", type=int, default=5, help="Number of random JSON samples to print")
    p.add_argument("--seed", type=int, default=None, help="PRNG seed for reproducible sampling")
    p.add_argument("--require-colors", action="store_true",
                   help="Only sample parts that have at least 1 color variant")
    p.add_argument("--exclude-category", type=str, default=None,
                   help="Exclude a category name (e.g., 'Stickers') from sampling")
    p.add_argument("--type", type=str, default="part", choices=["any", "part", "minifig"],
                   help="Record type to sample from")
    return p.parse_args()


def main():
    args = parse_args()
    qc_preview()
    qc_jsonl_full(
        sample_size=args.samples,
        seed=args.seed,
        require_colors=args.require_colors,
        exclude_category=args.exclude_category,
        sample_type=args.type,
    )


if __name__ == "__main__":
    main()



