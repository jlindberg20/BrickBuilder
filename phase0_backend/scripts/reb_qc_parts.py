import pandas as pd
import json
from pathlib import Path
from collections import Counter

PROCESSED = Path("data/processed/rebrickable")
PREVIEW = PROCESSED / "parts_preview.csv"
JSONL = PROCESSED / "parts_unified.jsonl"

def qc_preview():
    print("=== Preview CSV QC ===")
    df = pd.read_csv(PREVIEW)
    print(f"Rows: {len(df):,}")
    print("Columns:", df.columns.tolist())
    print("\nSample rows:")
    print(df.head(10).to_string(index=False))
    print("\nCategory counts (top 15):")
    print(df["category"].value_counts().head(15))

def qc_jsonl(sample_size=5, scan_size=50000):
    print("\n=== JSONL Scan ===")
    total = 0
    n_parts = 0
    n_minifigs = 0
    color_counts = []
    usage_counts = []
    relationships_ct = []
    categories = Counter()
    samples = []

    with open(JSONL, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            rec = json.loads(line)
            total += 1
            t = rec.get("type")
            if t == "part":
                n_parts += 1
                color_counts.append(rec["stats"].get("color_count", 0))
                usage_counts.append(rec["stats"].get("set_usage_count", 0))
                relationships_ct.append(len(rec.get("relationships", [])))
                categories[rec["category"]["name"]] += 1
            elif t == "minifig":
                n_minifigs += 1
                usage_counts.append(rec["stats"].get("set_usage_count", 0))

            if len(samples) < sample_size:
                samples.append(rec)

            if i > scan_size:
                break

    print(f"Total records scanned: {total:,}")
    print(f"  Parts: {n_parts:,}")
    print(f"  Minifigs: {n_minifigs:,}")
    print(f"Average color variants per part: {sum(color_counts)/len(color_counts):.2f} (from {len(color_counts):,} parts)")
    print(f"Average set usage per record: {sum(usage_counts)/len(usage_counts):.2f}")
    print(f"Average relationships per part: {sum(relationships_ct)/len(relationships_ct):.2f}")

    print("\nTop 10 categories:")
    for cat, ct in categories.most_common(10):
        print(f"  {cat}: {ct:,}")

    print("\n=== Sample JSON Records ===")
    for rec in samples:
        print(json.dumps(rec, indent=2)[:1000])  # truncate for readability

def main():
    qc_preview()
    qc_jsonl()

if __name__ == "__main__":
    main()
