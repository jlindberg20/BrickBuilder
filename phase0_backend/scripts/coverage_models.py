import json
from collections import Counter
import os

def main():
    path = r"data/processed/models/master_models.jsonl"
    if not os.path.exists(path):
        print("No models file found:", path)
        return

    total = 0
    with_steps = 0
    with_bbox = 0
    step_counts = []
    part_counter = Counter()

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            total += 1

            steps = obj.get("steps")
            if steps is not None:
                with_steps += 1
                step_counts.append(len(steps))

            bbox = bool((obj.get("geometry") or {}).get("final_bbox_mm"))
            if bbox:
                with_bbox += 1

            for row in obj.get("bom", []):
                part_counter[row["rb_part_num"]] += int(row["qty"])

    avg_steps = (sum(step_counts)/len(step_counts)) if step_counts else 0.0
    top_parts = part_counter.most_common(10)

    summary = {
        "total_models": total,
        "with_steps": with_steps,
        "with_bbox": with_bbox,
        "avg_steps": round(avg_steps, 2),
        "top_parts_by_qty": [{"rb_part_num": p, "qty": q} for p, q in top_parts]
    }

    outp = r"data/processed/reports/master_models.coverage.json"
    os.makedirs(os.path.dirname(outp), exist_ok=True)
    with open(outp, "w", encoding="utf-8") as w:
        json.dump(summary, w, ensure_ascii=False, indent=2)

    print("Wrote", outp)
    print(json.dumps(summary, indent=2))

if __name__ == "__main__":
    main()
