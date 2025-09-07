import argparse, json, math, csv
from pathlib import Path
import numpy as np
from openai import OpenAI

def load_embeddings(path):
    ids, metas, vecs = [], [], []
    with Path(path).open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)
            ids.append(obj["id"])
            metas.append(obj.get("metadata", {}))
            vecs.append(obj["embedding"])
    # Convert to numpy for fast cosine
    M = np.array(vecs, dtype=np.float32)
    # Precompute L2 norms
    norms = np.linalg.norm(M, axis=1)
    return ids, metas, M, norms

def load_features_csv(path):
    idx = {}
    with Path(path).open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            idx[row["id"]] = row
    return idx

def cosine_topk(query_vec, M, norms, topk):
    q = np.asarray(query_vec, dtype=np.float32)
    qn = np.linalg.norm(q)
    if qn == 0:
        return np.zeros(M.shape[0], dtype=np.float32)
    sims = (M @ q) / (norms * qn + 1e-12)  # add epsilon to avoid div-by-zero
    # Argpartition for topk
    if topk < len(sims):
        idx = np.argpartition(-sims, topk)[:topk]
        idx = idx[np.argsort(-sims[idx])]
    else:
        idx = np.argsort(-sims)
    return idx, sims

def main():
    ap = argparse.ArgumentParser(description="Fast semantic query over BrickBuilder embeddings (NumPy).")
    ap.add_argument("--emb", type=str, default="data/index/parts_embeddings.jsonl")
    ap.add_argument("--feats", type=str, default="data/index/parts_features.csv")
    ap.add_argument("--query", type=str, required=True)
    ap.add_argument("--topk", type=int, default=10)
    # Optional filters
    ap.add_argument("--category-like", type=str, default="")
    ap.add_argument("--min-x", type=float, default=None)
    ap.add_argument("--max-x", type=float, default=None)
    ap.add_argument("--min-y", type=float, default=None)
    ap.add_argument("--max-y", type=float, default=None)
    ap.add_argument("--min-z", type=float, default=None)
    ap.add_argument("--max-z", type=float, default=None)
    args = ap.parse_args()

    # 1) Load embeddings + precompute norms (one-time, ~200–400MB RAM max depending on dtype/dim)
    ids, metas, M, norms = load_embeddings(args.emb)

    # 2) Embed query (OpenAI)
    client = OpenAI()
    q_emb = client.embeddings.create(model="text-embedding-3-small", input=[args.query]).data[0].embedding

    # 3) Rank by cosine using vectorized math
    top_idx, sims = cosine_topk(q_emb, M, norms, args.topk * 8)  # pull more to filter later

    # 4) Apply optional filters using features CSV
    feats = load_features_csv(args.feats)
    out = []
    for j in top_idx:
        pid = ids[j]
        m = metas[j]
        row = feats.get(pid)
        if not row:
            continue

        # Category substring filter
        if args.category_like and args.category_like.lower() not in (row.get("category_name","").lower()):
            continue

        # Size filters
        ex = float(row["extent_x_mm"]); ey = float(row["extent_y_mm"]); ez = float(row["extent_z_mm"])
        if args.min_x is not None and ex < args.min_x: continue
        if args.max_x is not None and ex > args.max_x: continue
        if args.min_y is not None and ey < args.min_y: continue
        if args.max_y is not None and ey > args.max_y: continue
        if args.min_z is not None and ez < args.min_z: continue
        if args.max_z is not None and ez > args.max_z: continue

        out.append((float(sims[j]), pid, row, m))
        if len(out) >= args.topk:
            break

    # 5) Print results
    for sim, pid, row, m in out:
        print(f"{sim: .4f}  id={pid}  rb={row['rb_part_num']}  cat={row['category_name']}  "
              f"size=({row['extent_x_mm']},{row['extent_y_mm']},{row['extent_z_mm']})  mesh={row['mesh_path']}")

if __name__ == "__main__":
    main()
