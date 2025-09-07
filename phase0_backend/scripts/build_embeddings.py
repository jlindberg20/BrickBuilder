import argparse, json, os, sys, math, time, random
from pathlib import Path

def iter_docs(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

def dummy_embed(text: str, dim: int, seed: int) -> list[float]:
    rnd = random.Random(seed)
    return [rnd.uniform(-1.0, 1.0) for _ in range(dim)]

def openai_embed_batch(texts, model: str, dimensions: int | None):
    # Late import so dummy mode has no deps
    from openai import OpenAI
    client = OpenAI()
    # dimensions is optional for 3-large; ignored by 3-small if not supported
    kwargs = {"model": model, "input": texts}
    if dimensions is not None:
        kwargs["dimensions"] = dimensions
    resp = client.embeddings.create(**kwargs)
    return [item.embedding for item in resp.data]

def main():
    ap = argparse.ArgumentParser(description="Build embeddings for parts_docs.jsonl")
    ap.add_argument("--in-docs", type=str, default="data/index/parts_docs.jsonl")
    ap.add_argument("--out-emb", type=str, default="data/index/parts_embeddings.jsonl")
    ap.add_argument("--provider", type=str, default="dummy", choices=["dummy","openai"])
    ap.add_argument("--openai-model", type=str, default="text-embedding-3-small")
    ap.add_argument("--dimensions", type=int, default=None, help="Optional: shorten dims (mostly for 3-large)")
    ap.add_argument("--batch-size", type=int, default=256)
    ap.add_argument("--dummy-dim", type=int, default=1536)
    args = ap.parse_args()

    in_path = Path(args.in_docs)
    out_path = Path(args.out_emb)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    total = 0
    t0 = time.time()
    with in_path.open("r", encoding="utf-8") as fin, out_path.open("w", encoding="utf-8") as fout:
        batch = []
        for line in fin:
            if not line.strip():
                continue
            rec = json.loads(line)
            batch.append(rec)
            if len(batch) >= args.batch_size:
                total += process_batch(batch, args, fout)
                batch = []
        if batch:
            total += process_batch(batch, args, fout)

    dt = time.time() - t0
    print(f"[OK] Embedded {total} records via {args.provider} in {dt:.2f}s -> {out_path}")

def process_batch(batch, args, fout):
    texts = [r["text"] for r in batch]
    ids = [r["id"] for r in batch]
    metas = [r["metadata"] for r in batch]

    if args.provider == "dummy":
        # Stable seed per id
        embs = [dummy_embed(t, args.dummy_dim, seed=hash(i) & 0xffffffff) for t, i in zip(texts, ids)]
    else:
        if not os.getenv("OPENAI_API_KEY"):
            print("[ERROR] OPENAI_API_KEY not set.", file=sys.stderr)
            sys.exit(2)
        embs = openai_embed_batch(texts, model=args.openai_model, dimensions=args.dimensions)

    # write
    for i, e, m in zip(ids, embs, metas):
        fout.write(json.dumps({"id": i, "embedding": e, "metadata": m}) + "\n")
    return len(batch)

if __name__ == "__main__":
    main()
