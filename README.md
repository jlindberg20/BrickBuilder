# BrickBuilder

BrickBuilder AI is an end-to-end system for AI-driven LEGO model generation.  
The vision: given a natural language input (e.g., *“I want a foot-long submarine”*), the engine constructs a valid LEGO structure using real parts, generates instructions, and provides sourcing links for purchasing with one click.

---

## Overview

BrickBuilder unifies data from Rebrickable, LDraw, BrickLink, and BrickOwl into a **canonical parts catalog** enriched with geometry, marketplace metadata, and embeddings for semantic retrieval. On top of this dataset, AI pipelines can design and assemble novel LEGO structures that are physically consistent and buildable.

The project consists of three major phases:

1. **Canonical Parts Catalog**  
   - Ingest Rebrickable as the primary spine.  
   - Attach clean, millimeter-scaled 3D meshes from LDraw conversions.  
   - Enforce a comprehensive JSON schema (`part_record.schema.json`).  
   - Validate records and ensure cross-source ID consistency.  

2. **Marketplace Integration (in progress)**  
   - Map Rebrickable parts to BrickLink and BrickOwl IDs.  
   - Enrich with pricing, availability, and color variants.  
   - Add marketplace-only parts not present in Rebrickable.  

3. **Retrieval & Generation Engine (early prototype)**  
   - Generate embeddings for each part (`text-embedding-3-small`).  
   - Enable semantic search with category and dimension filters.  
   - Provide retrieval proofs-of-concept using NumPy-based cosine similarity (to be replaced with Pinecone/FAISS/HNSW for production).  

---

## Current State (September 2025)

**Canonical Dataset**
- `data/mesh/obj_mm_manifest.csv`: 23,301 OBJ/MM files indexed with triangle counts and bounding boxes.  
- `data/processed/rebrickable/parts_with_mesh_mm.jsonl`: 75,250 Rebrickable records processed; 24,872 parts with attached meshes.  
- `phase0_backend/part_record.schema.json`: Master JSON schema defining required and optional fields (source IDs, geometry, market data, metadata).

**Validation**
- `phase0_backend/scripts/validate_parts_jsonl.py`: Supports `--only-type part` and `--require-mesh` flags.  
- Confirmed: ~24,872 valid part records with meshes, schema-compliant.  

**Index Inputs**
- `data/index/parts_docs.jsonl`: Flattened text+metadata records (24,872 rows).  
- `data/index/parts_features.csv`: Tabular numeric features (extents, triangles).  

**Embeddings**
- `data/index/parts_embeddings.jsonl`: 24,872 embeddings generated via OpenAI `text-embedding-3-small` (~1.5k dims each).  
- Semantic retrieval validated for key queries:
  - *“classic 2×4 brick”* → 3001 and variants.  
  - *“curved slope”* → sloped brick families.  
  - *“technic axle connector”* → connectors and axles with dimensional filters.  

**Utility Scripts**
- `build_obj_mm_manifest.py`: Build OBJ manifest with extents, triangles, SHA1.  
- `attach_mesh_from_manifest_mm.py`: Attach OBJ data to Rebrickable JSONL.  
- `validate_parts_jsonl.py`: Schema and mesh validation.  
- `prepare_vector_index_inputs.py`: Create indexable docs + features.  
- `build_embeddings.py`: Generate embeddings (dummy or OpenAI).  
- `query_semantic.py`: Local semantic search with NumPy cosine + filters.

---

## Next Steps (Milestone: Marketplace Integration)

1. **Marketplace Adapters**  
   - `marketplaces/bricklink_adapter.py`  
   - `marketplaces/brickowl_adapter.py`  
   Normalize part IDs, names, colors, and offers into a consistent NDJSON format.

2. **ID Mapping**  
   - `scripts/build_part_id_map.py`  
   Reconcile RB ↔ BrickLink ↔ BrickOwl IDs using normalization + heuristics.

3. **Merge Market Data**  
   - `scripts/merge_market_data.py`  
   Enrich canonical parts JSONL with marketplace IDs, URLs, offers, and add marketplace-only parts.

4. **QA & Coverage Reports**  
   - `scripts/report_market_coverage.py`  
   Summarize match rates, coverage by category, new parts added, and price/availability statistics.

---

## Roadmap

- **Axis Normalization**: Standardize extents into canonical L/W/H.  
- **Connectivity Stubs**: Add `interfaces[]` (studs, tubes, axle holes, pin holes).  
- **Marketplace Expansion**: Integrate live API calls for BrickLink and BrickOwl.  
- **Vector DB Integration**: Move embeddings into Pinecone or FAISS for millisecond queries.  
- **Instruction Generator**: Sequential build instructions from assembly graphs.  
- **UI Layer**: Natural-language query → buildable instructions → sourcing.

---

## How to Rebuild the Pipeline

1. **Generate mesh manifest**  
   ```powershell
   python -u .\phase0_backend\scripts\build_obj_mm_manifest.py
