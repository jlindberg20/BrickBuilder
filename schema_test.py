import json, jsonschema

schema_path = r'phase0_backend/model_record.schema.json'
with open(schema_path, 'r', encoding='utf-8') as f:
    schema = json.load(f)

example = {
    "id":"model:ldraw:car",
    "type":"model",
    "source_ids": {
        "ldraw": { "root_file": "data/raw/ldraw/models/car.ldr", "file_stem": "car" }
    },
    "name":"Car (POC)",
    "metadata": { "piece_count": 0 },
    "geometry": {
        "ldraw": {
            "root_file": "data/raw/ldraw/models/car.ldr",
            "scale": {"units":"LDU","to_mm":0.4}
        }
    },
    "bom": [],
    "steps": None,
    "instructions": { "kind":"ldraw", "source":"data/raw/ldraw/models/car.ldr" }
}

jsonschema.validate(example, schema)
print("model_record.schema.json: OK")
