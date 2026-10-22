# SPDX-License-Identifier: Apache-2.0
"""Independent scalar/schema specification for the finite exchange fixtures.

This program never reads or decodes Vortex values. It hashes producer messages
and separately generates complete reference rows from the formulas below.
Run under the shared local storage/process guard after the upstream producer.
"""

import argparse
import gzip
import hashlib
import json
from pathlib import Path


CASES = [
    ("empty", 0), ("tiny", 7), ("nullable_plain", 16_387),
    ("all_null", 65_533), ("constant", 65_533),
    ("dictionary_small", 4_099), ("dictionary_large", 65_533),
    ("dictionary_other", 65_533), ("run_end", 65_533),
    ("bitpacked", 65_533), ("wide_text", 8_195),
    ("nested", 4_099), ("sliced_shared", 16_387),
]


def dictionary_value(row, domain, variant):
    key = (row * 19 + 3) % domain
    if row % 97 == 0 or key == 0:
        return None
    if key == 1:
        return ""
    return f"dict-{variant}-{key:03}-λ\0猫-" + "prefix/" * 4


def reference(name, ordinal):
    row = ordinal + 7 if name == "sliced_shared" else ordinal
    key = (1 << 52) + 17 * row
    if name in ("empty", "tiny", "nullable_plain"):
        return {
            "event_key": key,
            "measure": row % 201 - 100 if row % 7 else None,
            "enabled": row % 2 == 0 if row % 11 else None,
            "caption": ("" if row % 13 == 0 else f"row-{row}-λ\0猫") if row % 17 else None,
        }
    if name == "all_null":
        return {"absent_text": None, "absent_number": None}
    if name == "constant":
        return {"fixed_flag": True, "fixed_text": "constant λ\0猫", "fixed_number": -((1 << 53) + 17)}
    if name.startswith("dictionary_"):
        return {"event_key": key, "category": dictionary_value(
            row, 17 if name == "dictionary_small" else 257,
            "other" if name == "dictionary_other" else "primary")}
    if name == "run_end":
        return {"event_key": key, "run_value": (row // 97) % 201 - 100 if (row // 97) % 11 else None}
    if name == "bitpacked":
        return {"event_key": key, "packed_value": (row * 17) % 31}
    if name == "wide_text":
        value = f"row-{row:05}-" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" * 14 + "-猫"
        return {"event_key": key, "wide_caption": value if row % 17 else None}
    if name == "nested":
        return {
            "event_key": key,
            "attributes": {
                "category": dictionary_value(row, 17, "nested"),
                "enabled": row % 2 == 0 if row % 11 else None,
            },
            "samples": [n - 200 if n % 5 else None for n in range(2 * row, 2 * row + 2)] if row % 13 else None,
        }
    if name == "sliced_shared":
        value = dictionary_value(row, 257, "shared")
        return {"event_key": key, "left_text": value, "right_text": value}
    raise ValueError(name)


def primitive(kind, nullable=False):
    return {"Primitive": [kind, nullable]}


def structure(fields, nullable=False):
    return {"Struct": [{"names": [name for name, _ in fields],
                         "dtypes": [dtype for _, dtype in fields]}, nullable]}


def fields(name):
    key = ("event_key", primitive("i64"))
    if name in ("empty", "tiny", "nullable_plain"):
        return [key, ("measure", primitive("i32", True)), ("enabled", {"Bool": True}), ("caption", {"Utf8": True})]
    if name == "all_null":
        return [("absent_text", {"Utf8": True}), ("absent_number", primitive("i64", True))]
    if name == "constant":
        return [("fixed_flag", {"Bool": False}), ("fixed_text", {"Utf8": False}), ("fixed_number", primitive("i64"))]
    if name.startswith("dictionary_"):
        return [key, ("category", {"Utf8": True})]
    if name == "run_end":
        return [key, ("run_value", primitive("i64", True))]
    if name == "bitpacked":
        return [key, ("packed_value", primitive("i32"))]
    if name == "wide_text":
        return [key, ("wide_caption", {"Utf8": True})]
    if name == "nested":
        return [key, ("attributes", structure([("category", {"Utf8": True}), ("enabled", {"Bool": True})])),
                ("samples", {"List": [primitive("i32", True), True]})]
    if name == "sliced_shared":
        return [key, ("left_text", {"Utf8": True}), ("right_text", {"Utf8": True})]
    raise ValueError(name)


def json_bytes(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


def identity(path):
    data = path.read_bytes()
    return {"file": path.name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def generate(root):
    # Both dictionary domains must exercise every code, including NULL/empty values.
    for domain in (17, 257):
        assert {(row * 19 + 3) % domain for row in range(2 * domain) if row % 97} == set(range(domain))
    recipes = []
    recipe_root = Path(__file__).resolve().parent
    for name in ("reference.py", "producer/Cargo.toml", "producer/Cargo.lock", "producer/src/main.rs"):
        record = identity(recipe_root / name)
        record["file"] = name
        recipes.append(record)
    manifest = {"version": 1, "provider": "unpatched-vortex-0.85.0", "recipes": recipes, "cases": []}
    for name, count in CASES:
        schema = fields(name)
        dtype = identity(root / f"{name}.dtype.vxipc")
        array = identity(root / f"{name}.array.vxipc")
        if dtype["bytes"] + array["bytes"] > 64 << 20:
            raise ValueError("native fixture exceeds declared IPC ceiling")
        projected = {
            "all": [(field, field, typ) for field, typ in schema],
            "first": [(schema[0][0], schema[0][0], schema[0][1])],
            "last_first_renamed": [(schema[-1][0], "last_value", schema[-1][1]),
                                   (schema[0][0], "first_value", schema[0][1])],
        }
        queries = []
        for query, projection in projected.items():
            path = root / f"{name}.{query}.expected.jsonl.gz"
            digest, size = hashlib.sha256(), 0
            with path.open("xb") as output:
                with gzip.GzipFile(fileobj=output, mode="wb", filename="", mtime=0) as stream:
                    for ordinal in range(count):
                        row = reference(name, ordinal)
                        value = {alias: row[field] for field, alias, _ in projection}
                        encoded = json_bytes(value) + b"\n"
                        size += len(encoded)
                        if size > 8 << 20:
                            raise ValueError(f"{name}/{query} exceeds existing JSON result boundary")
                        digest.update(encoded)
                        stream.write(encoded)
            with gzip.open(path, "rb") as stream:
                restored = stream.read()
            assert len(restored) == size and hashlib.sha256(restored).hexdigest() == digest.hexdigest()
            queries.append({"id": query, "projection": [{"source": src, "alias": alias} for src, alias, _ in projection],
                            "dtype": structure([(alias, typ) for _, alias, typ in projection]),
                            "rows": count, "jsonl_bytes": size, "jsonl_sha256": digest.hexdigest(),
                            "reference": identity(path)})
        encoded = []
        if name.startswith("dictionary_"):
            encoded = [{"path": ["category"], "arrow": "Dictionary", "keys": "UInt16"}]
        elif name == "nested":
            encoded = [{"path": ["attributes", "category"], "arrow": "Dictionary", "keys": "UInt16"}]
        elif name == "sliced_shared":
            encoded = [{"path": [field], "arrow": "Dictionary", "keys": "UInt16"} for field in ("left_text", "right_text")]
        elif name == "run_end":
            encoded = [{"path": ["run_value"], "arrow": "RunEndEncoded", "ends": "Int32"}]
        native = [{"path": field["path"], "encoding": "vortex.dict" if field["arrow"] == "Dictionary" else "vortex.runend"}
                  for field in encoded]
        if name in ("all_null", "constant"):
            native.extend({"path": [field], "encoding": "vortex.constant"} for field, _ in schema)
        if name == "bitpacked":
            native.append({"path": ["packed_value"], "encoding": "fastlanes.bitpacked"})
        manifest["cases"].append({"id": name, "rows": count, "dtype": structure(schema),
                                  "native_preserving_fields": native,
                                  "arrow_preserving_fields": encoded,
                                  "dtype_message": dtype, "array_message": array, "queries": queries})
    path = root / "fixture-manifest.json"
    with path.open("xb") as stream:
        stream.write(json.dumps(manifest, indent=2, ensure_ascii=False).encode() + b"\n")
    return identity(path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    print(json.dumps(generate(args.root)))
