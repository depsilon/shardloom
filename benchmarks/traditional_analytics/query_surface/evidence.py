# SPDX-License-Identifier: Apache-2.0
"""Validate reusable fixture/readback evidence before admitting a query cohort."""
from __future__ import annotations

from pathlib import Path

from native_workflow_protocol import strict_json
from .datasets import DatasetSpec, GENERATOR_VERSION, LAYOUTS, SCHEMAS, file_hash
from .semantics import schema_record


def generation(path):
    value = path.stat()
    return {"device": value.st_dev, "inode": value.st_ino, "bytes": value.st_size,
            "mtime_ns": value.st_mtime_ns, "ctime_ns": value.st_ctime_ns}


def identity(path):
    before = generation(path)
    digest = file_hash(path)
    if generation(path) != before:
        raise ValueError(f"file changed while hashing: {path}")
    return {"path": str(path), "generation": before, "sha256": digest}


def no_fallback(fields):
    for suffix in ("fallback_attempted", "external_engine_invoked"):
        values = [value for key, value in fields.items() if key.endswith(suffix)]
        if not values or any(value is not False and value != "false" for value in values):
            raise ValueError(f"missing or unsafe native evidence: {suffix}")


def preparation_fields(envelope):
    fallback = envelope.get("fallback")
    if (envelope.get("status") != "success" or not isinstance(fallback, dict)
            or fallback.get("attempted") is not False or fallback.get("allowed") is not False
            or fallback.get("engine") is not None):
        raise ValueError("native preparation lacks successful no-fallback evidence")
    entries = envelope.get("fields")
    if not isinstance(entries, list):
        raise ValueError("native preparation fields must be an array")
    fields = {}
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get("key"), str) or "value" not in entry:
            raise ValueError("invalid native preparation field")
        if entry["key"] in fields:
            raise ValueError("duplicate native preparation field")
        fields[entry["key"]] = entry["value"]
    no_fallback(fields)
    if fields.get("vortex_ingest_performed") != "true":
        raise ValueError("native Vortex preparation did not complete")
    return fields


def validate_dataset(path):
    manifest_path = path / "dataset.json"
    manifest = strict_json(manifest_path.read_text())
    spec = DatasetSpec(**manifest["spec"])
    if (manifest["generator"] != GENERATOR_VERSION or manifest["logical_identity"] != spec.identity
            or manifest["layout"] not in LAYOUTS or set(manifest["tables"]) != set(spec.counts())):
        raise ValueError("dataset generator, logical identity, layout or tables differ")
    inputs, seen = [identity(manifest_path)], set()
    for table, count in spec.counts().items():
        item = manifest["tables"][table]
        if (type(item["rows"]) is not int or item["rows"] != count
                or item["schema"] != schema_record(SCHEMAS[table]) or not item["files"]
                or any(type(member["rows"]) is not int or member["rows"] < 0 for member in item["files"])
                or sum(member["rows"] for member in item["files"]) != count):
            raise ValueError("dataset row counts or logical schema disagree")
        for member in item["files"]:
            name = member["path"]
            if not isinstance(name, str) or Path(name).name != name or name in seen:
                raise ValueError("fixture members must be unique plain local filenames")
            seen.add(name)
            actual = identity(path / name)
            if actual["sha256"] != member["sha256"] or actual["generation"]["bytes"] != member["bytes"]:
                raise ValueError("immutable input digest or size differs")
            inputs.append(actual)
    return spec, manifest, inputs


def _checked_receipt(path, frozen):
    record = strict_json(path.read_text())
    if record.get("returncode") != 0 or record.get("guard_failures") != []:
        raise ValueError("reused preparation/readback receipt did not pass")
    frozen.append(identity(path))
    for output in ("stdout", "stderr"):
        item = identity(Path(record[output]))
        if item["sha256"] != record[output + "_sha256"]:
            raise ValueError("reused preparation/readback output changed")
        frozen.append(item)
    return record, strict_json(Path(record["stdout"]).read_text())


def validate_prepared(path, dataset, manifest):
    """Require one exact hashed source and complete readback for every input part.

    Receipts are local reproducibility evidence, not authenticated signatures.
    Original v1 packets remain readable; their worker jobs lacked a recorded
    job hash. Current packets additionally bind that job to its receipt.
    """
    prepared = strict_json(path.read_text())
    if (prepared["logical_identity"] != manifest["logical_identity"]
            or prepared["complete_independent_readback"] is not True
            or set(prepared["sources"]) != set(manifest["tables"])):
        raise ValueError("prepared inputs do not have the selected logical identity/readback")
    frozen = [identity(path)]
    paths = [name for members in prepared["sources"].values() for name in members]
    input_paths = [item["path"] for item in prepared["inputs"]]
    if len(set(paths)) != len(paths) or len(set(input_paths)) != len(input_paths) or set(paths) != set(input_paths):
        raise ValueError("prepared source paths do not exactly match the hashed input inventory")
    if len(prepared["receipts"]) != len(paths):
        raise ValueError("prepared source/readback coverage differs")
    expected = {}
    for table, record in manifest["tables"].items():
        if len(prepared["sources"][table]) != len(record["files"]):
            raise ValueError("prepared table parts differ from the source layout")
        for native, source in zip(prepared["sources"][table], record["files"]):
            if not Path(native).is_absolute():
                raise ValueError("prepared source must be an absolute path")
            expected[native] = (str(dataset / source["path"]), source["rows"], record["schema"])
    for item in prepared["inputs"]:
        if identity(Path(item["path"])) != item:
            raise ValueError("frozen prepared input changed")
    checked = set()
    for refs in prepared["receipts"]:
        prep, envelope = _checked_receipt(Path(refs["prepare"]), frozen)
        fields = preparation_fields(envelope)
        stdout = Path(refs["readback"])
        if not stdout.name.endswith(".stdout.json"):
            raise ValueError("readback output is not a supervised worker result")
        prefix = stdout.with_name(stdout.name.removesuffix(".stdout.json"))
        receipt, result = _checked_receipt(prefix.with_suffix(".receipt.json"), frozen)
        command = receipt["command"]
        if len(command) < 2 or command[-2] != "--job" or receipt["stdout"] != str(stdout):
            raise ValueError("readback receipt does not bind its worker job/output")
        job_path = Path(command[-1])
        job_identity = identity(job_path)
        if "job_sha256" in receipt and receipt["job_sha256"] != job_identity["sha256"]:
            raise ValueError("readback job changed after execution")
        frozen.append(job_identity)
        job = strict_json(job_path.read_text())
        native = job["native_path"]
        if native not in expected or native in checked or job["kind"] != "readback":
            raise ValueError("readback has a duplicate or unrelated native source")
        source, rows, schema = expected[native]
        if (job["source_paths"] != [source] or prep["command"][1:4] != ["vortex-prepare", source, native]
                or fields.get("source_path") != source or fields.get("reopen_row_count") != str(rows)
                or result.get("status") != "passed" or type(result.get("checked_rows")) is not int
                or result.get("checked_rows") != rows or result.get("source_rows") != rows
                or result.get("schema") != schema or result.get("comparison_engine_invoked") is not False):
            raise ValueError("prepared source/readback identity, rows or schema disagree")
        checked.add(native)
    return prepared, frozen
