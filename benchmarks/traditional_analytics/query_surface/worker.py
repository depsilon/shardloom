# SPDX-License-Identifier: Apache-2.0
"""One supervised native query, independent expectation or input readback job."""
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import re
import sys
import traceback

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "python" / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from native_workflow_protocol import strict_json
from query_surface.adapters import AdapterGap, execute
from query_surface.corpus import corpus
from query_surface.datasets import DatasetSpec, file_hash
from query_surface.evidence import no_fallback
from query_surface.reference import ReferenceGap, expected
from query_surface.semantics import (
    ComparisonLimit, ResultMismatch, arrow_schema, compare, result_from_record,
    result_record, schema_record,
)


def load_native(path):
    import shardloom
    path = Path(path)
    if not path.is_absolute() or not path.is_file():
        raise ValueError("native library must be an existing absolute file")
    spec = importlib.util.spec_from_file_location("shardloom._native", path)
    if spec is None or spec.loader is None:
        raise ValueError("cannot load the declared native library")
    native = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = native
    spec.loader.exec_module(native)
    return native


def readback(job, native):
    """Compare every persisted source value with native streamed readback."""
    import pyarrow as pa
    session = native.Session(native.ExecutionResources(job["memory_bytes"], job["parallelism"]))
    checked, expected_rows, source_schema = 0, 0, None
    result = None
    try:
        plan = session.plan(native.Relation.scan(job["native_path"]))
        result = plan.iter_batches()
        def batches():
            for native_batch in result:
                with pa.RecordBatchReader.from_stream(native_batch) as reader:
                    yield from reader
        stream = iter(batches())
        actual = None
        offset = 0
        for path in job["source_paths"]:
            with pa.memory_map(path, "r") as file:
                reader = pa.ipc.open_file(file)
                if source_schema is None:
                    source_schema = reader.schema
                elif reader.schema != source_schema:
                    raise ResultMismatch("source parts have different schemas")
                for i in range(reader.num_record_batches):
                    wanted, consumed = reader.get_batch(i), 0
                    expected_rows += wanted.num_rows
                    while consumed < wanted.num_rows:
                        if actual is None or offset == actual.num_rows:
                            actual, offset = next(stream, None), 0
                            if actual is None:
                                raise ResultMismatch("native input readback is truncated")
                            if arrow_schema(actual.schema) != arrow_schema(source_schema):
                                raise ResultMismatch("native preparation changed the complete logical schema")
                            if actual.num_rows == 0:
                                continue
                        count = min(wanted.num_rows - consumed, actual.num_rows - offset)
                        # Normalize transport offset widths only, after exact
                        # logical dtype/nullability comparison above.
                        observed = actual.slice(offset, count).cast(source_schema)
                        if not wanted.slice(consumed, count).equals(observed):
                            raise ResultMismatch(f"native preparation changed values at row {checked}")
                        consumed, offset, checked = consumed + count, offset + count, checked + count
        if actual is not None and offset != actual.num_rows:
            raise ResultMismatch("native input readback has extra rows")
        if any(batch.num_rows for batch in stream):
            raise ResultMismatch("native input readback has extra batches")
        if result.report is None:
            raise ResultMismatch("native readback lacks completed no-fallback evidence")
        no_fallback(dict(result.report))
        if expected_rows == 0:
            # A stream may emit no batch for an empty input; separately inspect
            # the complete empty owner so its schema is still checked.
            with pa.RecordBatchReader.from_stream(plan.collect()) as empty:
                if arrow_schema(empty.schema) != arrow_schema(source_schema) or empty.read_all().num_rows:
                    raise ResultMismatch("empty native input schema or values differ")
        return {"status": "passed", "checked_rows": checked, "source_rows": expected_rows,
                "schema": schema_record(arrow_schema(source_schema)),
                "native_report": dict(result.report),
                "method": "all source Arrow values versus independently streamed native Vortex readback",
                "comparison_engine_invoked": False}
    finally:
        if result is not None:
            result.close()
        session.close()


def run(job):
    if job["kind"] == "readback":
        return readback(job, load_native(job["library"]))
    item = next((case for case in corpus(interactions="all") if case.id == job["case_id"]), None)
    if item is None or item.identity != job["case_identity"]:
        raise ValueError("frozen case identity differs from the current corpus")
    if job["kind"] == "reference":
        result = expected(item, DatasetSpec(**job["dataset"]), max_output_rows=job["max_output_rows"])
        return {"status": "passed", "case_id": item.id, "case_identity": item.identity,
                "reference": result_record(result), "method": "independent_standard_library_over_seeded_records",
                "comparison_engine_invoked": False}
    if job["kind"] not in {"execute", "polars"}:
        raise ValueError("unknown worker job kind")
    path = Path(job["reference_path"])
    if file_hash(path) != job["reference_sha256"]:
        raise ValueError("frozen independent expectation changed")
    reference = strict_json(path.read_text())
    if reference["case_identity"] != item.identity or reference["status"] != "passed":
        raise ValueError("reference does not belong to this exact case")
    wanted = result_from_record(reference["reference"])
    baseline = job["kind"] == "polars"
    comparison = compare
    if baseline:
        from query_surface.polars_adapter import execute as polars_execute, compare_result
        actual, report = polars_execute(item.relation, job["sources"], parallelism=job["parallelism"])
        comparison = compare_result
    else:
        actual, report = execute(item.relation, job["sources"], job["surface"],
                                 native=load_native(job["library"]), binary=job["binary"],
                                 memory_bytes=job["memory_bytes"], parallelism=job["parallelism"], timeout=job["timeout"])
    result = {"case_id": item.id, "case_identity": item.identity, "observation": report,
              "actual": result_record(actual), "comparison_engine_invoked": baseline}
    try:
        comparison(wanted, actual, item.contract)
        result["status"] = "passed"
    except ResultMismatch as error:
        result.update(status="incorrect", detail=str(error))
    except ComparisonLimit as error:
        result.update(status="comparison_inconclusive", detail=str(error))
    return result


def failure(error):
    if isinstance(error, AdapterGap):
        status = "adapter_gap"
    elif isinstance(error, ReferenceGap):
        status = "reference_gap"
    elif isinstance(error, ResultMismatch):
        status = "incorrect"
    elif getattr(error, "envelope", None) is not None:
        status = "engine_rejected" if error.envelope.status == "unsupported" else "engine_error"
    elif isinstance(error, RuntimeError) and re.search(r"\bSL_[A-Z_]+:", str(error)):
        status = "engine_error"
    else:
        status = "harness_error"
    return {"status": status, "exception": type(error).__name__, "detail": str(error),
            "traceback": traceback.format_exc(), "diagnostic_codes": re.findall(r"\bSL_[A-Z_]+\b", str(error)),
            "failure_stage": "public_request_or_worker; exact bind/execute stage unavailable",
            "raw_report": error.envelope.raw if getattr(error, "envelope", None) is not None else None,
            "comparison_engine_invoked": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job", type=Path, required=True)
    args = parser.parse_args()
    job = None
    try:
        job = strict_json(args.job.read_text())
        result = run(job)
    except Exception as error:
        result = failure(error)
        if job and job.get("kind") == "polars":
            result["comparison_engine_invoked"] = True
            if type(error).__module__.startswith("polars."):
                result["status"] = "engine_error"
    print(json.dumps(result, allow_nan=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
