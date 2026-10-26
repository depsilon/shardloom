# SPDX-License-Identifier: Apache-2.0
"""Corruption controls for the corpus, reusable inputs and diagnostic summaries."""
from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "benchmarks" / "traditional_analytics"))
sys.path.insert(0, str(ROOT / "scripts"))

from query_surface.corpus import corpus
from query_surface.datasets import DatasetSpec, SCHEMAS, GENERATOR_VERSION, file_hash
from query_surface.diagnostics import extract
from query_surface.evidence import identity, no_fallback, preparation_fields, validate_dataset, validate_prepared
from query_surface.reference import scalar
from query_surface.report import summarize
from query_surface.semantics import ResultType, schema_record
from query_surface.specification import Expression


def write(path, value):
    path.write_text(json.dumps(value))
    return path


class QueryEvidenceTests(unittest.TestCase):
    def test_unsafe_or_ambiguous_preparation_cannot_be_certified(self):
        good = {"status": "success", "fallback": {"attempted": False, "allowed": False, "engine": None},
            "fields": [{"key": k, "value": v} for k, v in {
                "vortex_ingest_performed": "true", "fallback_attempted": "false", "external_engine_invoked": "false"}.items()]}
        preparation_fields(good)
        for changed in ("duplicate", "allowed", "attempted", "hidden", "missing"):
            value = deepcopy(good)
            if changed == "duplicate":
                value["fields"].append(value["fields"][0])
            elif changed in {"allowed", "attempted"}:
                value["fallback"][changed] = True
            elif changed == "hidden":
                value["fields"].append({"key": "nested_external_engine_invoked", "value": "true"})
            else:
                value["fields"].pop()
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                preparation_fields(value)
        for value in (0, "False", None, True):
            with self.assertRaises(ValueError):
                no_fallback({"fallback_attempted": value, "external_engine_invoked": False})

    def fixture(self, root):
        spec = DatasetSpec(fact_rows=0, entity_rows=1, product_rows=1, relationship_rows=0, calendar_rows=1)
        manifest = {"generator": GENERATOR_VERSION, "spec": spec.__dict__, "logical_identity": spec.identity,
                    "layout": "sorted", "tables": {}}
        prepared = {"logical_identity": spec.identity, "complete_independent_readback": True,
                    "sources": {}, "inputs": [], "receipts": []}
        def receipt(prefix, command, output):
            stdout, stderr = prefix.with_suffix(".stdout.json"), prefix.with_suffix(".stderr.txt")
            write(stdout, output)
            stderr.write_text("")
            record = {"returncode": 0, "guard_failures": [], "command": command,
                "stdout": str(stdout), "stderr": str(stderr), "stdout_sha256": file_hash(stdout), "stderr_sha256": file_hash(stderr)}
            if command[-2] == "--job":
                record["job_sha256"] = file_hash(Path(command[-1]))
            return write(prefix.with_suffix(".receipt.json"), record), stdout
        for table, schema in SCHEMAS.items():
            count = spec.counts()[table]
            source, native = root / (table + ".arrow"), root / (table + ".vortex")
            source.write_bytes(b"fixture transport placeholder; never executed")
            native.write_bytes(b"native placeholder; never executed")
            manifest["tables"][table] = {"rows": count, "schema": schema_record(schema),
                "files": [{"path": source.name, "rows": count, "bytes": source.stat().st_size, "sha256": file_hash(source)}]}
            prepared["sources"][table] = [str(native)]
            prepared["inputs"].append(identity(native))
            fields = {"vortex_ingest_performed": "true", "fallback_attempted": "false", "external_engine_invoked": "false",
                      "source_path": str(source), "reopen_row_count": str(count)}
            prep, _ = receipt(root / ("prepare-" + table), ["shardloom", "vortex-prepare", str(source), str(native)],
                {"status": "success", "fallback": {"attempted": False, "allowed": False, "engine": None},
                 "fields": [{"key": k, "value": v} for k, v in fields.items()]})
            job = write(root / ("readback-" + table + ".job.json"),
                {"kind": "readback", "source_paths": [str(source)], "native_path": str(native)})
            _, result = receipt(root / ("readback-" + table), ["python", "worker", "--job", str(job)],
                {"status": "passed", "checked_rows": count, "source_rows": count, "schema": schema_record(schema),
                 "comparison_engine_invoked": False})
            prepared["receipts"].append({"prepare": str(prep), "readback": str(result)})
        write(root / "dataset.json", manifest)
        write(root / "prepared.json", prepared)
        return manifest, prepared

    def test_reused_prepared_inputs_require_exact_readback_and_hash_coverage(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, prepared = self.fixture(root)
            validate_dataset(root)
            validate_prepared(root / "prepared.json", root, manifest)
            for changed in ("source", "duplicate", "missing", "schema", "wrong_table"):
                value = deepcopy(prepared)
                current = deepcopy(manifest)
                if changed == "source":
                    value["sources"]["fact"] = [str(root / "other.vortex")]
                elif changed == "duplicate":
                    value["receipts"][0] = value["receipts"][1]
                elif changed == "missing":
                    value["inputs"].pop()
                elif changed == "schema":
                    current["tables"]["fact"]["schema"] = []
                else:
                    value["sources"]["fact"], value["sources"]["entities"] = value["sources"]["entities"], value["sources"]["fact"]
                write(root / "prepared.json", value)
                with self.subTest(changed=changed), self.assertRaises(ValueError):
                    validate_prepared(root / "prepared.json", root, current)
            write(root / "prepared.json", prepared)
            job = root / "readback-fact.job.json"
            job.write_text(job.read_text() + " ")
            with self.assertRaisesRegex(ValueError, "job changed"):
                validate_prepared(root / "prepared.json", root, manifest)

    def test_dataset_duplicate_members_schema_and_bytes_cannot_be_hidden(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, _ = self.fixture(root)
            for changed in ("member", "schema", "bytes", "tables"):
                value = deepcopy(manifest)
                if changed == "member":
                    value["tables"]["fact"]["files"] *= 2
                elif changed == "schema":
                    value["tables"]["fact"]["schema"] = []
                elif changed == "bytes":
                    value["tables"]["fact"]["files"][0]["bytes"] += 1
                else:
                    del value["tables"]["entities"]
                write(root / "dataset.json", value)
                with self.subTest(changed=changed), self.assertRaises(ValueError):
                    validate_dataset(root)

    def test_summary_keeps_proposed_unexecuted_failures_and_missing_clocks(self):
        cases = corpus()
        one = next(case for case in cases if case.relation is not None)
        proposed = next(case for case in cases if case.relation is None)
        observations = [{"case_id": one.id, "surface": "python_sql", "status": "passed"}]
        summary = summarize(cases, observations, ("python_sql",))
        self.assertEqual(summary["families"][one.family]["correct_engine_clock_samples"], 0)
        self.assertIsNone(summary["families"][one.family]["engine_seconds_sum_of_recorded_correct_calls"])
        for changed in (observations * 2, [{**observations[0], "status": "made_up"}],
                        [{**observations[0], "case_id": proposed.id}],
                        [{**observations[0], "engine_execution_seconds": True}]):
            with self.assertRaises(ValueError):
                summarize(cases, changed, ("python_sql",))
        for surfaces in ((), ("python_sql", "python_sql"), ("polars",)):
            with self.assertRaises(ValueError):
                summarize(cases, [], surfaces)
        failed = summarize(cases, [{**observations[0], "status": "timed_out"}], ("python_sql",))
        self.assertEqual(failed["families"][one.family]["case_surface_outcomes"]["timed_out"], 1)

    def test_work_counters_keep_missing_and_uninstrumented_distinct_from_zero(self):
        observed = extract({"execution_resource_observed_spill_bytes": "0",
                            "resident_decoded_byte_accounting": "not_instrumented"})
        self.assertEqual(observed["counters"]["execution_resource_observed_spill_bytes"]["value"], 0)
        self.assertIsNone(observed["counters"]["local_primitive_rows_scanned"]["value"])
        self.assertTrue(all(value is None for value in observed["whole_query_bytes"].values()))

    def test_reference_coalesce_does_not_evaluate_unselected_errors(self):
        bad = Expression.literal(1).binary("mod", 0)
        good = Expression.call("coalesce", (Expression.literal(7), bad), ResultType("int64", False))
        self.assertEqual(scalar(good, {}), 7)
        needed = Expression.call("coalesce", (Expression.literal(None), bad), ResultType("int64", True))
        with self.assertRaises(ZeroDivisionError):
            scalar(needed, {})


if __name__ == "__main__":
    unittest.main()
