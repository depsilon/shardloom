# SPDX-License-Identifier: Apache-2.0
"""Parquet preparation remains paired to original Arrow IPC readback evidence."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "benchmarks" / "traditional_analytics"))
sys.path.insert(0, str(ROOT / "scripts"))

from query_surface.datasets import file_hash
from query_surface.evidence import identity, validate_prepared
from query_surface import run as query_run
import test_query_surface_evidence as fixtures
from test_query_surface_evidence import write


class ExpandedPreparationTests(unittest.TestCase):
    def parquet_fixture(self, root):
        manifest, prepared = fixtures.QueryEvidenceTests().fixture(root)
        parquet_sources = {}
        prepared["physical_sources"] = {}
        for table in manifest["tables"]:
            source = root / f"{table}.parquet"
            source.write_bytes(f"equivalent parquet fixture {table}".encode())
            parquet_sources[table] = [str(source)]
            prepared["physical_sources"][table] = [identity(source)]

        for refs in prepared["receipts"]:
            prep_path = Path(refs["prepare"])
            prep_receipt = json.loads(prep_path.read_text())
            old_source = prep_receipt["command"][2]
            table = Path(old_source).stem
            parquet = parquet_sources[table][0]
            prep_receipt["command"][2] = parquet
            prep_receipt["command"].extend(["--input-format", "parquet"])
            stdout = Path(prep_receipt["stdout"])
            envelope = json.loads(stdout.read_text())
            for field in envelope["fields"]:
                if field["key"] == "source_path":
                    field["value"] = parquet
            write(stdout, envelope)
            prep_receipt["stdout_sha256"] = file_hash(stdout)
            write(prep_path, prep_receipt)

        write(root / "prepared.json", prepared)
        return manifest, prepared, parquet_sources

    def test_prepare_uses_parquet_for_cli_and_ipc_for_readback(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, _, parquet_sources = self.parquet_fixture(root)
            output, logs = root / "new-prepared", root / "new-logs"
            logs.mkdir()
            commands, jobs = [], []

            def fake_supervise(command, prefix, timeout, guard):
                commands.append(command)
                destination = Path(command[3])
                destination.write_bytes(b"native placeholder")
                stdout = prefix.with_suffix(".stdout.json")
                stdout.write_text(
                    json.dumps(
                        {
                            "status": "success",
                            "fallback": {"attempted": False, "allowed": False, "engine": None},
                            "fields": [
                                {"key": "vortex_ingest_performed", "value": "true"},
                                {"key": "fallback_attempted", "value": "false"},
                                {"key": "external_engine_invoked", "value": "false"},
                                {"key": "source_path", "value": command[2]},
                                {
                                    "key": "reopen_row_count",
                                    "value": str(manifest["tables"][Path(command[2]).stem]["rows"]),
                                },
                            ],
                        }
                    )
                )
                receipt = {"returncode": 0, "guard_failures": [], "stdout": str(stdout)}
                write(prefix.with_suffix(".receipt.json"), receipt)
                return receipt

            def fake_worker(job, prefix, timeout, guard):
                jobs.append(job)
                stdout = prefix.with_suffix(".stdout.json")
                stdout.write_text("{}")
                return {
                    "status": "passed",
                    "checked_rows": manifest["tables"][Path(job["source_paths"][0]).stem]["rows"],
                }, {"stdout": str(stdout)}

            args = SimpleNamespace(
                binary=Path("/runtime/shardloom"),
                library=Path("/runtime/lib.so"),
                memory_gb=1,
                parallelism=2,
                preparation_timeout=10,
            )
            with (
                patch.object(query_run, "supervise", side_effect=fake_supervise),
                patch.object(query_run, "worker", side_effect=fake_worker),
            ):
                result = query_run.prepare(
                    root, manifest, output, logs, args, object(), parquet_sources=parquet_sources
                )

            self.assertEqual(len(commands), len(jobs))
            for command, job in zip(commands, jobs):
                self.assertEqual(command[1], "vortex-prepare")
                self.assertEqual(command[command.index("--input-format") + 1], "parquet")
                self.assertEqual(command[2], parquet_sources[Path(command[2]).stem][0])
                ipc = root / f"{Path(command[2]).stem}.arrow"
                self.assertEqual(job["source_paths"], [str(ipc)])
                self.assertNotEqual(job["source_paths"][0], command[2])
            self.assertEqual(set(result["physical_sources"]), set(manifest["tables"]))

    def test_parquet_preparation_evidence_keeps_arrow_ipc_readback_pairing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, prepared, parquet_sources = self.parquet_fixture(root)
            validated, frozen = validate_prepared(
                root / "prepared.json", root, manifest, parquet_sources=parquet_sources
            )
            self.assertEqual(validated["physical_sources"], prepared["physical_sources"])
            self.assertEqual(
                len(frozen),
                len(prepared["receipts"]) * 7 + sum(map(len, prepared["physical_sources"].values())) + 1,
            )
            for refs in prepared["receipts"]:
                receipt = json.loads(Path(refs["prepare"]).read_text())
                self.assertEqual(
                    receipt["command"][receipt["command"].index("--input-format") + 1], "parquet"
                )
                readback_receipt = Path(refs["readback"]).with_name(
                    Path(refs["readback"]).name.removesuffix(".stdout.json") + ".receipt.json"
                )
                job = json.loads(Path(json.loads(readback_receipt.read_text())["command"][-1]).read_text())
                original = manifest["tables"]
                expected_ipc = next(
                    root / member["path"]
                    for table, spec in original.items()
                    for member in spec["files"]
                    if str(root / member["path"]) == job["source_paths"][0]
                )
                self.assertEqual(job["source_paths"], [str(expected_ipc)])
                self.assertNotEqual(job["source_paths"][0], receipt["command"][2])

    def test_parquet_source_identity_format_and_pairing_corruption_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, prepared, parquet_sources = self.parquet_fixture(root)
            original = deepcopy(prepared)
            mutations = ("source", "identity", "format", "report_source", "pairing")
            for changed in mutations:
                with self.subTest(changed=changed):
                    value = deepcopy(original)
                    mapping = deepcopy(parquet_sources)
                    refs = value["receipts"][0]
                    prep_path = Path(refs["prepare"])
                    prep_receipt = json.loads(prep_path.read_text())
                    prep_stdout = Path(prep_receipt["stdout"])
                    envelope = json.loads(prep_stdout.read_text())
                    readback = Path(refs["readback"])
                    readback_prefix = readback.with_name(readback.name.removesuffix(".stdout.json"))
                    readback_receipt_path = readback_prefix.with_suffix(".receipt.json")
                    readback_receipt = json.loads(readback_receipt_path.read_text())
                    job_path = Path(readback_receipt["command"][-1])
                    job = json.loads(job_path.read_text())
                    if changed == "source":
                        mapping["entities"] = mapping["entities"][:]
                        mapping["entities"][0] = mapping["fact"][0]
                    elif changed == "identity":
                        value["physical_sources"]["fact"][0]["sha256"] = "0" * 64
                    elif changed == "format":
                        index = prep_receipt["command"].index("--input-format")
                        prep_receipt["command"][index + 1] = "arrow-ipc"
                        write(prep_path, prep_receipt)
                    elif changed == "report_source":
                        for field in envelope["fields"]:
                            if field["key"] == "source_path":
                                field["value"] = str(root / "wrong.parquet")
                        write(prep_stdout, envelope)
                        prep_receipt["stdout_sha256"] = file_hash(prep_stdout)
                        write(prep_path, prep_receipt)
                    else:
                        job["source_paths"] = [mapping["fact"][0]]
                        write(job_path, job)
                        readback_receipt["job_sha256"] = file_hash(job_path)
                        write(readback_receipt_path, readback_receipt)
                    write(root / "prepared.json", value)
                    with self.assertRaises(ValueError):
                        validate_prepared(root / "prepared.json", root, manifest, parquet_sources=mapping)
                    # Restore immutable fixture receipts before the next corruption case.
                    self.parquet_fixture(root)


if __name__ == "__main__":
    unittest.main()
