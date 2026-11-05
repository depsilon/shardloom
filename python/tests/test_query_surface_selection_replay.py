# SPDX-License-Identifier: Apache-2.0
"""Reject changed single-pass contracts, public source proofs and rehashed results."""

from copy import deepcopy
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "benchmarks/traditional_analytics"))
from query_surface.corpus import corpus
from query_surface.expanded_packet import canonical_blob_bytes
from query_surface.expanded_replay import lines, sha
from query_surface.expanded_selection_analysis import checked_clocks, work_counters
from query_surface.expanded_selection_proof import PROTOCOL, python_core_counts, validate_capture
from query_surface.expanded_selection_replay import checked_case, intake, read_proofs

PACKETS = ROOT / "docs/benchmarks/evidence/native-expanded-query-2026-11-03"
ARCHIVE = PACKETS / "selection-retention"


class SelectionReplayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        (cls.index, cls.original, cls.prior, cls.docs, cls.proofs, cls.protocol,
         cls.previous_runtime) = intake(ARCHIVE, PACKETS / "packet", PACKETS / "shared-mechanisms")
        from query_surface.expanded_selection_replay import read_proofs
        from query_surface.expanded_replay import metadata
        _, cls.text = read_proofs(ARCHIVE, cls.index)
        _, cls.hashes = metadata(PACKETS / "packet", cls.original)
        cls.records = [next(lines(root, "cases-0000.jsonl.xz", idx["parts"][0], 250))
                       for root, idx in ((PACKETS / "packet", cls.original),
                                         (PACKETS / "shared-mechanisms", cls.prior), (ARCHIVE, cls.index))]
        cls.spec = cls.docs["design.json"]["cases"][0]
        cls.item = next(item for item in corpus(interactions="all", include_pending=False) if item.id == cls.spec["id"])

    def capture(self, name=None, mutate=None):
        proofs = self.proofs
        if name is not None:
            proofs = dict(proofs)
            proofs[name] = deepcopy(proofs[name])
            mutate(proofs[name])
        return validate_capture(self.index, proofs, self.index["members"], self.text, self.hashes)

    def case(self, current=None):
        reference, prior, saved = self.records
        return checked_case(0, reference, prior, saved if current is None else current, self.spec, self.item,
                            self.docs["inputs.json"], self.docs["native-preparation/complete.json"],
                            self.docs["design.json"]["execution"], self.protocol, self.previous_runtime)

    def changed_response(self, mutate):
        current = deepcopy(self.records[2])
        entry = current["native"]
        file = entry["packet"]["files"]["one.stdout.json"]
        response = json.loads(file["utf8"])
        mutate(response)
        raw = json.dumps(response, separators=(",", ":"), allow_nan=False).encode()
        file.update(utf8=raw.decode(), bytes=len(raw), sha256=sha(raw))
        entry["json_sha256"] = sha(canonical_blob_bytes(entry["packet"]))
        return current

    def test_complete_source_and_public_package_contract(self):
        self.assertEqual(self.capture()["expected_attempts"], 10000)
        row = self.case()
        self.assertEqual(row["candidate"]["correct_rows"], row["baseline"]["correct_rows"])

    def test_repeat_or_changed_baseline_policy_is_rejected(self):
        for key, value in (("attempts_per_case", 2), ("warmups", 1), ("requested_repeats", 1),
                           ("external_engine_attempts", 1), ("baseline_engine_nanos", 1),
                           ("candidate_python_change_explicit", False), ("benchmark_code_and_clocks_byte_identical", False)):
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.capture(PROTOCOL, lambda p: p.update({key: value}))

    def test_incomplete_results_and_posthash_are_rejected(self):
        for key, value in (("total", 9999), ("posthash_verified", False), ("outcomes", {"passed": 9999, "failed": 1})):
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.capture("combined-10k-r1/complete.json", lambda p: p.update({key: value}))

    def test_process_cleanup_and_lightweight_query_guard_are_required(self):
        for key, value in (("process_group_drained", False), ("direct_child_reaped", False),
                           ("query_workload", False), ("timed_out", True), ("build_inventory_policy", "during")):
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.capture("combined-10k-guard-r1.json", lambda p: p.update({key: value}))
        with self.assertRaises(ValueError):
            self.capture("combined-10k-guard-r1.json", lambda p: p["storage_inventory_scopes"].append(
                {"scopes": ["cargo"], "child_started": True, "forced": False}))

    def test_runtime_features_and_source_are_bound(self):
        for key, value in (("source_revision", "0" * 40), ("features", ["vortex-read"]), ("profile", "debug")):
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.capture("combined-runtime-r1/manifest.json", lambda p: p.update({key: value}))
        with self.assertRaises(ValueError):
            self.capture("selection-combined-rust-acceptance-r1.json", lambda p: p["checks"][2].update(passed=1))

    def test_installed_proof_and_public_compatibility_cannot_be_dropped(self):
        with self.assertRaises(ValueError):
            self.capture("installed-combined-r1/report.json", lambda p: p["integration"].update(skipped=1))
        with self.assertRaises(ValueError):
            self.capture("combined-public-acceptance-r1.json", lambda p: p.update(canonical_classes=75))
        with self.assertRaises(ValueError):
            self.capture("facade-scope-primary-r1.json", lambda p: p["coverage"].update(qualified_global_references=1514))
        with self.assertRaises(ValueError):
            self.capture("combined-harness-r1/manifest.json", lambda p: next(iter(p["files"].values())).update(sha256="0" * 64))

    def test_original_failed_python_observation_is_preserved(self):
        with self.assertRaises(ValueError):
            self.capture("combined-public-acceptance-r1.json", lambda p: p["python_core"].update(failed_initially=0))

    def test_planned_tests_cannot_be_reported_as_executed_passes(self):
        counts = python_core_counts(self.proofs, self.text)
        self.assertEqual(counts["planned_test_count"], 1145)
        self.assertEqual(counts["reported_tests_run"], 817)
        self.assertEqual(counts["historical_inferred_pass_count"], 1086)
        self.assertIsNone(counts["successful_test_count"])
        changed = dict(self.text)
        name = "selection-combined-python-core-r2.log"
        changed[name] = changed[name].replace("Ran 817 tests", "Ran 1145 tests")
        with self.assertRaises(ValueError):
            python_core_counts(self.proofs, changed)

    def test_rehashed_incorrect_rows_fail_independent_comparison(self):
        changed = self.changed_response(lambda p: p["actual"]["rows"][0].__setitem__(
            p["actual"]["schema"][0]["name"], 987654321))
        with self.assertRaises(ValueError):
            self.case(changed)

    def test_rehashed_wrong_case_and_incomplete_consumption_are_rejected(self):
        for mutate in (lambda p: p.update(case_identity="0" * 64),
                       lambda p: p["observation"].update(complete_consumption=False)):
            with self.subTest(mutate=mutate), self.assertRaises(ValueError):
                self.case(self.changed_response(mutate))

    def test_rehashed_resource_change_is_rejected(self):
        changed = self.changed_response(lambda p: p["observation"]["fields"].update(execution_resource_declared_max_parallelism="99"))
        with self.assertRaises(ValueError):
            self.case(changed)

    def test_rehashed_engine_clock_change_is_rejected(self):
        changed = self.changed_response(lambda p: p["observation"]["clocks"].update(engine_execution_seconds=999.0))
        with self.assertRaises(ValueError):
            self.case(changed)

    def test_missing_work_is_not_invented_and_malformed_counters_fail(self):
        self.assertEqual(work_counters({}), {})
        name = "streaming_ordering_prefix_batches"
        for value in (f"{name}=-1", f"{name}=1 {name}=2", f"{name}={1 << 64}", "payload_prepared_selections=1"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                work_counters({"local_primitive_native_io_pushdown_proof_basis": value})

    def test_public_clock_boundaries_reconcile(self):
        report = json.loads(self.records[2]["native"]["packet"]["files"]["one.stdout.json"]["utf8"])["observation"]
        checked_clocks(report)
        report["clocks"]["setup_and_declaration_seconds"] += 1
        with self.assertRaises(ValueError):
            checked_clocks(report)

    def test_fragmented_receipts_require_complete_order_and_content_identity(self):
        for mutate in (lambda record: record.update(parts=record["parts"][:1]),
                       lambda record: record.update(sha256="0" * 64)):
            index = deepcopy(self.index)
            record = next(iter(index["fragmented_members"].values()))
            mutate(record)
            with self.assertRaises(ValueError):
                read_proofs(ARCHIVE, index)

    def test_proof_part_inventory_cannot_be_reordered_or_duplicated(self):
        index = deepcopy(self.index)
        index["proof_parts"][1] = index["proof_parts"][0]
        with self.assertRaises(ValueError):
            read_proofs(ARCHIVE, index)


if __name__ == "__main__":
    unittest.main()
