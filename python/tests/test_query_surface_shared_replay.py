# SPDX-License-Identifier: Apache-2.0
"""Shared-work evidence rejects changed capture contracts and rehashed result corruption."""

from copy import deepcopy
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
import zlib

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "benchmarks/traditional_analytics"))
from query_surface.expanded_packet import canonical_blob_bytes, write_part
from query_surface.expanded_replay import lines, metadata, sha
from query_surface.expanded_shared_investigations import validate_public_models, validate_reads
from query_surface.expanded_shared_analysis import validate_groups
from query_surface.expanded_shared_proof import validate_capture
from query_surface.expanded_shared_replay import intake, read_proofs, validate_result, verify
from query_surface.expanded_shared_trace import self_test

PACKETS = ROOT / "docs/benchmarks/evidence/native-expanded-query-2026-11-03"
ARCHIVE = PACKETS / "shared-mechanisms"


class SharedReplayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.index = json.loads((ARCHIVE / "index.json").read_bytes())
        cls.original = json.loads((PACKETS / "packet/index.json").read_bytes())
        cls.previous = json.loads((PACKETS / "updated-native/index.json").read_bytes())
        cls.docs, cls.hashes = metadata(PACKETS / "packet", cls.original)
        cls.proofs, cls.text = read_proofs(ARCHIVE, cls.index)
        cls.members = cls.index["members"]

    def capture(self, proofs=None):
        return validate_capture(self.index, self.proofs if proofs is None else proofs, self.members,
                                self.text, self.hashes, self.previous["members"])

    def test_valid_capture_includes_complete_source_and_package_gates(self):
        self.assertEqual(self.capture()["head"], self.index["head"])
        intake(ARCHIVE, PACKETS / "packet", PACKETS / "updated-native")

    def test_source_features_and_native_products_are_bound(self):
        for key, value in (("features", ["vortex-read"]), ("source_revision", "0" * 40)):
            with self.subTest(key=key):
                proofs = deepcopy(self.proofs)
                proofs["runtime.json"][key] = value
                with self.assertRaisesRegex(ValueError, "native release build"):
                    self.capture(proofs)
        proofs = deepcopy(self.proofs)
        proofs["runtime.json"]["products"]["_native.abi3.so"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "native product"):
            self.capture(proofs)

    def test_attempt_policy_and_previous_complete_baseline_are_fixed(self):
        for field, value in (("attempts_per_case", 2), ("warmups", 1)):
            with self.subTest(field=field):
                proofs = deepcopy(self.proofs)
                proofs["protocol.json"][field] = value
                with self.assertRaisesRegex(ValueError, "capture contract"):
                    self.capture(proofs)
        proofs = deepcopy(self.proofs)
        proofs["complete.json"]["outcomes"] = {"passed": 9999, "timed_out": 1}
        with self.assertRaisesRegex(ValueError, "capture contract"):
            self.capture(proofs)

    def test_supervision_and_observer_resumption_are_required(self):
        for field in ("query_workload", "process_group_drained", "direct_child_reaped"):
            with self.subTest(field=field):
                proofs = deepcopy(self.proofs)
                proofs["guard.json"][field] = False
                with self.assertRaisesRegex(ValueError, "supervision or isolation"):
                    self.capture(proofs)
        proofs = deepcopy(self.proofs)
        proofs["isolation.json"]["resumed_pids"] = []
        with self.assertRaisesRegex(ValueError, "supervision or isolation"):
            self.capture(proofs)

    def test_tested_source_bytes_and_test_counts_must_match(self):
        proofs = deepcopy(self.proofs)
        name = "mechanism-native-workspace-tests-r2"
        path = proofs["source-gates.json"]["receipts"][name]["source_files_checked"][0]
        proofs[name + ".json"]["changed_source_inventory"][path]["sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "tested source differs"):
            self.capture(proofs)
        proofs = deepcopy(self.proofs)
        proofs["source-gates.json"]["receipts"][name]["tests"]["passed"] -= 1
        with self.assertRaisesRegex(ValueError, "test counts differ"):
            self.capture(proofs)

    def test_installed_package_must_use_the_measured_library(self):
        proofs = deepcopy(self.proofs)
        proofs["installed.json"]["integrity"]["native_library_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "installed package differs"):
            self.capture(proofs)

    def test_rejected_public_model_trial_keeps_both_failures(self):
        validate_public_models(self.proofs)
        proofs = deepcopy(self.proofs)
        proofs["public-model-compatibility-r1/report.json"]["observations"]["candidate"]["checks"]["source_available"] = True
        with self.assertRaisesRegex(ValueError, "public model observation"):
            validate_public_models(proofs)

    def test_read_traces_reproduce_actual_consumption_and_all_releases(self):
        observations = validate_reads(PACKETS / "packet", self.original, self.docs,
                                      self.proofs, self.members, self.text)
        self.assertEqual(observations[0]["totals"], observations[1]["totals"])
        self.assertEqual(observations[2]["totals"]["returned_data_extent_bytes"], 12_473_340)
        controls = self_test()
        self.assertEqual(controls["status"], "passed")
        self.assertEqual(len(controls["controls"]), 9)

    def test_read_summary_cannot_substitute_callback_bytes_for_consumption(self):
        proofs = deepcopy(self.proofs)
        proofs["mechanism-io-policy-capture-r2/alternate-trace.json"]["totals"]["consumed_member_union_bytes"] += 1
        with self.assertRaisesRegex(ValueError, "read ownership chain"):
            validate_reads(PACKETS / "packet", self.original, self.docs, proofs, self.members, self.text)

    def test_complete_result_mutations_are_rejected_after_rehashing(self):
        controls = (
            ("value", "offline complete-result comparator"),
            ("identity", "result is not bound"),
            ("engine", "native nanosecond"),
            ("fallback", "fallback"),
            ("order", "case order"),
        )
        records = list(lines(ARCHIVE, "cases-0000.jsonl.xz", self.index["parts"][0], 250))
        for control, diagnostic in controls:
            with self.subTest(control=control), tempfile.TemporaryDirectory() as temp:
                archive = Path(temp) / "packet"
                shutil.copytree(ARCHIVE, archive)
                changed = deepcopy(records)
                entry = changed[0]["native"]
                packet = entry["packet"]
                response = json.loads(packet["files"]["one.stdout.json"]["utf8"])
                if control == "value":
                    response["actual"]["rows"][0]["row_id"] = 999999
                elif control == "identity":
                    response["case_identity"] = "0" * 64
                elif control == "engine":
                    response["observation"]["clocks"]["engine_execution_seconds"] += 1
                elif control == "fallback":
                    response["observation"]["fields"]["fallback_attempted"] = "true"
                else:
                    changed[0], changed[1] = changed[1], changed[0]
                raw = json.dumps(response, separators=(",", ":")).encode()
                packet["files"]["one.stdout.json"] = dict(utf8=raw.decode(), bytes=len(raw), sha256=sha(raw))
                raw_packet = canonical_blob_bytes(packet)
                entry["json_sha256"] = sha(raw_packet)
                entry["blob_sha256"] = sha(zlib.compress(raw_packet))
                (archive / "cases-0000.jsonl.xz").unlink()
                index = deepcopy(self.index)
                index["parts"][0] = write_part(archive, 0, changed)
                (archive / "index.json").write_text(json.dumps(index))
                with self.assertRaisesRegex(ValueError, diagnostic):
                    verify(archive, PACKETS / "packet", PACKETS / "updated-native")

    def test_aggregate_clock_cannot_move_or_change_the_previous_denominator(self):
        local = self.proofs["local-replay.json"]
        previous = json.loads((PACKETS / "updated-native/results.json").read_bytes())
        validate_result(local, local, previous)
        result = deepcopy(local)
        result["complete_engine_seconds"] -= 1
        with self.assertRaisesRegex(ValueError, "portable and local shared replay"):
            validate_result(result, local, previous)
        previous["complete_engine_seconds"] += 1
        with self.assertRaisesRegex(ValueError, "previous native result"):
            validate_result(local, local, previous)

    def test_request_shape_groups_preserve_every_case_and_exact_engine_sum(self):
        rows = [dict(family="join", original_engine_seconds=2.0, updated_engine_seconds=1.0),
                dict(family="join", original_engine_seconds=1.0, updated_engine_seconds=3.0)]
        group = dict(family="join", cases=2, original_engine_seconds=3.0,
                     updated_engine_seconds=4.0, delta_engine_seconds=1.0)
        validate_groups(rows, [group], ("family",))
        for field, value in (("cases", 1), ("updated_engine_seconds", 3.0), ("family", "window")):
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "analysis group"):
                validate_groups(rows, [dict(group, **{field: value})], ("family",))


if __name__ == "__main__":
    unittest.main()
