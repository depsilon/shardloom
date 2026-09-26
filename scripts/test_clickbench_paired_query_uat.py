# SPDX-License-Identifier: Apache-2.0
import argparse
import hashlib
from pathlib import Path
import tarfile
import tempfile
import unittest

from local_uat_storage import StorageGuardError
from run_clickbench_paired_query_uat import (
    archive_completed_logs,
    paired_scores,
    positive_finite,
    resolve_role_sources,
    role_order,
    source_generations,
    source_receipt,
    verify_source_generations,
)


class PairedQueryUatTests(unittest.TestCase):
    def test_role_sources_default_to_control_and_receipt_freezes_both_roles(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            control = root / "control.vortex"
            candidate = root / "candidate.vortex"
            control.write_bytes(b"control generation")
            candidate.write_bytes(b"candidate generation")

            same_source = resolve_role_sources(control, home=root, platform="linux")
            self.assertEqual(same_source["control"], control.resolve())
            self.assertEqual(same_source["candidate"], control.resolve())
            same_receipt = source_receipt(same_source)
            self.assertEqual(same_receipt["control"]["path"], str(control.resolve()))
            self.assertEqual(same_receipt["candidate"], same_receipt["control"])

            role_sources = resolve_role_sources(control, candidate, home=root, platform="linux")
            frozen = source_generations(role_sources)
            receipt = source_receipt(role_sources, frozen)
            self.assertEqual(receipt["control"]["path"], str(control.resolve()))
            self.assertEqual(receipt["candidate"]["path"], str(candidate.resolve()))
            self.assertEqual(receipt["control"]["generation"], frozen["control"])
            self.assertEqual(receipt["candidate"]["generation"], frozen["candidate"])
            verify_source_generations(role_sources, frozen)

            candidate.write_bytes(b"changed candidate generation")
            with self.assertRaisesRegex(ValueError, "candidate"):
                verify_source_generations(role_sources, frozen)

    def test_max_workspace_limit_must_be_positive_and_finite(self):
        self.assertEqual(positive_finite("110"), 110)
        for value in ("0", "-1", "nan", "inf", "-inf", "not-a-number"):
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
                positive_finite(value)

    def test_each_query_has_both_orders_and_adjacent_queries_balance(self):
        first_roles = []
        for query in (1, 2):
            orders = [role_order(query, run) for run in range(1, 4)]
            self.assertEqual({o[0] for o in orders}, {"control", "candidate"})
            for run, order in enumerate(orders, 1):
                self.assertEqual(set(order), {"control", "candidate"})
                self.assertEqual(role_order(query, run, True), tuple(reversed(order)))
                first_roles.append(order[0])
        self.assertEqual(first_roles.count("control"), 3)
        self.assertEqual(first_roles.count("candidate"), 3)

    def test_scoring_keeps_queries_roles_and_pairs_separate(self):
        records = []
        for query in (1, 2):
            for run in (1, 2, 3):
                for role in ("candidate", "control"):
                    seconds = (100 if query == 1 else 2) + (3 if role == "candidate" else 0) + run
                    records.append(dict(query=query, run=run, role=role, seconds=seconds, passed=True))
        result = paired_scores(list(reversed(records)), [1, 2])
        self.assertTrue(result["complete"])
        self.assertEqual(result["score_scope"], "targeted_only")
        self.assertEqual(result["roles"]["control"]["query_total_seconds"], 104)
        self.assertEqual(result["roles"]["candidate"]["query_total_seconds"], 110)
        self.assertEqual(result["per_query"][0]["median_seconds"], {"control": 102, "candidate": 105})
        self.assertEqual(result["per_query"][1]["paired_candidate_minus_control_seconds"], [3, 3, 3])
        self.assertFalse(paired_scores(records[:-1], [1, 2])["complete"])
        self.assertFalse(paired_scores(records[:-1] + [records[0]], [1, 2])["complete"])
        records[0]["passed"] = False
        self.assertFalse(paired_scores(records, [1, 2])["complete"])

    def test_archive_preserves_values_hashes_and_rejects_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = {"result.json": b'{"values":[1,null,"Tokyo"]}', "stderr.txt": b""}
            paths = [root / name for name in data]
            for path in paths:
                path.write_bytes(data[path.name])
            reservations = []
            target = root / "completed.tar.xz"
            receipt = archive_completed_logs(paths, target, reservations.append)
            self.assertGreater(reservations[0], 0)
            self.assertEqual(reservations[-1], 0)
            with tarfile.open(target, "r:xz") as archive:
                for member in receipt["members"]:
                    raw = archive.extractfile(member["name"]).read()
                    self.assertEqual(raw, data[member["name"]])
                    self.assertEqual(hashlib.sha256(raw).hexdigest(), member["sha256"])
            self.assertTrue(all(not p.exists() for p in paths))
            for path in paths:
                path.write_bytes(b"competing evidence")
            with self.assertRaises(FileExistsError):
                archive_completed_logs(paths, target, lambda reserve: None)
            self.assertTrue(all(p.read_bytes() == b"competing evidence" for p in paths))

    def test_archive_denial_preserves_raw_and_rejects_unowned_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "result.json"
            path.write_bytes(b"complete")
            target = root / "completed.tar.xz"
            def deny(reserve):
                raise StorageGuardError("no temporary archive budget")
            with self.assertRaises(StorageGuardError):
                archive_completed_logs([path], target, deny)
            self.assertEqual(path.read_bytes(), b"complete")
            self.assertFalse(target.exists())
            with self.assertRaises(ValueError):
                archive_completed_logs([path, path], target, lambda reserve: None)
            with self.assertRaises(ValueError):
                archive_completed_logs([path], root / "nested" / "completed.tar.xz", lambda reserve: None)


if __name__ == "__main__":
    unittest.main()
