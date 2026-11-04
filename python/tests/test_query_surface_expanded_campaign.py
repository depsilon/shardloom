# SPDX-License-Identifier: Apache-2.0
"""Cohort identity, interaction coverage, run order and immutable outcome controls."""

import hashlib
from contextlib import closing
from itertools import product
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "benchmarks/traditional_analytics"), str(ROOT / "scripts")]
from query_surface.adapters import relation_sql
from query_surface.corpus import corpus
from query_surface.datasets import SCHEMAS
from query_surface.evidence import identity
from query_surface.expanded_catalog import build, catalog, spaces, valid, signatures, SURROGATES
from query_surface.expanded_freeze import assign
from query_surface.expanded_run import observe, request, stage_complete
from query_surface.expanded_report import raw_files


class ExpandedCampaignTests(unittest.TestCase):
    def test_unique_requests_include_frozen_legacy_and_all_feasible_interactions(self):
        legacy = corpus(interactions="all", include_pending=False)
        wire = "".join(sorted(c.id + " " + c.identity + "\n" for c in legacy)).encode()
        # Digest of the exact retained November 2 case-to-identity mapping.
        self.assertEqual(
            hashlib.sha256(wire).hexdigest(),
            "543758812717fef086f430e48d949153aa449c3ea7adb5f6f0767d0518e6303d",
        )
        sources = {name: [name + ".parquet"] for name in SCHEMAS}
        statements = {relation_sql(c.relation, sources) for c in legacy}
        descriptors, coverage = catalog()
        declarations = {s.name: s for s in spaces()}
        self.assertEqual(len(legacy), 347)
        self.assertEqual(len(descriptors), 9653)
        for row in descriptors:
            statement = relation_sql(build(row, declarations).relation, sources)
            self.assertNotIn(statement, statements, row["id"])
            statements.add(statement)
        self.assertEqual(len(statements), 10000)
        for record in coverage:
            space = declarations[record["archetype"]]
            feasible = [dict(zip(space.axes, cell)) for cell in product(*space.axes.values())]
            want = set().union(*(signatures(c, space.higher) for c in feasible if valid(space, c)))
            chosen = [r["cell"] for r in descriptors if r["archetype"] == space.name]
            observed = set().union(*(signatures(c, space.higher) for c in chosen))
            self.assertEqual(want, observed, space.name)
            self.assertEqual(record["pairwise_and_targeted_interactions"], len(observed))
        assigned = assign(descriptors)
        self.assertEqual(sum(r["profile"] == "scale" for r in assigned), 64)
        self.assertEqual(sum(r["profile"] == "smoke" for r in assigned), 132)
        self.assertEqual(sum(r["profile"] == "holdout" for r in assigned), 880)
        named = {r["surrogate"]: (r["archetype"], r["cell"]) for r in assigned if r["surrogate"]}
        self.assertEqual(named, SURROGATES)
        for row in assigned:
            if row["surrogate"]:
                self.assertEqual(
                    (row["surface"], row["native_format"], row["polars_format"]),
                    ("python_sql", "parquet", "parquet"),
                )

    def test_prerequisite_verifies_every_record_and_rejects_a_changed_ledger(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "frozen.json").write_text("{}")
            stage = root / "observe-polars"
            stage.mkdir()
            ledger = stage / "attempts.sqlite"
            with closing(sqlite3.connect(ledger)) as db, db:
                db.execute("CREATE TABLE attempt (status TEXT)")
                db.executemany("INSERT INTO attempt VALUES (?)", [("passed",), ("engine_error",)])
            receipt = dict(
                status="complete_observation",
                total=2,
                attempt_slots=2,
                posthash_verified=True,
                ledger=identity(ledger),
                frozen=identity(root / "frozen.json"),
                outcomes={"passed": 1, "engine_error": 1},
            )
            path = stage / "complete.json"
            path.write_text(json.dumps(receipt))
            self.assertEqual(stage_complete(root, "polars", 2)["outcomes"], receipt["outcomes"])
            wrong = {**receipt, "outcomes": {"passed": 2}}
            path.write_text(json.dumps(wrong))
            with self.assertRaisesRegex(ValueError, "incomplete"):
                stage_complete(root, "polars", 2)
            path.write_text(json.dumps(receipt))
            with closing(sqlite3.connect(ledger)) as db, db:
                db.execute("UPDATE attempt SET status='passed'")
            with self.assertRaisesRegex(ValueError, "prerequisite"):
                stage_complete(root, "polars", 2)

    def test_native_requires_completed_polars_and_alternate_requires_designated_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch("query_surface.expanded_run.admission", return_value=({"count": 10000}, {}, [])):
                with self.assertRaises(FileNotFoundError):
                    observe(Path(tmp), "native", False)
            row = {
                "surface": "python_sql",
                "profile": "primary",
                "native_format": "parquet",
                "surrogate": None,
            }
            with self.assertRaisesRegex(ValueError, "predeclared surrogate"):
                request(row, "alternate", {"profiles": {"primary": {}}}, {}, {}, {})
        with self.assertRaisesRegex(ValueError, "archived worker file differs"):
            raw_files({"files": {"one.stdout.json": {"utf8": "altered", "bytes": 7, "sha256": "0" * 64}}})
