"""Contract tests for conservative CI selection and final result accounting."""
from __future__ import annotations

import json
import contextlib
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import ci_execution_plan as execution


JOBS = [
    "ci-work-shaping", "ci-gate-matrix", "rust-baseline", "rust-feature-matrix",
    "native-test-build", "native-test-partitions", "patched-provider-tests", "native-tests",
    "native-cli-build-debug", "native-cli-build-release", "rust-msrv", "python-test-shards",
    "python-tests", "python-compatibility-matrix", "native-package-build-linux",
    "native-package-build-macos", "python-native-wheel-linux", "python-native-wheel-macos",
    "python-native-wheel", "python-package", "dependency-security", "release-runtime-core",
    "release-package-governance", "release-user-surface", "website-docs", "release-readiness",
]
DEFAULT_PROSE = "# ShardLoom\n\nThe engine runs locally.\n"


class GitPlanFixture:
    def __init__(self, path: str = "README.md", before: str = DEFAULT_PROSE,
                 after: str = "# ShardLoom\n\nThe engine runs entirely locally.\n",
                 *, mode: str = "pull_request", field_overrides: dict | None = None,
                 envelope: dict | str | None = None, chmod: int | None = None,
                 delete: bool = False, rename_to: str | None = None) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "checkout"
        self.root.mkdir()
        self.planner = Path(self.temp.name) / "planner.json"
        subprocess.run(["git", "init", "-q"], cwd=self.root, check=True)
        subprocess.run(["git", "config", "user.email", "ci-tests@example.invalid"],
                       cwd=self.root, check=True)
        subprocess.run(["git", "config", "user.name", "CI tests"], cwd=self.root, check=True)
        self._write_workflow()
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(before)
        if chmod is not None:
            target.chmod(chmod)
        self._commit("base")
        self.base = self._head()
        if delete:
            target.unlink()
        elif rename_to is not None:
            target.unlink()
            destination = self.root / rename_to
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(after)
        elif after == "<SYMLINK>":
            target.unlink()
            target.symlink_to("target.txt")
        else:
            target.write_text(after)
        if chmod is not None and not delete and rename_to is None:
            target.chmod(chmod)
        self._commit("revision")
        self.revision = self._head()
        fields = {
            "schema_version": "shardloom.ci_work_shaping_plan.v1",
            "recommended_job_order": "ci-work-shaping,ci-gate-matrix,website-docs",
            "changed_path_order": path if rename_to is None else f"{path},{rename_to}",
            "changed_path_count": "1" if rename_to is None else "2",
            "ci_mode": mode,
            "merge_hard_lane_required": "false",
            "release_proof_lane_required": "false",
            "unknown_path_hard_gate_required": "false",
            "capillary_family_order": "docs_only",
        }
        if field_overrides:
            fields.update(field_overrides)
        if envelope is None:
            payload = {"status": "success", "fields": [
                {"key": key, "value": value} for key, value in fields.items()]}
            self.planner.write_text(json.dumps(payload))
        elif isinstance(envelope, str):
            self.planner.write_text(envelope)
        else:
            self.planner.write_text(json.dumps(envelope))

    def _write_workflow(self) -> None:
        lines = ["name: CI", "", "on:", "  push:", "  pull_request:", "", "jobs:"]
        for job in JOBS:
            lines.extend([f"  {job}:", f"    name: {job}", "    runs-on: ubuntu-latest"])
        lines.extend(["    needs:"])
        lines.extend(f"      - {job}" for job in JOBS if job != "release-readiness")
        (self.root / ".github/workflows").mkdir(parents=True, exist_ok=True)
        (self.root / ".github/workflows/ci.yml").write_text("\n".join(lines) + "\n")

    def _commit(self, message: str) -> None:
        subprocess.run(["git", "add", "-A"], cwd=self.root, check=True)
        subprocess.run(["git", "commit", "-qm", message], cwd=self.root, check=True)

    def _head(self) -> str:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=self.root,
                                       text=True).strip()

    def plan(self, mode: str = "pull_request") -> dict:
        return execution.make_plan(self.root, self.planner, self.base, self.revision, mode)


class ClassificationTests(unittest.TestCase):
    def fixture(self, *args, **kwargs) -> GitPlanFixture:
        fixture = GitPlanFixture(*args, **kwargs)
        self.addCleanup(fixture.temp.cleanup)
        return fixture

    def test_plain_existing_readme_and_getting_started_wording_can_be_narrow(self) -> None:
        for path in ("README.md", "docs/getting-started/install.md"):
            with self.subTest(path=path):
                fixture = self.fixture(path)
                plan = fixture.plan()
                self.assertFalse(plan["full_validation"], plan["reason"])
                self.assertIn(path, plan["classified_paths"])

    def test_code_directives_front_matter_and_shell_or_sql_snippets_force_full_validation(self) -> None:
        cases = [
            ("fenced code", "# Title\n\n```rust\nlet x = 1;\n```\n",
             "# Title\n\n```rust\nlet x = 2;\n```\n"),
            ("inline code", "Run `cargo test` now.\n", "Run `cargo build` now.\n"),
            ("directives", ":::note\nOld wording\n:::\n", ":::warning\nOld wording\n:::\n"),
            ("front matter", "---\ntitle: Old\n---\n\nBody.\n", "---\ntitle: New\n---\n\nBody.\n"),
            ("shell snippet", "# Guide\n\n```sh\necho old\n```\n", "# Guide\n\n```sh\necho new\n```\n"),
            ("SQL snippet", "# Guide\n\n```sql\nSELECT 1;\n```\n", "# Guide\n\n```sql\nSELECT 2;\n```\n"),
        ]
        for name, before, after in cases:
            with self.subTest(name=name):
                plan = self.fixture("README.md", before, after).plan()
                self.assertTrue(plan["full_validation"], plan)

    def test_add_delete_rename_mode_symlink_json_other_docs_and_unknown_paths_are_broad(self) -> None:
        cases = [
            ("new file", dict(path="docs/getting-started/new.md", before="", after="New.\n", delete=False)),
            ("deleted file", dict(path="README.md", delete=True)),
            ("rename", dict(path="README.md", rename_to="docs/getting-started/moved.md")),
            ("executable mode", dict(path="README.md")),
            ("symlink", dict(path="README.md", after="<SYMLINK>")),
            ("JSON fixture", dict(path="docs/getting-started/cases.json", before="{}\n", after="{\"x\":1}\n")),
            ("docs elsewhere", dict(path="docs/architecture/topic.md")),
            ("generator", dict(path="scripts/generate_docs.py")),
            ("config", dict(path="website-src/astro.config.mjs")),
            ("manifest", dict(path="python/pyproject.toml")),
            ("dependency", dict(path="Cargo.toml")),
            ("unknown", dict(path="tools/unknown.dat")),
        ]
        for name, kwargs in cases:
            with self.subTest(name=name):
                if name == "new file":
                    f = self.fixture("README.md")
                    target = f.root / kwargs["path"]
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_text(kwargs["after"])
                    f._commit("add file")
                    f.revision = f._head()
                    envelope = json.loads(f.planner.read_text())
                    for row in envelope["fields"]:
                        if row["key"] == "changed_path_order":
                            row["value"] = "README.md," + kwargs["path"]
                        if row["key"] == "changed_path_count":
                            row["value"] = "2"
                    f.planner.write_text(json.dumps(envelope))
                    plan = f.plan()
                    self.assertIn("unclassified or executable", plan["reason"])
                    self.addCleanup(f.temp.cleanup)
                elif name == "executable mode":
                    f = self.fixture()
                    (f.root / "README.md").chmod(0o755)
                    f._commit("change executable mode")
                    f.revision = f._head()
                    plan = f.plan()
                    self.addCleanup(f.temp.cleanup)
                else:
                    plan = self.fixture(**kwargs).plan()
                self.assertTrue(plan["full_validation"], plan)

    def test_website_source_can_use_narrow_plan_while_retaining_website_rust_python_policy(self) -> None:
        recommended = ",".join([
            "website-docs", "rust-baseline", "rust-feature-matrix", "python-test-shards",
            "python-tests", "python-compatibility-matrix", "python-package", "dependency-security",
        ])
        fixture = self.fixture("website-src/src/pages/index.astro", "<p>old</p>\n", "<p>new</p>\n",
                              field_overrides={"recommended_job_order": recommended,
                                               "capillary_family_order": "website_docs"})
        plan = fixture.plan()
        self.assertFalse(plan["full_validation"], plan["reason"])
        self.assertTrue({"website-docs", "rust-baseline", "rust-feature-matrix", "python-test-shards",
                         "python-tests", "python-compatibility-matrix", "python-package",
                         "dependency-security"} <= set(plan["selected_jobs"]))

    def test_push_release_bad_planner_schema_recommendations_paths_and_flags_are_broad(self) -> None:
        cases = [
            ("push/merge", dict(mode="merge"), "merge"),
            ("release", dict(mode="release"), "release"),
            ("planner error", dict(envelope={"status": "error", "fields": []}), "pull_request"),
            ("bad schema", dict(field_overrides={"schema_version": "wrong"}), "pull_request"),
            ("unknown recommendation", dict(field_overrides={"recommended_job_order": "unknown-job"}), "pull_request"),
            ("duplicate recommendation", dict(field_overrides={"recommended_job_order": "website-docs,website-docs"}), "pull_request"),
            ("changed path mismatch", dict(field_overrides={"changed_path_order": "other.md"}), "pull_request"),
            ("hard lane", dict(field_overrides={"merge_hard_lane_required": "true"}), "pull_request"),
            ("release lane", dict(field_overrides={"release_proof_lane_required": "true"}), "pull_request"),
            ("unknown path flag", dict(field_overrides={"unknown_path_hard_gate_required": "true"}), "pull_request"),
            ("unknown family", dict(field_overrides={"capillary_family_order": "rust_runtime"}), "pull_request"),
        ]
        for name, kwargs, mode in cases:
            with self.subTest(name=name):
                plan = self.fixture(**kwargs).plan(mode=mode)
                self.assertTrue(plan["full_validation"], plan)

    def test_comma_and_newline_paths_are_ambiguous_and_force_broad_validation(self) -> None:
        for name, path in (("comma", "README,notes.md"), ("newline", "README\nnotes.md")):
            with self.subTest(name=name):
                plan = self.fixture(path=path).plan()
                self.assertTrue(plan["full_validation"], plan)

    def test_malformed_planner_json_and_unclosed_or_bad_schema_are_broad(self) -> None:
        for name, envelope in (("invalid JSON", "{"),
                               ("non-object envelope", "[]"),
                               ("non-object field", {"status": "success", "fields": [None]}),
                               ("missing fields", {"status": "success"}),
                               ("duplicate field", {"status": "success", "fields": [
                                   {"key": "schema_version", "value": "shardloom.ci_work_shaping_plan.v1"},
                                   {"key": "schema_version", "value": "shardloom.ci_work_shaping_plan.v1"}]})):
            with self.subTest(name=name):
                self.assertTrue(self.fixture(envelope=envelope).plan()["full_validation"])

    def test_make_plan_never_removes_a_job_recommended_by_rust(self) -> None:
        fixture = self.fixture(field_overrides={
            "recommended_job_order": "ci-work-shaping,ci-gate-matrix,rust-feature-matrix,website-docs"})
        plan = fixture.plan()
        self.assertFalse(plan["full_validation"], plan["reason"])
        self.assertIn("rust-feature-matrix", plan["selected_jobs"])
        self.assertNotIn("rust-feature-matrix", plan["omitted_jobs"])


class SummaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = GitPlanFixture()
        self.addCleanup(self.fixture.temp.cleanup)
        jobs = execution.job_inventory(self.fixture.root)
        self.plan = {"schema_version": execution.SCHEMA, "jobs": jobs,
                     "revision": self.fixture.revision,
                     "workflow_sha256": execution.digest(self.fixture.root / ".github/workflows/ci.yml"),
                     "full_validation": True, "selected_jobs": jobs.copy(),
                     "omitted_jobs": {}, "classified_paths": {}}
        self.needs = {job: {"result": "success"} for job in jobs if job != execution.SUMMARY}

    def test_summary_accepts_exact_all_success_results(self) -> None:
        result = execution.verify_summary(self.fixture.root, self.plan, self.needs)
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["job_results"], {job: "success" for job in self.needs})

    def test_summary_command_preserves_failure_evidence_and_exits_unsuccessfully(self) -> None:
        plan_path = self.fixture.root / "plan.json"
        plan_path.write_text(json.dumps(self.plan))
        output = self.fixture.root / "evidence/summary.json"
        for needs in (self.needs | {"rust-baseline": {"result": "failure"}}, [],
                      self.needs | {"rust-baseline": None}):
            with self.subTest(needs=needs), patch.object(execution, "ROOT", self.fixture.root), \
                    patch.object(sys, "argv", ["ci_execution_plan", "summary", "--plan", str(plan_path),
                                               "--output", str(output)]), \
                    patch.dict(os.environ, {"CI_JOB_RESULTS": json.dumps(needs)}), \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(execution.main(), 1)
            report = json.loads(output.read_text())
            self.assertEqual(report["status"], "failed")
            self.assertTrue(report["error"])

    def test_summary_requires_selected_jobs_success_and_omitted_jobs_skipped(self) -> None:
        omitted_job = next(job for job in self.plan["jobs"]
                           if job not in execution.ALWAYS and job != execution.SUMMARY)
        selected_plan = self.plan.copy()
        selected_plan["full_validation"] = False
        selected_plan["classified_paths"] = {"README.md": "verified wording"}
        selected_plan["selected_jobs"] = [job for job in self.plan["jobs"] if job != omitted_job]
        selected_plan["omitted_jobs"] = {omitted_job: "verified narrow-path classification"}
        needs = self.needs.copy()
        needs[omitted_job] = {"result": "skipped"}
        execution.verify_summary(self.fixture.root, selected_plan, needs)
        for result in ("failure", "cancelled", "skipped", "unknown"):
            with self.subTest(selected_result=result):
                bad = needs.copy()
                selected_job = next(job for job in selected_plan["selected_jobs"] if job != execution.SUMMARY)
                bad[selected_job] = {"result": result}
                with self.assertRaises(ValueError):
                    execution.verify_summary(self.fixture.root, selected_plan, bad)
        for result in ("success", "failure", "cancelled", "unknown"):
            with self.subTest(omitted_result=result):
                bad = needs.copy()
                bad[omitted_job] = {"result": result}
                with self.assertRaises(ValueError):
                    execution.verify_summary(self.fixture.root, selected_plan, bad)

    def test_summary_rejects_missing_invented_duplicate_and_unclassified_omissions(self) -> None:
        cases = []
        missing = self.plan.copy()
        missing["selected_jobs"] = missing["selected_jobs"][:-1]
        cases.append(("missing job", missing, self.needs))
        invented_needs = self.needs | {"invented": {"result": "success"}}
        cases.append(("invented needs", self.plan, invented_needs))
        missing_needs = self.needs.copy()
        missing_needs.pop("rust-baseline")
        cases.append(("missing needs", self.plan, missing_needs))
        duplicate = self.plan.copy()
        duplicate["selected_jobs"] = duplicate["selected_jobs"] + ["rust-baseline"]
        cases.append(("duplicate selected job", duplicate, self.needs))
        unclassified = self.plan.copy()
        unclassified["full_validation"] = False
        unclassified["classified_paths"] = {"README.md": "wording"}
        unclassified["selected_jobs"] = [job for job in unclassified["jobs"]
                                          if job in execution.ALWAYS]
        unclassified["omitted_jobs"] = {job: "classified omission"
                                        for job in unclassified["jobs"]
                                        if job not in unclassified["selected_jobs"]}
        unclassified["omitted_jobs"][next(iter(unclassified["omitted_jobs"]))] = ""
        cases.append(("unclassified omission", unclassified, self.needs))
        for name, plan, needs in cases:
            with self.subTest(name=name), self.assertRaises(ValueError):
                execution.verify_summary(self.fixture.root, plan, needs)

    def test_summary_rejects_wrong_workflow_or_revision(self) -> None:
        for key, value in (("workflow_sha256", "bad"), ("revision", "0" * 40)):
            plan = self.plan.copy()
            plan[key] = value
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "current workflow/revision"):
                execution.verify_summary(self.fixture.root, plan, self.needs)


class WorkflowTests(unittest.TestCase):
    def test_real_ci_inventory_excludes_triggers_and_release_readiness_needs_every_other_job(self) -> None:
        root = Path(__file__).resolve().parents[1]
        workflow_path = root / ".github/workflows/ci.yml"
        text = workflow_path.read_text()
        jobs = execution.job_inventory(root)
        for trigger in ("push", "pull_request", "workflow_dispatch", "schedule"):
            self.assertNotIn(trigger, jobs)
        match = __import__("re").search(
            r"(?ms)^  release-readiness:\n(?P<body>.*?)(?=^  [a-z][a-z0-9-]*:\n|\Z)", text)
        self.assertIsNotNone(match)
        needs_match = __import__("re").search(
            r"(?m)^    needs:\n(?P<items>(?:^      - [a-z][a-z0-9-]*\n)+)", match.group("body"))
        self.assertIsNotNone(needs_match)
        needs = __import__("re").findall(r"^      - ([a-z][a-z0-9-]*)$", needs_match.group("items"),
                                         __import__("re").MULTILINE)
        self.assertEqual(len(needs), len(set(needs)))
        self.assertEqual(set(needs), set(jobs) - {execution.SUMMARY})


if __name__ == "__main__":
    unittest.main()
