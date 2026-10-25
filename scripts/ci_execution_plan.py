#!/usr/bin/env python3
"""Conservatively apply the existing Rust CI plan and verify every job outcome."""
from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "shardloom.ci_execution_plan.v1"
ALWAYS = {"ci-work-shaping", "ci-gate-matrix", "rust-baseline", "python-test-shards",
          "python-tests", "website-docs", "release-readiness"}
SUMMARY = "release-readiness"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def job_inventory(root: Path) -> list[str]:
    workflow = (root / ".github/workflows/ci.yml").read_text().split("\njobs:\n", 1)[1]
    jobs = re.findall(r"^  ([a-z][a-z0-9-]+):$", workflow, re.MULTILINE)
    if len(jobs) != len(set(jobs)) or not ALWAYS <= set(jobs):
        raise ValueError("CI workflow job inventory is incomplete or duplicated")
    return jobs


def git(root: Path, *args: str) -> bytes:
    return subprocess.check_output(["git", *args], cwd=root)


def planner_fields(envelope: dict) -> dict[str, str]:
    if not isinstance(envelope, dict) or envelope.get("status") != "success":
        raise ValueError("Rust CI planner did not succeed")
    rows = envelope["fields"]
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError("Rust CI planner fields are invalid")
    fields = {row["key"]: row["value"] for row in rows}
    if (len(fields) != len(rows)
            or fields.get("schema_version") != "shardloom.ci_work_shaping_plan.v1"
            or not all(isinstance(value, str) for value in fields.values())):
        raise ValueError("Rust CI planner schema is invalid")
    return fields


def markdown_parts(text: str) -> tuple[list[str], list[str]]:
    """Separate plain wording from code, directives, front matter and inline code."""
    prose, protected = [], []
    fence = None
    frontmatter = False
    for index, line in enumerate(text.splitlines()):
        stripped = line.lstrip()
        if index == 0 and line == "---":
            frontmatter = True
            protected.append(line)
        elif frontmatter:
            protected.append(line)
            if line in {"---", "..."}:
                frontmatter = False
        elif fence is not None:
            protected.append(line)
            if stripped.startswith(fence):
                fence = None
        elif stripped.startswith(("```", "~~~")):
            fence = stripped[:3]
            protected.append(line)
        elif (line.startswith(("    ", "\t")) or any(token in line for token in
                ("`", "<", ">", "{", "}", ":::"))):
            protected.append(line)
        else:
            prose.append(line)
    if fence is not None or frontmatter:
        raise ValueError("unclosed Markdown code/front matter")
    return prose, protected


def safe_prose(before: str, after: str) -> bool:
    try:
        old, old_protected = markdown_parts(before)
        new, new_protected = markdown_parts(after)
    except ValueError:
        return False
    if old_protected != new_protected:
        return False
    changed = [line[2:] for line in difflib.ndiff(old, new) if line.startswith(("+ ", "- "))]
    # Fail closed on shell/SQL snippets, Markdown directives and structured data.
    return bool(changed) and all(not re.search(
        r"[=;\\$|]|^\s*(?:import |from |export |select |with |def |class |cargo |python\d? |"
        r"pip\d? |curl |wget |git |npm |node |bash |sh |sudo |uv |!)", line, re.IGNORECASE)
                                 for line in changed)


def classify_change(root: Path, path: str, base: str, revision: str) -> str | None:
    item = PurePosixPath(path)
    if (item.is_absolute() or ".." in item.parts or any(char in path for char in "\n\r,\0")
            or path != path.strip()):
        return None
    # Only existing regular files qualify; renames/deletes/additions and mode
    # changes take broad validation. --no-renames preserves both sides of moves.
    entries = [git(root, "ls-tree", ref, "--", path).decode() for ref in (base, revision)]
    if any(not entry.startswith("100644 blob ") for entry in entries):
        return None
    if path == "README.md" or (path.startswith("docs/getting-started/") and item.suffix == ".md"):
        before, after = [git(root, "show", f"{ref}:{path}").decode() for ref in (base, revision)]
        return "documentation wording; code and directives unchanged" if safe_prose(before, after) else None
    if (any(path.startswith(f"website-src/src/{part}/") for part in
            ("components", "layouts", "pages", "styles"))
            and item.suffix in {".astro", ".css", ".ts", ".tsx", ".js"}):
        return "website implementation covered by website, policy and contract checks"
    return None


def make_plan(root: Path, planner: Path, base: str, revision: str, mode: str) -> dict:
    jobs = job_inventory(root)
    result = {"schema_version": SCHEMA, "revision": revision, "base_revision": base,
              "planner_sha256": digest(planner), "workflow_sha256": digest(root / ".github/workflows/ci.yml"),
              "jobs": jobs, "full_validation": True, "selected_jobs": jobs.copy(),
              "omitted_jobs": {}, "classified_paths": {}, "reason": "broad validation is the default"}
    try:
        if (mode != "pull_request" or not re.fullmatch(r"[a-f0-9]{40}", base)
                or not re.fullmatch(r"[a-f0-9]{40}", revision)
                or git(root, "rev-parse", "HEAD").decode().strip() != revision):
            raise ValueError("push/release or uncertain revision requires broad validation")
        fields = planner_fields(json.loads(planner.read_text()))
        paths = sorted(set(git(root, "diff", "--no-renames", "--name-only", "-z", base, revision)
                           .decode().rstrip("\0").split("\0")))
        recommended = fields["recommended_job_order"].split(",")
        if (not paths or paths == [""] or any(any(c in path for c in "\n\r,") for path in paths)
                or sorted(fields["changed_path_order"].split(",")) != paths
                or fields["changed_path_count"] != str(len(paths))
                or fields["ci_mode"] != mode or not set(recommended) <= set(jobs)
                or len(set(recommended)) != len(recommended)):
            raise ValueError("changed-path or recommended-job evidence is ambiguous")
        if (fields["merge_hard_lane_required"] != "false"
                or fields["release_proof_lane_required"] != "false"
                or fields["unknown_path_hard_gate_required"] != "false"
                or not set(fields["capillary_family_order"].split(",")) <= {"docs_only", "website_docs"}):
            raise ValueError("Rust planner requires broad validation")
        for path in paths:
            classification = classify_change(root, path, base, revision)
            if classification is None:
                raise ValueError(f"unclassified or executable change: {path}")
            result["classified_paths"][path] = classification
        selected = ALWAYS | set(recommended)
        result.update(full_validation=False, selected_jobs=[job for job in jobs if job in selected],
                      omitted_jobs={job: "all changed paths are verified wording or website implementation"
                                    for job in jobs if job not in selected},
                      reason="conservative wording/website plan; all default Rust, Python and policy checks retained")
    except (ValueError, KeyError, TypeError, UnicodeError, subprocess.CalledProcessError) as error:
        result["reason"] = str(error)
    return result


def verify_summary(root: Path, plan: dict, needs: dict) -> dict:
    jobs = job_inventory(root)
    if (not isinstance(plan, dict) or not isinstance(needs, dict)
            or any(not isinstance(row, dict) for row in needs.values())):
        raise ValueError("execution plan and job outcomes must be objects")
    if (plan.get("schema_version") != SCHEMA or plan.get("jobs") != jobs
            or plan.get("workflow_sha256") != digest(root / ".github/workflows/ci.yml")
            or plan.get("revision") != git(root, "rev-parse", "HEAD").decode().strip()):
        raise ValueError("execution plan does not match the current workflow/revision")
    selected = plan["selected_jobs"]
    omitted = plan["omitted_jobs"]
    if (len(selected) != len(set(selected)) or set(selected) & set(omitted)
            or not isinstance(plan.get("full_validation"), bool)
            or set(selected) | set(omitted) != set(jobs) or not ALWAYS <= set(selected)
            or not all(isinstance(reason, str) and reason for reason in omitted.values())
            or (plan["full_validation"] and omitted)
            or (not plan["full_validation"] and not plan["classified_paths"])
            or set(needs) != set(jobs) - {SUMMARY}):
        raise ValueError("execution plan/needs omits, duplicates or invents a required job")
    failures = {job: row.get("result") for job, row in needs.items()
                if row.get("result") != ("success" if job in selected else "skipped")}
    if failures:
        raise ValueError(f"selected or deliberately omitted job outcomes are invalid: {failures}")
    return {"schema_version": "shardloom.ci_execution_summary.v1", "status": "passed",
            "revision": plan["revision"], "full_validation": plan["full_validation"],
            "selected_jobs": selected, "omitted_jobs": omitted,
            "job_results": {job: row["result"] for job, row in needs.items()},
            "release_evidence_required": plan["full_validation"]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("plan", "summary"))
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--base", default="")
    parser.add_argument("--revision", default="")
    parser.add_argument("--mode", default="merge")
    args = parser.parse_args()
    if args.action == "plan":
        result = make_plan(ROOT, args.plan, args.base, args.revision, args.mode)
        if os.environ.get("GITHUB_OUTPUT"):
            with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
                output.write("selected_jobs=" + json.dumps(result["selected_jobs"], separators=(",", ":")) + "\n")
                output.write("full_validation=" + str(result["full_validation"]).lower() + "\n")
    else:
        try:
            result = verify_summary(ROOT, json.loads(args.plan.read_text()),
                                    json.loads(os.environ["CI_JOB_RESULTS"]))
        except (OSError, ValueError, KeyError, TypeError) as error:
            # Retain a failed result for upload as well as failing the required job.
            result = {"schema_version": "shardloom.ci_execution_summary.v1", "status": "failed",
                      "error": f"{type(error).__name__}: {error}"}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    return 1 if result.get("status") == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
