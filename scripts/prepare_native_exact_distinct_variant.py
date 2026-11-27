#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Freeze a new binary comparison using an existing cohort's exact input bytes.

Preparation only: this never executes a query or alters the existing cohort.
The timed driver, SQL operations, references and resource protocol stay fixed.
"""
from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
import shutil

import native_exact_distinct_cohort as cohort


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-cohort", required=True, type=Path)
    parser.add_argument("--work-dir", required=True, type=Path)
    parser.add_argument("--provenance", required=True, type=Path)
    args = parser.parse_args()
    source = args.source_cohort.resolve(strict=True)
    work = args.work_dir.resolve()
    cohort.need(not work.exists(), "variant requires a new work directory")
    cohort.need(not work.is_relative_to(cohort.ROOT), "artifacts must remain outside Git")
    original = json.loads((source / "protocol.json").read_text())
    cohort.need(original["schema"] == cohort.SCHEMA and original["mode"] == "benchmark",
                "source must be a frozen full cohort")
    for key in ["driver", "timing_helper"]:
        cohort.check_identity(original[key], full=True)
    cohort.need(cohort.python_source_hash() == original["python_source_sha256"],
                "Python client changed since source cohort")
    provenance, binaries = cohort.checked_binaries(args.provenance.resolve(strict=True))
    cohort.need(binaries["base"]["binary"]["sha256"] ==
                original["binaries"]["base"]["binary"]["sha256"], "baseline executable changed")
    work.mkdir(parents=True)
    for name in ["inputs", "references", "tmp", "source_preparation_reports"]:
        (work / name).mkdir()
    cohort.guard(work)
    protocol = copy.deepcopy(original)
    replacements = {}
    for format_name, files in original["sources"].items():
        protocol["sources"][format_name] = []
        for item in files:
            cohort.check_identity(item, full=True)
            destination = work / "inputs" / Path(item["path"]).name
            cohort.guard(work, item["bytes"])
            shutil.copy2(item["path"], destination)
            copied = cohort.identity(destination)
            cohort.need(copied["sha256"] == item["sha256"], "copied source differs")
            protocol["sources"][format_name].append(copied)
            replacements[item["path"]] = str(destination)
    references = {}
    for case in protocol["cases"]:
        item = case["reference"]
        if item["path"] not in references:
            cohort.check_identity(item, full=True)
            destination = work / "references" / Path(item["path"]).name
            shutil.copy2(item["path"], destination)
            copied = cohort.identity(destination)
            cohort.need(copied["sha256"] == item["sha256"], "copied reference differs")
            references[item["path"]] = copied
        case["reference"] = references[item["path"]]
        for old, new in replacements.items():
            case["sql"] = case["sql"].replace(
                "'" + old.replace("'", "''") + "'", "'" + new.replace("'", "''") + "'")
    for report in source.glob("prepare_*_vortex.json"):
        shutil.copy2(report, work / "source_preparation_reports" / report.name)
    cohort.write_json(work / "source_protocol.json", original)
    cohort.write_json(work / "provenance.json", provenance)
    protocol["binaries"] = binaries
    protocol["provenance"] = cohort.identity(work / "provenance.json")
    protocol["source_protocol"] = cohort.identity(work / "source_protocol.json")
    protocol["preparer"] = cohort.identity(Path(__file__))
    protocol["input_preparation"] = "byte-identical copies of source cohort inputs and references; no Vortex regeneration"
    cohort.write_json(work / "preparation_context.json", cohort.context_snapshot())
    cohort.write_json(work / "protocol.json", protocol)
    cohort.guard(work)
    print(json.dumps({"stage": "frozen_variant", "protocol": str(work / "protocol.json"),
                      "protocol_sha256": cohort.sha(work / "protocol.json"),
                      "cases": len(protocol["cases"]), "rows": protocol["rows"],
                      "input_and_reference_bytes_unchanged": True}), flush=True)


if __name__ == "__main__":
    main()
