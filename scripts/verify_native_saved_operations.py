# SPDX-License-Identifier: Apache-2.0
"""Replay the retained component investigation from committed bytes."""
import argparse
import io
import json
import lzma
from pathlib import Path
import sys
import tarfile

REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(REPO / "scripts"), str(REPO / "python/src"), str(REPO / "benchmarks/traditional_analytics")]
from native_saved_components import METRICS, component_evidence, project, totals
from native_saved_operations import broad, clickbench, preparations
from native_workflow_protocol import strict_json
from query_surface.replay import MAX_ARCHIVE, MAX_FILE, bounded_bytes, need, sha
from shared_performance_bundle import Bundle, json_bytes

EVIDENCE = REPO / "docs/benchmarks/evidence"
PACKET = EVIDENCE / "native-post-110-optimization-2026-11-03"
ANCHORS = {
    "probe": "native-source-strategy-probe-2026-11-02/acceptance.json",
    "workflow": "native-preparation-contracts-2026-11-02/acceptance.json",
    "broad": "native-preparation-fresh347-2026-11-02.json",
}


def operations(raw, anchors):
    return (clickbench(raw, anchors["probe"], anchors["workflow"]) +
            preparations(raw, anchors["workflow"]) + broad(raw, anchors["broad"]))


def identities(records):
    return [{k: r[k] for k in ("cohort", "ordinal", "operation", "workload_sha256", "runtime_sha256")}
            | {"stdout": r["evidence"]["stdout"]} for r in records]


def prior_objects(anchor, folder, selected):
    """Resolve already committed material from its immutable original archive."""
    archive = anchor["archive"]
    data = bounded_bytes(folder / "verification.tar.xz", MAX_ARCHIVE)
    need(len(data) == archive["bytes"] and sha(data) == archive["sha256"], "original archive identity changed")
    expected = set(selected)
    need(len(expected) == len(selected) and expected <= archive["source_map"].keys(), "invalid prior member index")
    objects = {}
    with tarfile.open(fileobj=io.BytesIO(data), mode="r|xz") as stream:
        for member in stream:
            if member.name not in expected:
                continue
            expected.remove(member.name)
            ref = archive["source_map"][member.name]
            need(member.isfile() and 0 <= member.size == ref["bytes"] <= MAX_FILE, "invalid prior member")
            value = stream.extractfile(member).read()
            need(sha(value) == ref["sha256"] and ref["source_path"] not in objects, "ambiguous prior member")
            objects[ref["source_path"]] = value
    need(not expected, "missing prior archive member")
    return objects


def analyze(packet, evidence):
    bundle = Bundle(packet / "inputs")
    frozen = bundle.index["evidence"]
    previous, anchors = {}, {}
    for role, rel in ANCHORS.items():
        data = bounded_bytes(evidence / rel, MAX_FILE)
        need(sha(data) == frozen["anchor_sha256"][role], "investigation anchor changed: " + role)
        anchors[role] = strict_json(data)
        if role in ("probe", "workflow"):
            members = prior_objects(anchors[role], (evidence / rel).parent, frozen["prior_members"][role])
            need(previous.keys().isdisjoint(members), "ambiguous path across original archives")
            previous.update(members)
    need(previous.keys().isdisjoint(bundle.paths), "duplicated retained source")
    used = set()

    def raw(path):
        used.add(path)
        return previous[path] if path in previous else bundle.raw(path)

    records = operations(raw, anchors)
    need(used == previous.keys() | bundle.paths.keys(), "frozen input index differs from actual replay")
    need(identities(records) == frozen["operations"], "frozen operation identity or ordinal changed")
    rows = [project(r) for r in records]
    table = b"".join(json_bytes(r) for r in rows)
    report = dict(schema_version="shardloom.retained_component_investigation.v1",
        input_manifest_sha256=sha((packet / "inputs/manifest.json").read_bytes()),
        archived_inputs=len(used), operation_count=len(rows), operation_table_sha256=sha(table),
        new_engine_executions=0, metric_catalog={k: dict(source_field=v[0], unit=v[1], owner=v[2])
                                                for k, v in METRICS.items()},
        cohorts=totals(rows), components=component_evidence(rows))
    return report, table


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet", type=Path, default=PACKET)
    parser.add_argument("--evidence", type=Path, default=EVIDENCE, help="parent of the three original anchors")
    parser.add_argument("--operations", type=Path, help="export verified uncompressed JSONL to a new file")
    args = parser.parse_args()
    report, table = analyze(args.packet, args.evidence)
    need(strict_json(bounded_bytes(args.packet / "report.json", MAX_FILE)) == report, "component report changed")
    decoder = lzma.LZMADecompressor(memlimit=128 << 20)
    data = decoder.decompress(bounded_bytes(args.packet / "operations.jsonl.xz", MAX_ARCHIVE), max_length=len(table) + 1)
    need(decoder.eof and not decoder.unused_data and data == table, "saved operation table changed")
    if args.operations:
        with args.operations.open("xb") as stream:
            stream.write(table)
    print(json.dumps({k: report[k] for k in ("operation_count", "archived_inputs", "new_engine_executions", "operation_table_sha256")}))


if __name__ == "__main__":
    main()
