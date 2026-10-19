# SPDX-License-Identifier: Apache-2.0
"""Public unary membership spill values, destinations, and retained state."""

from __future__ import annotations

import hashlib
import json

from shardloom.errors import ShardLoomCommandError

from native_streaming_set_cases import complete
from native_streaming_unary_cases import (
    apply,
    consume,
    fixture,
    frame,
    oracle,
    output_fields,
)
from native_streaming_unary_pressure_cases import GRANT, POLICY, row, source
from native_streaming_order_pressure_cases import encoded
from native_workflow_outputs import LOCAL_FORMATS, verify_output

KINDS = (
    "distinct",
    "dedup-first",
    "dedup-last",
    "dedup-false",
    "mask-first",
    "mask-last",
    "mask-false",
)
SHAPES = ("mixed", "empty", "all-null")
SPILL_POLICY = {
    "workspace": "",
    "quota_bytes": 128 << 20,
    "buffer_bytes": 1 << 20,
}


def run(harness):
    workspace = harness.output / "unary-membership-spill"
    workspace.mkdir()
    foreign = workspace / "foreign"
    foreign.write_bytes(b"preserve")
    spill = dict(SPILL_POLICY, workspace=str(workspace))
    requested = dict(POLICY, spill=spill)

    def clean():
        assert foreign.read_bytes() == b"preserve"
        assert list(workspace.iterdir()) == [foreign]

    declarations = []
    for kind in KINDS:
        for shape in SHAPES:
            rows, schema, fields = fixture(kind, shape)
            declarations.append({
                "case": f"unary-membership-{kind}-{shape}",
                "shape": shape,
                "kind": kind,
                "rows": rows,
                "expected": oracle(kind, rows),
                "fields": repr(output_fields(kind, fields)),
            })
    stress_expected = {}
    for kind in KINDS:
        rows = ([{"duplicated": False} for _ in range(257)] if kind.startswith("mask-") else
                [row(index, unique=True, width=8192) for index in range(257)])
        digest = hashlib.sha256()
        for value in rows:
            digest.update(encoded(value))
        stress_expected[kind] = {"rows": len(rows), "sha256": digest.hexdigest()}
    harness.json(harness.logs / "unary-membership-predeclared.json", {
        "kinds": KINDS,
        "shapes": SHAPES,
        "surfaces": ("dataframe", "sql"),
        "terminals": ("collect", "batches"),
        "small_cases": declarations,
        "destination_rows": 257,
        "destination_key": "s for distinct/dedup/masks; all n and s values are unique",
        "destination_width": 8192,
        "destination_oracles": stress_expected,
        "grant_bytes": GRANT,
        "spill": spill,
    })

    for declaration in declarations:
        kind, shape = declaration["kind"], declaration["shape"]
        name = f"unary-membership-{kind}-{shape}"
        for surface in ("dataframe", "sql"):
            for terminal in ("collect", "batches"):
                case_name = f"{name}-{surface}-{terminal}"

                def small_case(case_name=case_name, kind=kind, shape=shape,
                               surface=surface, terminal=terminal):
                    rows, schema, fields = fixture(kind, shape)
                    batch, trace = frame(harness, rows, schema, width=3)
                    query = apply(batch, kind, surface)
                    expected = oracle(kind, rows)
                    fields = output_fields(kind, fields)
                    envelope = consume(
                        harness, case_name, query, expected, fields, [trace], terminal,
                        policy=requested,
                    )
                    assert envelope.field_int("relational_spillable_unary_membership_stages") == 1
                    assert envelope.field_bool("relational_spill_owned_cleanup_completed") is True
                    clean()
                    return {"complete_rows": len(expected), "shape": shape, "surface": surface,
                            "terminal": terminal, "unary_membership_spill": True}

                harness.case(case_name, small_case)

    def accepted(name, report):
        harness.envelope(name, report.envelope)
        return report.envelope

    def values(name, actual, expected, _path):
        harness.values(name, actual, expected)

    for kind in KINDS:
        for destination in (*LOCAL_FORMATS, "fanout"):
            name = f"unary-membership-{kind}-write-{destination}"

            def destination_case(name=name, kind=kind, destination=destination):
                rows = ([{"duplicated": False} for _ in range(257)] if kind.startswith("mask-") else
                        [row(index, unique=True, width=8192) for index in range(257)])
                schema = {"n": "int64", "s": "utf8"}
                batch, trace = source(harness, unique=True, count=257, width=8192)
                if kind == "distinct":
                    query = batch.distinct()
                else:
                    keep = kind.split("-", 1)[1]
                    method = batch.drop_duplicates if kind.startswith("dedup-") else batch.duplicated
                    query = method("s", keep=False if keep == "false" else keep)

                case_name = name
                formats = LOCAL_FORMATS if destination == "fanout" else (destination,)
                targets = {
                    extension: harness.output / f"{case_name}.{extension}"
                    for extension in formats
                }
                report = query.fanout(targets, **requested) if destination == "fanout" else getattr(
                    query, f"write_{destination}")(targets[destination], **requested)
                envelope = report.envelope
                complete(harness, case_name, envelope, [trace], [True], grant=GRANT)
                assert envelope.field_int("relational_spillable_unary_membership_stages") == 1
                for suffix in ("input_rows", "observed_rows", "groups", "output_rows"):
                    assert envelope.field_int(f"relational_spillable_unary_{suffix}") == 257
                assert envelope.field_int("relational_spill_runs_written") > 0
                assert envelope.field_bool("relational_spill_owned_cleanup_completed") is True
                assert envelope.field_int("relational_spill_peak_disk_bytes") <= spill["quota_bytes"]
                assert envelope.field_int("native_input_replay_sources") == 0
                if destination == "fanout":
                    assert envelope.field_int("native_vortex_result_export_execution_count") == 1
                    assert envelope.field_int("native_vortex_result_export_target_count") == len(formats)
                    assert envelope.field_int("native_vortex_result_export_replay_count") == len(formats)
                columns = ["duplicated"] if kind.startswith("mask-") else list(schema)
                for extension, path in targets.items():
                    verify_output(
                        harness.context, path, extension, rows, columns,
                        name=case_name + "-" + extension,
                        accepted=accepted,
                        complete=values,
                    )
                clean()
                return {
                    "artifacts": [harness.artifact(path) for path in targets.values()],
                    "complete_rows_per_output": len(rows),
                    "producer_executions": trace["opened"],
                    "spill_runs_written": envelope.field_int("relational_spill_runs_written"),
                }

            harness.case(name, destination_case)

    for kind in KINDS:
        for failure in ("producer", "corrupt", "quota"):
            for terminal in ("batches", "vortex", "fanout"):
                name = f"unary-membership-{kind}-{failure}-{terminal}"

                def publication_failure(name=name, kind=kind, failure=failure, terminal=terminal):
                    batch, trace = source(
                        harness, kind, unique=True, workspace=workspace, failure=failure,
                    )
                    if kind == "distinct":
                        query = batch.distinct()
                    else:
                        keep = kind.split("-", 1)[1]
                        method = batch.drop_duplicates if kind.startswith("dedup-") else batch.duplicated
                        query = method("s", keep=False if keep == "false" else keep)

                    selected = dict(spill, quota_bytes=2 << 20) if failure == "quota" else spill
                    targets = {
                        extension: harness.output / f"{name}.{extension}"
                        for extension in (("vortex", "jsonl") if terminal == "fanout" else ("vortex",))
                    }
                    before = set(harness.output.iterdir())
                    iterator = None
                    try:
                        if terminal == "batches":
                            iterator = query.iter_batches(batch_rows=257, spill=selected, **POLICY)
                            with iterator:
                                for _ in iterator:
                                    assert iterator.report is None
                        elif terminal == "vortex":
                            query.write_vortex(targets["vortex"], spill=selected, **POLICY)
                        else:
                            query.fanout(targets, spill=selected, **POLICY)
                    except (RuntimeError, ShardLoomCommandError) as error:
                        assert trace["fault"] or failure == "quota"
                        if failure == "producer":
                            assert type(error) is RuntimeError
                            assert str(error) == "unary replay producer sentinel"
                        else:
                            assert isinstance(error, ShardLoomCommandError)
                            harness.envelope(name, error.envelope, success=False)
                            expected_diagnostic = "quota" if failure == "quota" else "byte length changed"
                            assert expected_diagnostic in json.dumps(error.envelope.raw)
                    else:
                        raise AssertionError("failed unary membership spill returned a complete report")

                    assert not any(path.exists() for path in targets.values())
                    assert set(harness.output.iterdir()) == before
                    assert not trace["opened"] or trace["closed"]
                    assert trace["opened"] <= 1
                    if failure in ("producer", "corrupt"):
                        assert trace["fault"] and trace["opened"] == 1 and not trace["ended"]
                    if iterator is not None:
                        assert iterator.report is None and iterator._process.poll() is not None
                    clean()

                    recovery_rows = [row(0, unique=True, width=2048)]
                    recovery_source, recovery_trace = frame(
                        harness, recovery_rows, {"n": "int64", "s": "utf8"}, width=1,
                    )
                    recovery = recovery_source.distinct().collect(check=True, **POLICY)
                    harness.values(name + "-recovery", recovery.result_rows, recovery_rows)
                    complete(harness, name + "-recovery", recovery.envelope, [recovery_trace], [True],
                             grant=GRANT)
                    clean()
                    return {"failure": failure, "terminal": terminal, "unary_kind": kind,
                            "recovery_rows": len(recovery_rows), "failed_trace": trace}

                harness.case(name, publication_failure)
