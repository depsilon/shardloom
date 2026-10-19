# SPDX-License-Identifier: Apache-2.0
"""Exact public distinct-set completion, native run pressure and publication failures."""

from __future__ import annotations

import gzip
import hashlib
import json

from shardloom.errors import ShardLoomCommandError
from native_streaming_order_pressure_cases import encoded
from native_streaming_set_cases import combine, complete
from native_workflow_outputs import LOCAL_FORMATS, verify_output


def reference(kind, count, offset, row):
    """Independent key-domain oracle with descending first-left occurrence order."""
    for value in range(count - 1, -1, -1):
        present = offset <= value < offset + count
        if kind == "union" or present == (kind == "intersect"):
            yield row(value)
    if kind == "union":
        for value in range(offset, offset + count):
            if value >= count:
                yield row(value)


def run(harness, source, spill, clean):
    # Imported after the parent family has loaded; the pressure fixture and
    # complete-value recorder are shared without maintaining another protocol.
    from native_streaming_set_pressure_cases import GRANT, POLICY, ROWS, capture, row

    kinds = ("union", "intersect", "except_")
    offsets = (0, ROWS // 2, ROWS)
    oracles = {}
    for kind in kinds:
        for offset in offsets:
            digest, count = hashlib.sha256(), 0
            for value in reference(kind, ROWS, offset, row):
                digest.update(encoded(value))
                count += 1
            oracles[f"{kind}-{offset}"] = {"rows": count, "sha256": digest.hexdigest()}
    harness.json(harness.logs / "set-membership-spill-fixtures.json", {
        "rows_per_source": ROWS, "grant": GRANT, "spill": spill,
        "left_keys": "ROWS - 1 - input_row", "right_keys": "offset + input_row",
        "right_offsets": offsets, "kinds": kinds, "writer_formats": LOCAL_FORMATS,
        "oracle": "exact integer domain membership; descending left then unseen right order",
        "oracles": oracles,
    })

    def verify(name, envelope, traces, expected_rows, offset):
        complete(harness, name, envelope, traces, [True, True], grant=GRANT)
        assert envelope.field_int("relational_ordered_set_stages") == 1
        assert envelope.field_int("relational_ordered_set_input_rows") == ROWS * 2
        assert envelope.field_int("relational_ordered_set_groups") == ROWS + min(ROWS, offset)
        assert envelope.field_int("relational_ordered_set_output_rows") == expected_rows
        assert envelope.field_int("native_input_logical_bytes") > GRANT * 2
        assert envelope.field_int("native_input_replay_spilled_sources") == 0
        assert envelope.field_int("relational_spill_runs_written") > 3
        assert envelope.field_int("relational_spill_merge_passes") > 1
        assert envelope.field_bool("relational_spill_owned_cleanup_completed") is True
        assert envelope.field_int("relational_spill_peak_disk_bytes") <= spill["quota_bytes"]
        clean()

    for kind in kinds:
        for offset in offsets:
            for surface in ("dataframe", "sql"):
                name = f"set-membership-spilled-{kind}-{offset}-{surface}"
                def execute(name=name, kind=kind, offset=offset, surface=surface):
                    left, a = source(0, unique=True, reverse=True)
                    right, b = source(1, unique=True, offset=offset)
                    query = combine(left, right, kind, surface)
                    envelope, record = capture(harness, name,
                        query.iter_batches(batch_rows=257, spill=spill, **POLICY),
                        lambda: reference(kind, ROWS, offset, row))
                    verify(name, envelope, [a, b], record["complete_rows"], offset)
                    assert record["expected_sha256"] == oracles[f"{kind}-{offset}"]["sha256"]
                    return record
                harness.case(name, execute)

    def accepted(name, report):
        harness.envelope(name, report.envelope)
        return report.envelope

    def checked_values(name, actual, expected, _destination):
        assert actual == expected, name
        # Keep every independently checked value compactly, including CSV's
        # format-specific spellings. Reopen the new archive before accepting it.
        path = harness.logs / f"{name}-values.jsonl.gz"
        digest = hashlib.sha256()
        with path.open("xb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as stream:
            for value in actual:
                data = encoded(value)
                digest.update(data)
                stream.write(data)
        reread = hashlib.sha256()
        count = 0
        with gzip.open(path, "rb") as stream:
            for count, line in enumerate(stream, 1):
                assert json.loads(line) == expected[count - 1]
                reread.update(line)
        assert count == len(expected) and reread.digest() == digest.digest()
        harness.json(harness.logs / f"{name}-values.json", {
            "complete_rows": count, "sha256": digest.hexdigest(), "values": harness.artifact(path),
        })

    for kind in kinds:
        for destination in LOCAL_FORMATS:
            name = f"set-membership-spilled-{kind}-write-{destination}"
            def write(name=name, kind=kind, destination=destination):
                offset = ROWS // 2
                left, a = source(0, unique=True, reverse=True)
                right, b = source(1, unique=True, offset=offset)
                query = combine(left, right, kind)
                target = harness.output / f"{name}.{destination}"
                expected = list(reference(kind, ROWS, offset, row))
                report = getattr(query, "write_" + destination)(target, spill=spill, **POLICY)
                verify(name, report.envelope, [a, b], len(expected), offset)
                verify_output(harness.context, target, destination, expected, ("n", "s"),
                              name=name, accepted=accepted, complete=checked_values)
                return {"output": harness.artifact(target), "complete_rows": len(expected)}
            harness.case(name, write)

    for destination in LOCAL_FORMATS:
        name = f"set-membership-spilled-except-empty-write-{destination}"
        def empty(name=name, destination=destination):
            left, a = source(0, unique=True, reverse=True)
            right, b = source(1, unique=True)
            target = harness.output / f"{name}.{destination}"
            report = getattr(combine(left, right, "except_"), "write_" + destination)(
                target, spill=spill, **POLICY)
            verify(name, report.envelope, [a, b], 0, 0)
            verify_output(harness.context, target, destination, [], ("n", "s"),
                          name=name, accepted=accepted, complete=checked_values)
            return {"output": harness.artifact(target), "complete_rows": 0}
        harness.case(name, empty)

    for kind in kinds:
        name = f"set-membership-spilled-{kind}-fanout-all-eight"
        def fanout(name=name, kind=kind):
            offset = ROWS // 2
            left, a = source(0, unique=True, reverse=True)
            right, b = source(1, unique=True, offset=offset)
            targets = {destination: harness.output / f"{name}.{destination}" for destination in LOCAL_FORMATS}
            expected = list(reference(kind, ROWS, offset, row))
            report = combine(left, right, kind).fanout(targets, spill=spill, **POLICY)
            verify(name, report.envelope, [a, b], len(expected), offset)
            assert report.envelope.field_int("native_vortex_result_export_execution_count") == 1
            assert report.envelope.field_int("native_vortex_result_export_replay_count") == len(LOCAL_FORMATS)
            for destination, target in targets.items():
                verify_output(harness.context, target, destination, expected, ("n", "s"),
                              name=f"{name}-{destination}", accepted=accepted, complete=checked_values)
            return {"outputs": {destination: harness.artifact(target) for destination, target in targets.items()},
                    "complete_rows": len(expected)}
        harness.case(name, fanout)

    for kind in kinds:
        for failure in ("producer", "corrupt", "quota"):
            for write in (False, True):
                for side in ((0,) if failure == "quota" else (0, 1)):
                    name = f"set-membership-spill-{kind}-{failure}-side-{side}-write-{write}"
                    def failed(name=name, kind=kind, failure=failure, write=write, side=side):
                        left, a = source(0, unique=True, failure=failure if side == 0 else None)
                        right, b = source(1, unique=True, failure=failure if side == 1 else None)
                        query = combine(left, right, kind)
                        requested = dict(spill, quota_bytes=2 << 20) if failure == "quota" else spill
                        target = harness.output / f"{name}.vortex"
                        before = set(harness.output.iterdir())
                        iterator = None
                        try:
                            if write:
                                query.write_vortex(target, spill=requested, **POLICY)
                            else:
                                iterator = query.iter_batches(batch_rows=257, spill=requested, **POLICY)
                                with iterator:
                                    for _ in iterator:
                                        assert iterator.report is None
                        except (RuntimeError, ShardLoomCommandError) as error:
                            assert (a, b)[side]["fault"] or failure == "quota"
                            if failure == "producer":
                                assert type(error) is RuntimeError and str(error) == "set replay producer sentinel"
                            else:
                                assert isinstance(error, ShardLoomCommandError)
                                harness.envelope(name, error.envelope, success=False)
                                expected = "quota" if failure == "quota" else "byte length changed"
                                assert expected in json.dumps(error.envelope.raw)
                        else:
                            raise AssertionError("failed membership spill returned complete output")
                        assert all(not trace["opened"] or trace["closed"] for trace in (a, b))
                        assert not target.exists() and set(harness.output.iterdir()) == before
                        if iterator is not None:
                            assert iterator.report is None and iterator._process.poll() is not None
                        clean()
                    harness.case(name, failed)
