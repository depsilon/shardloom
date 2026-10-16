# SPDX-License-Identifier: Apache-2.0
"""No final completion or publication after interrupted finite unary input."""

from __future__ import annotations

import json

from shardloom.errors import ShardLoomCommandError
from native_streaming_unary_cases import KINDS, POLICY, apply, fixture, frame, oracle


def run(harness):
    workspace = harness.output / "unary-fault-spill"
    workspace.mkdir()
    foreign = workspace / "foreign"
    foreign.write_bytes(b"preserve")
    spill = {"workspace": str(workspace), "quota_bytes": 128 << 20, "buffer_bytes": 1 << 20}
    requested = dict(POLICY, spill=spill)

    def clean():
        assert foreign.read_bytes() == b"preserve" and list(workspace.iterdir()) == [foreign]

    for kind in KINDS:
        for side in ("standalone", "left", "right"):
            for failure in ("producer", "schema"):
                for terminal in ("collect", "zero", "vortex", "fanout"):
                    name = f"unary-fault-{kind}-{side}-{failure}-{terminal}"

                    def fail(name=name, kind=kind, side=side, failure=failure, terminal=terminal):
                        rows, schema, _ = fixture(kind)
                        events, sentinel = [], LookupError(name + " sentinel")

                        def broken():
                            events.append("opened")
                            try:
                                yield rows[:2]
                                events.append("fault")
                                if failure == "producer":
                                    raise sentinel
                                yield [{"changed": 1}]
                            finally:
                                events.append("closed")

                        source = harness.context.from_batches(broken, schema=schema, streaming=True)
                        query = apply(source, kind)
                        other_trace = None
                        if side != "standalone":
                            other, other_trace = frame(harness, rows, schema)
                            other = apply(other, kind)
                            query = query.union_all(other) if side == "left" else other.union_all(query)
                        targets = {extension: harness.output / (name + "." + extension)
                                   for extension in ("vortex", "jsonl")}
                        before, iterator = set(harness.output.iterdir()), None
                        try:
                            if terminal == "collect":
                                query.collect(check=True, **requested)
                            elif terminal == "zero":
                                iterator = query.limit(0).iter_batches(batch_rows=1, **requested)
                                with iterator:
                                    assert list(iterator) == []
                            elif terminal == "vortex":
                                query.write_vortex(targets["vortex"], **requested)
                            else:
                                query.fanout(targets, **requested)
                        except (LookupError, ValueError) as error:
                            if failure == "producer":
                                assert error is sentinel
                            else:
                                assert isinstance(error, ValueError)
                                assert "schema" in str(error) or "field" in str(error), str(error)
                        else:
                            raise AssertionError("unary input failure was hidden by completion")
                        assert events == ["opened", "fault", "closed"]
                        if other_trace is not None:
                            assert other_trace["opened"] in (0, 1)
                            assert not other_trace["opened"] or other_trace["closed"]
                        if iterator is not None:
                            assert iterator.report is None and iterator._process.poll() is not None
                        assert set(harness.output.iterdir()) == before
                        assert not any(path.exists() for path in targets.values())
                        clean()
                        # The same public client must remain usable after producer failure.
                        check = harness.context.from_rows([{"n": 7}], schema={"n": "int64"}).collect(
                            check=True, **POLICY)
                        harness.envelope(name + "-clean-reuse", check.envelope)
                        assert check.result_rows == ({"n": 7},)
                        return {"producer_events": events, "final_publication": False,
                                "original_exception_preserved": failure == "producer"}
                    harness.case(name, fail)

        name = "unary-early-close-" + kind

        def close(name=name, kind=kind):
            rows, schema, _ = fixture(kind)
            source, trace = frame(harness, rows, schema, width=1)
            query = apply(source, kind)
            with query.iter_batches(batch_rows=1, **requested) as iterator:
                retained = next(iterator)
                assert iterator.report is None
            assert iterator.report is None and iterator._process.poll() is not None
            assert trace["opened"] == 1 and trace["closed"]
            # A future-dependent operation can finish its input before first output.
            # Unacknowledged remaining output must still prevent a final report.
            harness.values(name, retained.result_rows, oracle(kind, rows)[:1])
            clean()
            return {"producer_trace": trace, "completion_report": None}
        harness.case(name, close)

    invalid_weights(harness)


def invalid_weights(harness):
    for weight in (None, 0.0, -1.0):
        for replace in (False, True):
            name = f"unary-sample-invalid-weight-{weight}-replace-{replace}"

            def fail(name=name, weight=weight, replace=replace):
                rows = [{"n": index, "weight": value} for index, value in enumerate((1.0, 2.0, weight))]
                source, trace = frame(harness, rows, {"n": "int64", "weight": "float64"}, width=1)
                iterator = source.sample(1, random_state=7, weights="weight", replace=replace).iter_batches(
                    batch_rows=1, **POLICY)
                try:
                    with iterator:
                        for _ in iterator:
                            assert iterator.report is None
                except ShardLoomCommandError as error:
                    harness.envelope(name, error.envelope, success=False)
                    message = json.dumps(error.envelope.raw).lower()
                    assert "weight" in message and ("positive" in message or "null" in message), message
                else:
                    raise AssertionError("invalid sample weight was silently ignored")
                assert iterator.report is None and iterator._process.poll() is not None
                assert trace["opened"] == 1 and trace["rows"] == 3 and trace["closed"]
                return {"producer_trace": trace, "completion_report": None}
            harness.case(name, fail)
