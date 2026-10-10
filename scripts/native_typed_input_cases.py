# SPDX-License-Identifier: Apache-2.0
"""Exact typed input through public SQL/DataFrame collection, iteration and files."""

from __future__ import annotations

from decimal import localcontext
import json
import math
import struct
import time

from native_typed_input_fixtures import rich_fixture
from native_streaming_protocol_cases import Peer
from native_workflow_outputs import LOCAL_FORMATS, write_outputs
from shardloom._result_schema import ResultType as T
from shardloom.errors import ShardLoomCommandError
from shardloom.query import SqlWorkflow


def run(harness):
    context = harness.context
    policy = {"memory_gb": 1, "max_parallelism": 1}
    schema, fields, rows, expected = rich_fixture()
    columns = tuple(reversed(schema))
    fields = tuple(reversed(fields))
    expected = [{name: row[name] for name in columns} for row in expected]

    def source(mode, supplied, declaration=schema):
        # An input factory deliberately permits each complete workflow to replay.
        if mode == "rows":
            return context.from_rows(supplied, schema=declaration)
        return context.from_batches(lambda: iter([[], supplied[:2], [], supplied[2:], []]),
                                    schema=declaration, streaming=mode == "streaming")

    def query(mode, front, supplied, declaration=schema, selected=columns):
        frame = source(mode, supplied, declaration)
        if front == "dataframe":
            return frame.select(*selected)
        return SqlWorkflow(f"SELECT {','.join(selected)} FROM '{frame.source.uri}'", harness.client,
                           source_bindings=frame._declared_sources())

    def exact_schema(result, wanted=fields):
        assert result.result_columns == tuple(name for name, _ in wanted)
        assert result.result_schema == wanted, (result.result_schema, wanted)

    def accepted(name, report):
        harness.envelope(name, report.envelope)
        return report.envelope

    def complete(name, actual, wanted, _destination=None):
        harness.values(name, actual, wanted)

    def denied(name, report, destination, message):
        harness.envelope(name, report.envelope, success=False)
        assert message in json.dumps(report.envelope.raw), report.envelope.raw
        assert not destination.exists()

    profiles = (("values", rows, expected), ("empty", [], []),
                ("all-null", [dict.fromkeys(schema)], [dict.fromkeys(columns)]))
    for profile, supplied, wanted in profiles:
        for mode in ("rows", "resident", "streaming"):
            for front in ("dataframe", "sql"):
                name = f"typed-{profile}-{mode}-{front}"

                def collect(name=name, mode=mode, front=front, supplied=supplied, wanted=wanted):
                    with localcontext() as decimal_context:
                        decimal_context.prec = 2
                        report = query(mode, front, supplied).collect(check=True, **policy)
                    exact_schema(report)
                    harness.values(name + "-collect", report.result_rows, wanted)
                    if mode == "streaming":
                        harness.completed(name + "-collect", report.envelope, batches=5, rows=len(supplied))
                    else:
                        harness.envelope(name + "-collect", report.envelope)
                    return {"complete_rows_verified": len(wanted), "exact_recursive_schema": True}
                harness.case(name + "-collect", collect)

                def incremental(name=name, mode=mode, front=front, supplied=supplied, wanted=wanted):
                    with query(mode, front, supplied).iter_batches(batch_rows=1, **policy) as iterator:
                        retained = []
                        for batch in iterator:
                            exact_schema(batch)
                            retained.append(batch)
                            assert iterator.report is None
                        assert iterator.report is not None and iterator._process.poll() == 0
                        assert iterator._schema == fields
                        harness.values(name + "-iter", [row for batch in retained for row in batch.result_rows], wanted)
                        if mode == "streaming":
                            harness.completed(name + "-iter", iterator.report.envelope, batches=5, rows=len(supplied))
                        else:
                            harness.envelope(name + "-iter", iterator.report.envelope)
                    return {"retained_python_batches": len(retained), "exact_recursive_schema": True}
                harness.case(name + "-iter", incremental)

                def outputs(name=name, mode=mode, front=front, supplied=supplied, wanted=wanted, profile=profile):
                    formats = ("vortex",) if mode == "streaming" else LOCAL_FORMATS
                    denials = {"orc": "ORC does not admit nested output"}
                    if profile == "values":
                        denials["avro"] = "exceeds i64::MAX"

                    def written(label, report):
                        if mode == "streaming":
                            harness.completed(label, report.envelope, batches=5, rows=len(supplied))
                        else:
                            accepted(label, report)

                    def prepared(label, report):
                        envelope = accepted(label, report)
                        if label.endswith("-prepare"):
                            assert "file_min_max=omitted_for_full_domain_timestamp" in json.dumps(envelope.raw)
                            assert "timestamp_fields=preserved_uncompressed" in json.dumps(envelope.raw)
                        return envelope

                    destinations = write_outputs(
                        context, harness.output, query(mode, front, supplied), wanted, columns,
                        name=name, guard=harness.guard, accepted=prepared, complete=complete,
                        formats=formats, execution=policy, csv_json_columns=("blob", "amount"),
                        denied_formats=denials, denied=denied, written=written)
                    # Highest-fidelity persistence must preserve all DType parameters.
                    reopened = context.read_vortex(destinations["vortex"]).collect(check=True, **policy)
                    accepted(name + "-vortex-exact-schema", reopened)
                    exact_schema(reopened)
                    harness.values(name + "-vortex-exact-schema", reopened.result_rows, wanted)
                    return {"artifacts": [harness.artifact(path) for path in destinations.values()],
                            "denied_formats": {key: value for key, value in denials.items() if key in formats}}
                harness.case(name + "-outputs", outputs)

    def operators(mode, front):
        name = f"typed-operators-{mode}-{front}"
        frame = source(mode, rows)
        selected = ("blob", "amount", "u64", "samples", "detail")
        if front == "sql":
            filtered = SqlWorkflow(
                f"SELECT {','.join(selected)} FROM '{frame.source.uri}' WHERE flag = FALSE", harness.client,
                source_bindings=frame._declared_sources())
            aggregate = SqlWorkflow(
                f"SELECT COUNT(*) AS n,SUM(amount) AS total,MIN(u64) AS low,MAX(u64) AS high FROM '{frame.source.uri}'",
                harness.client, source_bindings=frame._declared_sources())
            grouped = SqlWorkflow(
                f"SELECT samples,COUNT(*) AS n FROM '{frame.source.uri}' WHERE flag IS NOT NULL GROUP BY samples ORDER BY samples",
                harness.client, source_bindings=frame._declared_sources())
        else:
            filtered = frame.filter("flag = FALSE").select(*selected)
            aggregate = frame.aggregate("COUNT(*) AS n", "SUM(amount) AS total", "MIN(u64) AS low", "MAX(u64) AS high")
            grouped = frame.filter("flag IS NOT NULL").group_by("samples").aggregate("COUNT(*) AS n").sort("samples")
        for label, workflow, wanted in (
            ("filtered", filtered, [{key: expected[1][key] for key in selected}]),
            ("aggregate", aggregate, [{"n": 4, "total": "decimal128(38,2):0", "low": 0, "high": (1 << 64) - 1}]),
            ("nested-keys", grouped, [{"samples": [], "n": 1}, {"samples": [[1, -2], None, [-32768, 32767]], "n": 1}]),
        ):
            result = workflow.collect(check=True, **policy)
            if mode == "streaming":
                harness.completed(f"{name}-{label}", result.envelope, batches=5, rows=len(rows))
            else:
                accepted(f"{name}-{label}", result)
            harness.values(f"{name}-{label}", result.result_rows, wanted)
        return {"shared_typed_operator_workflows": 3}

    for mode in ("rows", "resident", "streaming"):
        for front in ("dataframe", "sql"):
            harness.case(f"typed-operators-{mode}-{front}", lambda mode=mode, front=front: operators(mode, front))

    def compatibility_values(extension):
        local_rows = [dict(row) for row in rows]
        local_expected = [dict(row) for row in expected]
        # Both pinned writers explicitly reject uint64 values above i64::MAX.
        local_rows[1]["u64"] = local_expected[1]["u64"] = (1 << 63) - 1
        selected = columns if extension == "avro" else tuple(name for name in columns if name not in {
            "amount", "day", "stamp", "samples", "detail", "empty_vector"})
        declaration = {name: schema[name] for name in selected}
        supplied = [{name: row[name] for name in selected} for row in local_rows]
        wanted = [{name: row[name] for name in selected} for row in local_expected]
        destinations = write_outputs(
            context, harness.output, source("resident", supplied, declaration).select(*selected), wanted, selected,
            name="typed-compatible-values", guard=harness.guard, accepted=accepted, complete=complete,
            formats=(extension,), execution=policy, csv_json_columns=("blob", "amount"))
        return {"artifacts": [harness.artifact(path) for path in destinations.values()]}
    for extension in ("avro", "orc"):
        harness.case(f"typed-{extension}-compatible-values", lambda extension=extension: compatibility_values(extension))

    def float_bits():
        values = {"f32": [0.0, -0.0, 2.0 ** -149, -(2.0 ** -149),
                           struct.unpack("!f", bytes.fromhex("7f7fffff"))[0]],
                  "f64": [0.0, -0.0, math.ulp(0.0), -math.ulp(0.0), float.fromhex("0x1.fffffffffffffp+1023")]}
        supplied = [{name: column[index] for name, column in values.items()} for index in range(5)]
        declaration = {name: {"type": "float32" if name == "f32" else "float64", "nullable": False} for name in values}
        for mode in ("rows", "resident", "streaming"):
            frame = source(mode, supplied, declaration)
            target = harness.output / f"typed-float-bits-{mode}.vortex"
            written = frame.write_vortex(target, **policy)
            accepted(f"typed-float-bits-{mode}-write", written)
            result = context.read_vortex(target).collect(check=True, **policy)
            accepted(f"typed-float-bits-{mode}-reopen", result)
            exact_schema(result, (("f32", T("float32", False)), ("f64", T("float64", False))))
            for index, row in enumerate(result.result_rows):
                for name, code in (("f32", "!f"), ("f64", "!d")):
                    assert struct.pack(code, row[name]) == struct.pack(code, values[name][index])
            assert len(result.result_rows) == 5
        return {"complete_bitwise_float_values_verified": 30}
    harness.case("typed-float-bits-write-reopen", float_bits)

    def failure(mode, write):
        name = f"typed-late-{mode}-{'write' if write else 'iter'}"
        closed, pulls = [], []

        def produce():
            try:
                pulls.append(0)
                yield rows[:1]
                pulls.append(1)
                if mode == "producer":
                    raise RuntimeError("typed input late producer sentinel")
                broken = dict(rows[1])
                if mode == "fixed-shape":
                    broken["samples"] = [[1]]
                else:
                    broken["detail"] = dict(rows[0]["detail"], enabled=None)
                yield [broken]
            finally:
                closed.append(True)

        frame = context.from_batches(produce(), schema=schema, streaming=True)
        target = harness.output / (name + ".vortex")
        before = set(harness.output.iterdir())
        iterator = None
        try:
            if write:
                frame.write_vortex(target, **policy)
            else:
                iterator = frame.iter_batches(batch_rows=1, **policy)
                with iterator:
                    first = next(iterator)
                    harness.values(name + "-provisional", first.result_rows,
                                   [{key: expected[0][key] for key in schema}])
                    assert iterator.report is None and pulls == [0]
                    next(iterator)
        except (RuntimeError if mode == "producer" else ValueError) as error:
            if mode == "producer":
                assert str(error) == "typed input late producer sentinel"
            else:
                assert ("wrong width" if mode == "fixed-shape" else "nonnullable") in str(error)
        else:
            raise AssertionError("typed input late failure was ignored")
        assert pulls == [0, 1] and closed == [True]
        assert set(harness.output.iterdir()) == before and not target.exists()
        if iterator is not None:
            assert iterator.report is None and iterator._process.poll() is not None
        return {"successful_prefix": False, "producer_closed": True, "staging_remaining": False}

    for mode in ("producer", "fixed-shape", "nonnull"):
        for write in (False, True):
            harness.case(f"typed-late-{mode}-{'write' if write else 'iter'}",
                         lambda mode=mode, write=write: failure(mode, write))

    def cancel():
        pulled, closed = [], []

        def produce():
            try:
                for index, row in enumerate(rows):
                    pulled.append(index)
                    yield [row]
            finally:
                closed.append(True)

        with context.from_batches(produce(), schema=schema, streaming=True).iter_batches(
                batch_rows=1, **policy) as iterator:
            first = next(iterator)
            assert pulled == [0] and iterator.report is None
            time.sleep(0.01)
            assert pulled == [0]
        harness.values("typed-cancel-retained", first.result_rows, [{key: expected[0][key] for key in schema}])
        assert closed == [True] and iterator.report is None and iterator._process.poll() is not None
        return {"consumed_before_cancel": 1, "producer_closed": True, "successful_completion_reported": False}
    harness.case("typed-cancel-retained", cancel)

    def deny_stream_outputs():
        for extension in LOCAL_FORMATS[1:]:
            name = f"typed-stream-deny-{extension}"
            opened = []

            def produce():
                opened.append(True)
                return iter([rows])

            frame = context.from_batches(produce, schema=schema, streaming=True)
            target = harness.output / f"{name}.{extension}"
            before = set(harness.output.iterdir())
            try:
                getattr(frame, f"write_{extension}")(target, **policy)
            except ShardLoomCommandError as error:
                harness.envelope(name, error.envelope, success=False)
                assert "SL-NATIVE-BATCH" in json.dumps(error.envelope.raw)
            else:
                raise AssertionError("streaming compatibility output unexpectedly admitted")
            assert opened == [] and set(harness.output.iterdir()) == before
        return {"denied_before_producer_open": len(LOCAL_FORMATS) - 1}
    harness.case("typed-stream-compatibility-denied", deny_stream_outputs)

    native_schema = {"v": {"type": "struct", "fields": {
        "n": {"type": "uint8", "nullable": False},
        "xs": {"type": "fixed_size_list", "size": 2, "item": "int16"}}}}
    faults = (("duplicate", '{"n":1,"n":2,"xs":[1,2]}', "duplicate typed input JSON field"),
              ("nonnull", '{"n":null,"xs":[1,2]}', "nonnullable"),
              ("width", '{"n":256,"xs":[1,2]}', "declared width"),
              ("shape", '{"n":1,"xs":[1]}', "wrong width"),
              ("fields", '{"n":1,"xs":[1,2],"extra":0}', "complete declared fields"),
              ("wide-integer", '{"n":18446744073709551617,"xs":[1,2]}', "exact 64-bit domain"))
    for label, raw, diagnostic in faults:
        for write in (False, True):
            name = f"typed-native-{label}-{'write' if write else 'iter'}"

            def native_failure(name=name, raw=raw, diagnostic=diagnostic, write=write):
                frame = context.from_batches([], schema=native_schema, streaming=True)
                before = set(harness.output.iterdir())
                with Peer(harness, name, source=frame, write=write) as peer:
                    peer.demand(0)
                    peer.rows(0, [['{"n":1,"xs":[1,-1]}']])
                    if not write:
                        batch = peer.read()
                        assert batch["kind"] == "batch" and batch["rows"] == [{"v": {"n": 1, "xs": [1, -1]}}]
                        peer.send({"kind": "ack", "index": 0})
                    peer.demand(1)
                    peer.rows(1, [[raw]])
                    result = peer.failed()
                    assert diagnostic in json.dumps(peer.frames[-1]["value"]), peer.frames[-1]
                assert set(harness.output.iterdir()) == before
                return result
            harness.case(name, native_failure)

    invalid_types = (
        ("duplicate", '{"Bool":true,"Bool":false}', "duplicate typed input JSON field"),
        ("arity", '{"Struct":[{"names":["a"],"dtypes":[]},true]}', "equal length"),
        ("extension", '{"Extension":{"id":"unknown","metadata":[],"storage_dtype":{"Primitive":["i32",true]}}}', "unsupported native input extension"),
        ("fixed-expansion", '{"FixedSizeList":[{"Primitive":["i64",false]},4294967295,true]}', "typed native input byte bound"),
    )
    for label, raw, diagnostic in invalid_types:
        name = "typed-native-schema-" + label

        def invalid_schema(name=name, raw=raw, diagnostic=diagnostic, label=label):
            frame = context.from_batches([], schema={"v": "uint8"}, streaming=True)
            memory_input = dict(frame.source.memory_input)
            memory_input["schema"] = [["v", {"native": {"encoding": "vortex.dtype.serde.v1", "dtype": raw}}]]
            before = set(harness.output.iterdir())
            with Peer(harness, name, source=frame, memory_input=memory_input, write=True) as peer:
                if label == "fixed-expansion":
                    peer.demand(0)
                    peer.rows(0, [[None]])
                result = peer.failed()
                assert diagnostic in json.dumps(peer.frames[-1]["value"]), peer.frames[-1]
            assert set(harness.output.iterdir()) == before
            return result
        harness.case(name, invalid_schema)
