# SPDX-License-Identifier: Apache-2.0
"""Set branches compose with existing native operators, rich values and failures."""

from collections import Counter

from shardloom.query import SqlWorkflow
from native_streaming_dynamic_cases import frame as rich_frame, pivot, EXPECTED as PIVOT_VALUES
from native_streaming_set_cases import KINDS, LEFT, RIGHT, POLICY, batch_frame, combine, complete, oracle
from native_typed_input_fixtures import rich_fixture, rich_python_values
from native_typed_input_cases import assert_python_values


def ordered(rows):
    return sorted(rows, key=lambda row: (row["n"] is None, row["n"] or 0,
                                         row["s"] is None, row["s"] or ""))


def run(harness):
    directory = harness.output / "set-composition-spill"
    directory.mkdir()
    foreign = directory / "foreign"
    foreign.write_bytes(b"preserve")
    spill = {"workspace": str(directory), "quota_bytes": 128 << 20, "buffer_bytes": 1 << 20}

    def clean():
        assert foreign.read_bytes() == b"preserve" and list(directory.iterdir()) == [foreign]

    for stored in (False, True):
        requested = dict(POLICY, spill=spill) if stored else POLICY
        for kind in KINDS:
            for shape in ("filter-project", "sort", "limit-zero", "limit-two", "group", "join", "window", "nested"):
                name = f"set-composed-{kind}-{shape}-spill-{stored}"
                def composed(name=name, kind=kind, shape=shape, requested=requested):
                    left, a = batch_frame(harness, LEFT)
                    right, b = batch_frame(harness, RIGHT)
                    query = combine(left, right, kind)
                    expected = oracle(LEFT, RIGHT, kind)
                    sql = query._relation_statement()
                    bindings = query._declared_sources()
                    replay = 0
                    if shape == "filter-project":
                        sql = f"SELECT n*2 AS value,s AS label FROM ({sql}) AS q WHERE n>=2"
                        expected = [{"value": row["n"] * 2, "label": row["s"]} for row in expected
                                    if row["n"] is not None and row["n"] >= 2]
                    elif shape == "sort":
                        sql = f"SELECT * FROM ({sql}) AS q ORDER BY n ASC NULLS LAST,s ASC NULLS LAST"
                        expected = ordered(expected)
                    elif shape.startswith("limit-"):
                        count = 0 if shape == "limit-zero" else 2
                        sql = f"SELECT * FROM ({sql}) AS q LIMIT {count}"
                        expected = expected[:count]
                    elif shape == "group":
                        sql = f"SELECT s AS label,COUNT(*) AS rows FROM ({sql}) AS q GROUP BY s ORDER BY label ASC NULLS LAST"
                        counts = Counter(row["s"] for row in expected)
                        expected = [{"label": label, "rows": counts[label]} for label in
                                    sorted(counts, key=lambda label: (label is None, label or ""))]
                    elif shape == "join":
                        dimension = harness.context.from_rows([{"n": value} for value in (1, 2, 3, 9)], schema={"n": "int64"})
                        bindings += dimension._declared_sources()
                        sql = (f"SELECT q.n AS n,q.s AS s FROM ({sql}) AS q JOIN '{dimension.source.uri}' AS d "
                               "ON q.n=d.n ORDER BY n ASC NULLS LAST,s ASC NULLS LAST")
                        expected = ordered([row for row in expected if row["n"] in (1, 2, 3, 9)])
                    elif shape == "window":
                        sql = ("SELECT n,s,ROW_NUMBER() OVER (ORDER BY n ASC NULLS LAST,s ASC NULLS LAST) AS position "
                               f"FROM ({sql}) AS q ORDER BY position")
                        expected = [dict(row, position=index) for index, row in enumerate(ordered(expected), 1)]
                    else:
                        query = combine(query, left, "except_")
                        sql, bindings = query._relation_statement(), query._declared_sources()
                        expected = oracle(expected, LEFT, "except_")
                        replay = 1
                    query = SqlWorkflow(sql, harness.client, source_bindings=bindings)
                    assert a["opened"] == b["opened"] == 0
                    with query.iter_batches(batch_rows=2, **requested) as iterator:
                        actual = []
                        for batch in iterator:
                            assert iterator.report is None
                            actual.extend(batch.result_rows)
                        assert iterator.report is not None and iterator._process.poll() == 0
                        envelope = iterator.report.envelope
                    harness.values(name, actual, expected)
                    complete(harness, name, envelope, [a, b], [True, True], replay=replay)
                    clean()
                harness.case(name, composed)

            name = f"set-dynamic-{kind}-spill-{stored}"
            def dynamic(name=name, kind=kind, requested=requested):
                left, a = rich_frame(harness)
                right, b = rich_frame(harness)
                query = combine(pivot(left), pivot(right), kind)
                expected = oracle(PIVOT_VALUES, PIVOT_VALUES, kind)
                result = query.collect(check=True, **requested)
                harness.values(name, result.result_rows, expected)
                complete(harness, name, result.envelope, [a, b], [True, True])
                assert result.envelope.field("relational_schema_binding") == "during_execution"
                assert result.envelope.field_int("relational_dynamic_schema_stages") == 2
                clean()
            harness.case(name, dynamic)

    rich(harness)
    failures(harness, spill, clean)
    from native_streaming_set_boundary_cases import run as boundaries
    boundaries(harness, spill, clean)


def rich(harness):
    schema, fields, rows, expected = rich_fixture()
    python_expected = [{name: row[name] for name, _ in fields}
                       for row in rich_python_values()]
    left_indices, right_indices = [0, 3, 0, 2], [3, 2, 1, 1]
    for kind in KINDS:
        for destination in ("collect", "batches", "vortex"):
            name = f"set-rich-{kind}-{destination}"
            def check(name=name, kind=kind, destination=destination):
                left, a = rich_frame(harness, [rows[index] for index in left_indices], schema=schema)
                right, b = rich_frame(harness, [rows[index] for index in right_indices], schema=schema)
                query = combine(left, right, kind)
                wanted = oracle([expected[index] for index in left_indices], [expected[index] for index in right_indices], kind)
                objects = oracle([python_expected[index] for index in left_indices], [python_expected[index] for index in right_indices], kind)
                if destination == "collect":
                    result = query.collect(check=True, **POLICY)
                    actual, actual_objects = result.result_rows, result.python_objects
                    assert result.result_schema == fields
                    envelope = result.envelope
                elif destination == "batches":
                    actual, actual_objects = [], []
                    with query.iter_batches(batch_rows=1, **POLICY) as iterator:
                        for batch in iterator:
                            assert batch.result_schema == fields and iterator.report is None
                            actual.extend(batch.result_rows)
                            actual_objects.extend(batch.python_objects)
                        assert iterator.report is not None and iterator._process.poll() == 0
                        assert iterator._schema == fields
                        envelope = iterator.report.envelope
                else:
                    path = harness.output / (name + ".vortex")
                    report = query.write_vortex(path, **POLICY)
                    envelope = report.envelope
                    reopened = harness.context.read_vortex(path).collect(check=True, **POLICY)
                    harness.envelope(name + "-reopen", reopened.envelope)
                    assert reopened.result_schema == fields
                    actual, actual_objects = reopened.result_rows, reopened.python_objects
                harness.values(name, actual, wanted)
                assert_python_values(actual_objects, objects)
                complete(harness, name, envelope, [a, b], [True, True])
            harness.case(name, check)


def failures(harness, spill, clean):
    for kind in KINDS:
        for side in (0, 1):
            for destination in ("zero", "collect", "write"):
                name = f"set-late-{kind}-side-{side}-{destination}"
                def failed(name=name, kind=kind, side=side, destination=destination):
                    trace = {"opened": 0, "failed": False, "closed": False}
                    def broken():
                        trace["opened"] += 1
                        try:
                            yield LEFT[:2]
                            trace["failed"] = True
                            raise RuntimeError(name + " sentinel")
                        finally:
                            trace["closed"] = True
                    source = harness.context.from_batches(broken, schema={"n": "int64", "s": "utf8"}, streaming=True)
                    other, _ = batch_frame(harness, RIGHT)
                    left, right = (source, other) if side == 0 else (other, source)
                    query = combine(left, right, kind)
                    target = harness.output / (name + ".vortex")
                    before = set(harness.output.iterdir())
                    try:
                        if destination == "write":
                            query.write_vortex(target, spill=spill, **POLICY)
                        elif destination == "collect":
                            query.collect(check=True, spill=spill, **POLICY)
                        else:
                            with query.limit(0).iter_batches(spill=spill, **POLICY) as iterator:
                                assert list(iterator) == []
                    except RuntimeError as error:
                        assert type(error) is RuntimeError and str(error) == name + " sentinel"
                    else:
                        raise AssertionError("set operation hid a late producer failure")
                    assert trace == {"opened": 1, "failed": True, "closed": True}
                    assert not target.exists() and set(harness.output.iterdir()) == before
                    clean()
                    return {"original_exception_preserved": True, "no_final_publication": True}
                harness.case(name, failed)
