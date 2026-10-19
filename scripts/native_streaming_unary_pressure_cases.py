# SPDX-License-Identifier: Apache-2.0
"""Bounded unary state, native membership/population spill and source replay."""

from __future__ import annotations

import hashlib
import json

from shardloom.errors import ShardLoomCommandError
from native_streaming_unary_cases import KINDS, apply, fixture
from native_streaming_set_cases import complete
from native_streaming_set_pressure_cases import capture
from native_streaming_order_pressure_cases import encoded
from native_unary_reference import sample_ordinals
from generate_native_sampling_population_reference import fixture as sampling_reference

ROWS, BATCH_ROWS, WIDTH, GRANT = 8193, 251, 2048, 16 << 20
POLICY = {"memory_bytes": GRANT, "max_parallelism": 1}


def row(index, *, unique=False, width=WIDTH, nested=False, numbers=False):
    key = index if unique else index % 3
    result = {"n": key}
    if not numbers:
        result["s"] = f"{key:08d}" + "u" * (width - 8) if width else ""
    if nested:
        result["items"] = [key, None]
    return result


def expected(kind):
    if kind in ("distinct", "dedup-first"):
        yield from (row(index) for index in range(3))
    elif kind in ("tail", "dedup-last"):
        yield from (row(index) for index in range(ROWS - 3, ROWS))
    elif kind == "dedup-false":
        return
    elif kind == "sample":
        yield from (row(index) for index in sample_ordinals(ROWS, 3, 7))
    elif kind.startswith("mask-"):
        keep = kind.split("-", 1)[1]
        for index in range(ROWS):
            yield {"duplicated": index >= 3 if keep == "first" else index + 3 < ROWS if keep == "last" else True}
    elif kind == "expression":
        for index in range(ROWS):
            yield dict(row(index), index=index)
    elif kind == "melt":
        for index in range(ROWS):
            value = row(index)
            yield {"s": value["s"], "variable": "n", "value": value["n"]}
    elif kind == "rolling":
        for index in range(ROWS):
            yield {"total": float(sum(value % 3 for value in range(max(0, index - 1), min(ROWS, index + 2))))}
    else:
        assert kind == "explode"
        for index in range(ROWS):
            for item in (index % 3, None):
                yield dict(row(index), items=item)


def source(harness, kind="distinct", *, unique=False, count=ROWS, width=WIDTH,
           numbers=False, workspace=None, failure=None):
    schema = {"n": "int64"} if numbers else fixture(kind)[1]
    trace = {"opened": 0, "batches": 0, "rows": 0, "ended": False, "closed": False, "fault": False}

    def produce():
        trace["opened"] += 1
        try:
            for start in range(0, count, BATCH_ROWS):
                if trace["batches"] == 6 and failure in ("producer", "corrupt"):
                    runs = sorted(workspace.glob("shardloom-query-*/*.vortex"))
                    assert runs, "failure must follow actual unary source replay spill"
                    trace["fault"] = True
                    if failure == "producer":
                        raise RuntimeError("unary replay producer sentinel")
                    damaged = runs[0]
                    assert not damaged.is_symlink() and damaged.parent.parent == workspace
                    with damaged.open("r+b") as stream:
                        stream.truncate(1)
                batch = [row(index, unique=unique, width=width, nested=kind == "explode", numbers=numbers)
                         for index in range(start, min(start + BATCH_ROWS, count))]
                trace["batches"] += 1
                trace["rows"] += len(batch)
                yield batch
            trace["ended"] = True
        finally:
            trace["closed"] = True
    return harness.context.from_batches(produce, schema=schema, streaming=True), trace


def denied(harness, name, query, trace, requested, message="reservation denied"):
    iterator = query.iter_batches(batch_rows=257, **requested)
    try:
        with iterator:
            for _ in iterator:
                assert iterator.report is None
    except ShardLoomCommandError as error:
        harness.envelope(name, error.envelope, success=False)
        assert message in json.dumps(error.envelope.raw), error.envelope.raw
    else:
        raise AssertionError("unary state exceeded its admitted grant without refusal")
    assert iterator.report is None and iterator._process.poll() is not None
    assert trace["opened"] == 1 and trace["closed"] and not trace["ended"]


def run(harness):
    workspace = harness.output / "unary-pressure-spill"
    workspace.mkdir()
    foreign = workspace / "foreign"
    foreign.write_bytes(b"preserve")
    spill = {"workspace": str(workspace), "quota_bytes": 128 << 20, "buffer_bytes": 1 << 20}

    def clean():
        assert foreign.read_bytes() == b"preserve" and list(workspace.iterdir()) == [foreign]

    oracles = {}
    for kind in KINDS:
        digest, count = hashlib.sha256(), 0
        for value in expected(kind):
            digest.update(encoded(value))
            count += 1
        oracles[kind] = {"rows": count, "sha256": digest.hexdigest()}
    harness.json(harness.logs / "unary-pressure-predeclared.json", {
        "input_rows": ROWS, "batch_rows": BATCH_ROWS, "width": WIDTH, "grant": GRANT,
        "low_cardinality_key": "row_index % 3", "unique_key": "row_index",
        "oracles": oracles, "spill": spill, "unary_membership_spill": True,
        "sampling_population_spill": True,
        "sampling_reference": sampling_reference(),
        "source_replay_is_separate": True,
    })
    for kind in KINDS:
        name = "unary-bounded-state-" + kind

        def bounded(name=name, kind=kind):
            batch, trace = source(harness, kind)
            query = apply(batch, kind)
            envelope, record = capture(harness, name, query.iter_batches(batch_rows=257, **POLICY),
                                       lambda: expected(kind))
            complete(harness, name, envelope, [trace], [True], grant=GRANT)
            assert envelope.field_int("native_input_logical_bytes") > GRANT
            assert record["complete_rows"] == oracles[kind]["rows"]
            assert record["expected_sha256"] == oracles[kind]["sha256"]
            clean()
            return dict(record, unary_state_spill=False)
        harness.case(name, bounded)

    stateful = [kind for kind in KINDS if kind == "distinct" or kind.startswith(("dedup-", "mask-"))]
    for kind in stateful:
        for stored in (False, True):
            name = f"unary-membership-spill-{kind}" if stored else f"unary-resident-state-denied-{kind}-spill-{stored}"

            def membership(name=name, kind=kind, stored=stored):
                batch, trace = source(harness, unique=True)
                if kind.startswith(("dedup-", "mask-")):
                    keep = kind.split("-", 1)[1]
                    method = batch.drop_duplicates if kind.startswith("dedup-") else batch.duplicated
                    query = method("s", keep=False if keep == "false" else keep)
                else:
                    query = batch.distinct()
                if not stored:
                    denied(harness, name, query, trace, POLICY)
                    assert BATCH_ROWS < trace["rows"] <= ROWS
                    clean()
                    return {"producer_trace": trace, "unary_membership_spill": False}

                def reference():
                    for index in range(ROWS):
                        if kind.startswith("mask-"):
                            yield {"duplicated": False}
                        else:
                            yield row(index, unique=True)

                envelope, record = capture(
                    harness, name,
                    query.iter_batches(batch_rows=257, spill=spill, **POLICY), reference)
                complete(harness, name, envelope, [trace], [True], grant=GRANT)
                assert record["complete_rows"] == ROWS
                assert envelope.field_int("relational_spillable_unary_membership_stages") == 1
                for suffix in ("input_rows", "observed_rows", "groups", "output_rows"):
                    assert envelope.field_int(f"relational_spillable_unary_{suffix}") == ROWS
                identities = ROWS if kind in ("mask-last", "mask-false") else 0
                assert envelope.field_int("relational_spillable_unary_identity_rows") == identities
                assert envelope.field_int("relational_spill_runs_written") > 0
                assert envelope.field_bool("relational_spill_owned_cleanup_completed") is True
                assert envelope.field_int("relational_spill_peak_disk_bytes") <= spill["quota_bytes"]
                assert envelope.field_int("native_input_replay_spilled_sources") == 0
                clean()
                return dict(record, unary_membership_spill=True)
            harness.case(name, membership)

    for case in sampling_reference()["cases"]:
        for stored in (False, True):
            name = f"unary-sampling-population-{case['name']}-spill-{stored}"

            def population(name=name, case=case, stored=stored):
                batch, trace = source(harness, unique=True)
                if case["weighted"]:
                    batch = batch.filter("n > 0")
                query = batch.sample(
                    n=case["count"], frac=case["fraction"], random_state=case["seed"],
                    replace=case["replace"], weights="n" if case["weighted"] else None)
                if not stored:
                    denied(harness, name, query, trace, POLICY)
                    assert BATCH_ROWS < trace["rows"] <= ROWS
                    clean()
                    return {"producer_trace": trace, "sampling_population_spill": False}

                def reference():
                    yield from (row(index, unique=True) for index in case["expected"])

                envelope, record = capture(
                    harness, name, query.iter_batches(batch_rows=257, spill=spill, **POLICY), reference)
                complete(harness, name, envelope, [trace], [True], grant=GRANT)
                assert record["complete_rows"] == len(case["expected"])
                assert envelope.field_int("relational_spillable_sampling_stages") == 1
                # A public filter can run before the sample stage; the shared
                # producer's complete input row count is verified by complete().
                assert envelope.field_int("relational_spillable_sampling_input_rows") == case["population"]
                assert envelope.field_int("relational_spillable_sampling_population_rows") == case["population"]
                assert envelope.field_int("relational_spillable_sampling_output_rows") == len(case["expected"])
                draws = len(case["expected"]) if case["replace"] else 0
                assert envelope.field_int("relational_spillable_sampling_replacement_draws") == draws
                assert (envelope.field_int("relational_spillable_sampling_lookup_blocks") > 0) == case["replace"]
                assert envelope.field_int("relational_spill_runs_written") > 0
                assert envelope.field_bool("relational_spill_owned_cleanup_completed") is True
                assert envelope.field_int("relational_spill_peak_disk_bytes") <= spill["quota_bytes"]
                assert envelope.field_int("native_input_replay_spilled_sources") == 0
                clean()
                return dict(record, sampling_population_spill=True)
            harness.case(name, population)

    future_masks(harness, spill, clean)
    replay(harness, workspace, spill, clean)


def future_masks(harness, spill, clean):
    count = 262_145
    for keep in ("first", "last", False):
        for grant in ((8 << 20,) if keep == "first" else (8 << 20, 32 << 20)):
            for stored in (False, True):
                name = f"unary-future-mask-{keep}-grant-{grant}-spill-{stored}"

                def check(name=name, keep=keep, grant=grant, stored=stored):
                    batch, trace = source(harness, count=count, width=0, numbers=True)
                    query = batch.duplicated("n", keep=keep)
                    requested = dict(POLICY, memory_bytes=grant)
                    if stored:
                        requested["spill"] = spill
                    if keep != "first" and grant == 8 << 20 and not stored:
                        denied(harness, name, query, trace, requested, message="requested=8388608")
                        assert trace["rows"] == count
                        record = {"producer_trace": trace, "denied_population_identity_bytes": 8 << 20}
                    else:
                        def reference():
                            for index in range(count):
                                yield {"duplicated": index >= 3 if keep == "first" else index + 3 < count if keep == "last" else True}
                        envelope, record = capture(harness, name, query.iter_batches(batch_rows=257, **requested), reference)
                        complete(harness, name, envelope, [trace], [True], grant=grant)
                        assert record["complete_rows"] == count
                        if stored:
                            assert envelope.field_int("relational_spillable_unary_membership_stages") == 1
                            for suffix in ("input_rows", "observed_rows"):
                                assert envelope.field_int(f"relational_spillable_unary_{suffix}") == count
                            assert envelope.field_int("relational_spillable_unary_groups") == 3
                            if keep != "first":
                                assert envelope.field_int("relational_spillable_unary_identity_rows") == count
                                assert envelope.field_int("relational_spill_runs_written") > 0
                            assert envelope.field_int("native_input_replay_spilled_sources") == 0
                            assert envelope.field_bool("relational_spill_owned_cleanup_completed") is True
                            assert envelope.field_int("relational_spill_peak_disk_bytes") <= spill["quota_bytes"]
                    clean()
                    return dict(record, unary_membership_spill=stored)
                harness.case(name, check)


def replay(harness, workspace, spill, clean):
    def reference():
        for _ in range(2):
            yield from (row(index, unique=True) for index in range(ROWS - 3, ROWS))

    name = "unary-source-replay-resident-denied"

    def refuse():
        batch, trace = source(harness, unique=True)
        stage = batch.tail(3)
        denied(harness, name, stage.union_all(stage), trace, POLICY)
        clean()
    harness.case(name, refuse)

    for mode in ("ample", "spill-first", "spill-clean-reuse"):
        name = "unary-source-replay-" + mode

        def check(name=name, mode=mode):
            batch, trace = source(harness, unique=True)
            stage = batch.tail(3)
            requested = dict(POLICY, memory_bytes=128 << 20) if mode == "ample" else dict(POLICY, spill=spill)
            envelope, record = capture(harness, name, stage.union_all(stage).iter_batches(batch_rows=257, **requested), reference)
            complete(harness, name, envelope, [trace], [True], replay=1, grant=requested["memory_bytes"])
            assert envelope.field_int("native_input_replay_rows") == ROWS
            assert envelope.field_int("native_input_replay_rows_detached") == ROWS
            assert envelope.field_int("native_input_replay_buffer_bytes_copied") > GRANT
            assert envelope.field_int("native_input_replay_spilled_sources") == int(mode != "ample")
            if mode != "ample":
                assert envelope.field_int("relational_spill_runs_written") > 3
                assert envelope.field_int("relational_spill_merge_passes") > 1
                assert envelope.field_bool("relational_spill_owned_cleanup_completed") is True
                assert envelope.field_int("relational_spill_peak_disk_bytes") <= spill["quota_bytes"]
            clean()
            return dict(record, unary_state_spill=False)
        harness.case(name, check)

    for failure in ("producer", "corrupt", "quota"):
        for write in (False, True):
            name = f"unary-source-replay-{failure}-write-{write}"

            def fail(name=name, failure=failure, write=write):
                batch, trace = source(harness, unique=True, workspace=workspace, failure=failure)
                stage = batch.tail(3)
                query = stage.union_all(stage)
                selected = dict(spill, quota_bytes=2 << 20) if failure == "quota" else spill
                before, iterator = set(harness.output.iterdir()), None
                try:
                    if write:
                        query.write_vortex(harness.output / (name + ".vortex"), spill=selected, **POLICY)
                    else:
                        iterator = query.iter_batches(batch_rows=257, spill=selected, **POLICY)
                        with iterator:
                            for _ in iterator:
                                assert iterator.report is None
                except (RuntimeError, ShardLoomCommandError) as error:
                    assert trace["fault"] or failure == "quota"
                    if failure == "producer":
                        assert type(error) is RuntimeError and str(error) == "unary replay producer sentinel"
                    else:
                        assert isinstance(error, ShardLoomCommandError)
                        harness.envelope(name, error.envelope, success=False)
                        assert ("quota" if failure == "quota" else "byte length changed") in json.dumps(error.envelope.raw)
                else:
                    raise AssertionError("incomplete unary replay returned success")
                assert trace["closed"] and set(harness.output.iterdir()) == before
                if iterator is not None:
                    assert iterator.report is None and iterator._process.poll() is not None
                clean()
            harness.case(name, fail)
