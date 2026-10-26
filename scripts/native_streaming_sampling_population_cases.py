# SPDX-License-Identifier: Apache-2.0
"""Public sampling population semantics, native spill, outputs and failures."""

from __future__ import annotations

import json
from pathlib import Path

from shardloom._result_schema import ResultType as T
from shardloom.errors import ShardLoomCommandError
from shardloom.query import LazyFrame

from generate_native_sampling_population_reference import fixture as population_reference
from native_streaming_set_cases import complete
from native_streaming_unary_cases import consume, frame, payload
from native_streaming_unary_pressure_cases import row, source
from native_unary_reference import sample_ordinals
from native_workflow_outputs import LOCAL_FORMATS, verify_output

POLICY = {"memory_bytes": 32 << 20, "max_parallelism": 1}
FIELDS = (("n", T("int64", True)), ("s", T("utf8", True)))


def sample(source, options, surface="dataframe"):
    if surface == "dataframe":
        return source.sample(**options)
    assert surface == "sql"
    renamed = {"random_state": "seed", "frac": "fraction"}
    options = {renamed.get(key, key): value for key, value in options.items() if value is not None}
    return LazyFrame.from_sql(
        f"SELECT * FROM SAMPLE(({source._relation_statement()}), {payload(options)}) AS u",
        source.client, source_bindings=source._declared_sources(),
    )


def population(source, case, surface="dataframe"):
    if case["weighted"]:
        source = source.filter("n > 0")
    options = {"random_state": case["seed"], "replace": case["replace"]}
    options["frac" if case["fraction"] is not None else "n"] = (
        case["fraction"] if case["fraction"] is not None else case["count"])
    if case["weighted"]:
        options["weights"] = "n"
    return sample(source, options, surface)


def proof(envelope, case, *, runs, stages=1):
    count = len(case["expected"])
    assert envelope.field_int("relational_spillable_sampling_stages") == stages
    for suffix in ("input_rows", "population_rows"):
        assert envelope.field_int(f"relational_spillable_sampling_{suffix}") == stages * case["population"]
    assert envelope.field_int("relational_spillable_sampling_output_rows") == stages * count
    assert envelope.field_int("relational_spillable_sampling_replacement_draws") == (
        stages * count if case["replace"] else 0)
    assert (envelope.field_int("relational_spillable_sampling_lookup_blocks") > 0) == (
        case["replace"] and count > 0)
    assert (envelope.field_int("relational_spill_runs_written") > 0) == runs
    assert envelope.field_bool("relational_spill_owned_cleanup_completed") is True
    assert envelope.field_int("relational_spill_peak_disk_bytes") <= 128 << 20


def run(harness):
    workspace = harness.output / "sampling-population-spill"
    workspace.mkdir()
    foreign = workspace / "foreign"
    foreign.write_bytes(b"preserve")
    spill = {"workspace": str(workspace), "quota_bytes": 128 << 20, "buffer_bytes": 1 << 20}
    requested = dict(POLICY, spill=spill)

    def clean():
        assert foreign.read_bytes() == b"preserve"
        assert list(workspace.iterdir()) == [foreign]

    reference = json.loads(Path(__file__).with_name("native_streaming_unary_reference_fixtures.json").read_text())
    assert len(reference["sampling_cases"]) == 54
    # Compare stored expected values to a separate full-ranking/linear-draw
    # oracle before any engine invocation. No runtime sampling helper is used.
    for case in reference["sampling_cases"]:
        rows = case.get("rows", reference["sampling_rows"])
        options = case["options"]
        selected = sample_ordinals(
            len(rows), options.get("n", case.get("declared_target")), options["random_state"],
            weights=[row["weight"] for row in rows] if options.get("weights") else None,
            replace=options.get("replace", False),
        )
        assert selected == case["ordinals"] and [rows[index] for index in selected] == case["expected"]
    populations = population_reference()
    harness.json(harness.logs / "sampling-population-predeclared.json", {
        "small": reference, "populations": populations, "spill": spill,
        "grant_bytes": POLICY["memory_bytes"], "formats": LOCAL_FORMATS,
        "source_replay_is_separate": True,
    })

    schema = {"id": "int64", "value": "int64", "weight": "float64"}
    fields = tuple((name, T(dtype, True)) for name, dtype in schema.items())
    for ordinal, case in enumerate(reference["sampling_cases"]):
        for surface in ("dataframe", "sql"):
            for terminal in ("collect", "batches"):
                name = f"sampling-population-{case['name']}-{surface}-{terminal}"

                def small(name=name, ordinal=ordinal, case=case, surface=surface, terminal=terminal):
                    rows = case.get("rows", reference["sampling_rows"])
                    batch, trace = frame(harness, rows, schema, width=(1, 3, 8)[ordinal % 3])
                    envelope = consume(harness, name, sample(batch, case["options"], surface),
                                       case["expected"], fields, [trace], terminal, policy=requested)
                    options = case["options"]
                    admitted = options.get("replace", False) or options.get("frac") is not None
                    if admitted:
                        proof(envelope, {"population": len(rows), "expected": case["expected"],
                                         "replace": options.get("replace", False)}, runs=False)
                    else:
                        assert envelope.field_int("relational_spillable_sampling_stages") == 0
                    clean()
                    return {"complete_rows": len(case["expected"]), "population_strategy": admitted}

                harness.case(name, small)

    wide = populations["failure_cases"]
    for case in wide:
        for terminal in ("collect", "batches"):
            name = f"sampling-population-wide-{case['name']}-{terminal}"

            def held(name=name, case=case, terminal=terminal):
                rows = [row(index, unique=True, width=case["width"]) for index in range(case["rows"])]
                batch, trace = frame(harness, rows, {"n": "int64", "s": "utf8"}, width=13)
                expected = [rows[index] for index in case["expected"]]
                envelope = consume(harness, name, population(batch, case), expected, FIELDS, [trace],
                                   terminal, policy=requested)
                proof(envelope, case, runs=True)
                clean()
                return {"complete_rows": len(expected), "retained_outputs": True}

            harness.case(name, held)

    destinations(harness, wide, requested, clean)
    failures(harness, populations["cases"], workspace, spill, clean)
    repeated_and_early_close(harness, wide, requested, clean)


def destinations(harness, cases, requested, clean):
    def accepted(name, report):
        harness.envelope(name, report.envelope)
        return report.envelope

    def values(name, actual, expected, _path):
        harness.values(name, actual, expected)

    for case in cases:
        for destination in (*LOCAL_FORMATS, "fanout"):
            name = f"sampling-population-{case['name']}-write-{destination}"

            def write(name=name, case=case, destination=destination):
                batch, trace = source(harness, unique=True, count=case["rows"], width=case["width"])
                query = population(batch, case)
                expected = [row(index, unique=True, width=case["width"]) for index in case["expected"]]
                formats = LOCAL_FORMATS if destination == "fanout" else (destination,)
                targets = {extension: harness.output / f"{name}.{extension}" for extension in formats}
                report = (query.fanout(targets, **requested) if destination == "fanout" else
                          getattr(query, "write_" + destination)(targets[destination], **requested))
                envelope = report.envelope
                complete(harness, name, envelope, [trace], [True], grant=POLICY["memory_bytes"])
                proof(envelope, case, runs=True)
                if destination == "fanout":
                    assert envelope.field_int("native_vortex_result_export_execution_count") == 1
                    assert envelope.field_int("native_vortex_result_export_target_count") == len(formats)
                    assert envelope.field_int("native_vortex_result_export_replay_count") == len(formats)
                for extension, path in targets.items():
                    verify_output(harness.context, path, extension, expected, ["n", "s"],
                                  name=name + "-" + extension, accepted=accepted, complete=values)
                clean()
                return {"artifacts": [harness.artifact(path) for path in targets.values()],
                        "complete_rows_per_output": len(expected), "producer_executions": trace["opened"]}

            harness.case(name, write)


def failures(harness, cases, workspace, spill, clean):
    for case in cases:
        for failure in ("producer", "corrupt", "quota"):
            for terminal in ("batches", "vortex", "fanout"):
                for existing in ((False, True) if failure == "producer" and terminal != "batches" else (False,)):
                    name = f"sampling-population-{case['name']}-{failure}-{terminal}-existing-{existing}"

                    def fail(name=name, case=case, failure=failure, terminal=terminal, existing=existing):
                        batch, trace = source(harness, unique=True, workspace=workspace, failure=failure)
                        query = population(batch, case)
                        selected = dict(spill, quota_bytes=2 << 20) if failure == "quota" else spill
                        targets = {extension: harness.output / f"{name}.{extension}" for extension in
                                   (("vortex", "jsonl") if terminal == "fanout" else ("vortex",))}
                        if existing:
                            for path in targets.values():
                                path.write_bytes(b"preserve existing sample target")
                        before, iterator = set(harness.output.iterdir()), None
                        try:
                            if terminal == "batches":
                                iterator = query.iter_batches(batch_rows=257, spill=selected, **POLICY)
                                with iterator:
                                    for _ in iterator:
                                        assert iterator.report is None
                            elif terminal == "vortex":
                                query.write_vortex(targets["vortex"], spill=selected,
                                                   allow_overwrite=existing, **POLICY)
                            else:
                                query.fanout(targets, spill=selected, allow_overwrite=existing, **POLICY)
                        except (RuntimeError, ShardLoomCommandError) as error:
                            if existing:
                                assert isinstance(error, ShardLoomCommandError)
                                harness.envelope(name, error.envelope, success=False)
                                assert "existing destination" in json.dumps(error.envelope.raw)
                            elif failure == "producer":
                                assert trace["fault"]
                                assert type(error) is RuntimeError and str(error) == "unary replay producer sentinel"
                            else:
                                assert trace["fault"] or failure == "quota"
                                assert isinstance(error, ShardLoomCommandError)
                                harness.envelope(name, error.envelope, success=False)
                                message = json.dumps(error.envelope.raw)
                                assert ("quota" in message if failure == "quota" else
                                        "byte length changed" in message or "prepared source changed" in message)
                        else:
                            raise AssertionError("failed sampling population returned a completion report")
                        assert set(harness.output.iterdir()) == before
                        for path in targets.values():
                            if existing:
                                assert path.read_bytes() == b"preserve existing sample target"
                            else:
                                assert not path.exists()
                        if existing:
                            assert trace["opened"] == 0 and not trace["fault"]
                            assert not trace["ended"] and not trace["closed"]
                        else:
                            assert trace["opened"] == 1 and trace["closed"] and not trace["ended"]
                        if iterator is not None:
                            assert iterator.report is None and iterator._process.poll() is not None
                        clean()
                        # A complete fresh sample reuses the same workspace/client.
                        batch, recovered = frame(harness, [{"n": 1, "s": "fresh"}],
                                                 {"n": "int64", "s": "utf8"})
                        report = population(batch, case).collect(check=True, spill=spill, **POLICY)
                        expected_count = case["count"] if case["count"] is not None else 1
                        harness.values(name + "-recovery", report.result_rows,
                                       [{"n": 1, "s": "fresh"}] * expected_count)
                        complete(harness, name + "-recovery", report.envelope, [recovered], [True],
                                 grant=POLICY["memory_bytes"])
                        clean()
                        return {"failure": failure, "terminal": terminal, "existing_preserved": existing,
                                "failure_phase": "destination_admission" if existing else "execution",
                                "complete_recovery_rows": expected_count, "failed_trace": trace}

                    harness.case(name, fail)


def repeated_and_early_close(harness, cases, requested, clean):
    for case in cases:
        name = "sampling-population-early-close-" + case["name"]

        def close(name=name, case=case):
            batch, trace = source(harness, unique=True, count=case["rows"], width=case["width"])
            with population(batch, case).iter_batches(batch_rows=1, **requested) as iterator:
                retained = next(iterator)
                assert iterator.report is None
            assert iterator.report is None and iterator._process.poll() is not None
            assert trace["opened"] == 1 and trace["ended"] and trace["closed"]
            harness.values(name, retained.result_rows,
                           [row(case["expected"][0], unique=True, width=case["width"])])
            clean()
            return {"completion_report": None, "source_completed_before_output": True}

        harness.case(name, close)
        # Replacement always uses population access even when source replay
        # reveals an upper bound. Fractional nonreplacement retains its top-k
        # route if that bound is known and is covered by the existing controls.
        if not case["replace"]:
            continue
        name = "sampling-population-source-replay-" + case["name"]

        def repeated(name=name, case=case):
            batch, trace = source(harness, unique=True, count=case["rows"], width=case["width"])
            query = population(batch, case)
            expected = [row(index, unique=True, width=case["width"]) for index in case["expected"]]
            envelope = consume(harness, name, query.union_all(query), expected * 2, FIELDS, [trace],
                               replay=1, policy=requested)
            proof(envelope, case, runs=True, stages=2)
            assert envelope.field_int("native_input_replay_spilled_sources") == 1
            assert envelope.field_int("native_input_replay_rows") == case["rows"]
            clean()
            return {"complete_rows": len(expected) * 2, "producer_executions": trace["opened"],
                    "source_replay_separate_from_population": True}

        harness.case(name, repeated)
