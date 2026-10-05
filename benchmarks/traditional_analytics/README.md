# Traditional Analytics Benchmark Harness

Status: interface and methodology are documented; live harness acceptance and any performance
claim remain pending. This harness has one ShardLoom candidate, `shardloom`, executed through the
public native workflow. Input state and output format are parameters to that candidate, not separate
engine identities or runtime routes.

## Run

Use a built ShardLoom executable, a local-only workspace outside synced folders, and an explicit
workload selection for a small check:

```sh
python3 benchmarks/traditional_analytics/run.py \
  --shardloom-binary /absolute/path/to/shardloom \
  --workspace "$HOME/LocalData/shardloom/traditional-benchmarks" \
  --input-state raw \
  --output-format collect \
  --formats csv \
  --scenarios "selective filter" \
  --reference-engine pandas \
  --repeats 3
```

To inspect supported declarations without running engines:

```sh
python3 benchmarks/traditional_analytics/run.py --help
python3 benchmarks/traditional_analytics/run.py --list
```

`--list` is the source of truth for the currently accepted workload names and complete SQL
declarations. `--formats` accepts `csv`, `json`, `jsonl`, `vortex`, `parquet`, `arrow-ipc`,
`avro`, and `orc`.
`--input-state` accepts `raw` or `prepared`. With `prepared`, explicit Vortex input preparation
occurs before query timing. With `raw`, the engine admits the original input and performs or
validates its native preparation within the timed request; native prepared-artifact reuse remains
visible in the execution evidence. Raw input does not imply a cold filesystem cache.
For native Vortex format cases, public `vortex-prepare` creates the fixture from the original
declared CSV before input hashes are frozen and before query timing. Each preparation receipt and
committed output hash is retained. Prepared mode reuses these existing Vortex files and records
their identities; it does not rewrite them. Other source roles, such as the CDC delta CSV, still
receive the preparation their own format requires.
`--output-format` accepts `collect`, `vortex`, `json`, `jsonl`, `csv`, `parquet`,
`arrow_ipc`, `avro`, or `orc`. A non-collect output is written to the fresh run workspace and
read back in full for result validation. JSON and JSONL are decoded directly from the committed
file, including empty outputs that carry no schema. The other formats are reopened through the
public workflow. Both paths retain output hashes and compare complete values with the independent
reference; neither executes workload logic in the validation reader.

The harness also accepts `--rows`, `--dim-rows`, `--dataset-profile`, `--engines`,
`--memory-gb`, `--max-parallelism`, and `--timeout`; their accepted values and defaults are shown
by `--help`. `--engines` selects independent comparison adapters alongside the required ShardLoom
candidate. `--reference-engine` selects the comparison result source, which is automatically
included in the run.

## Workload And Comparison Contract

Each workload in [`workloads.py`](workloads.py) declares its complete SQL statement or statements,
source roles, and result shape. The harness binds generated input paths into those declarations and
sends each complete statement through the public workflow protocol. The ShardLoom adapter does not
select private benchmark commands or substitute a workload-specific execution path.

Comparison engines run independently in isolated worker processes. Their results are correctness
references only: a rejected ShardLoom declaration remains unsupported and is never delegated to a
baseline. The harness records engine versions and compares complete normalized values. Query-answer
caching is disabled for the ShardLoom adapter.

The candidate's `format` is a matrix dimension. `comparison_input_formats` and each comparison
row's `comparison_input_format` identify what the independent process actually reads. JSON arrays
are read through declared Arrow schemas in the comparison adapters. For candidate Vortex cases,
comparison engines read the original CSV fixture; they do not claim a Vortex reader. The retained
worker job, process report and source inventory must all agree on that format. The verifier also
rebuilds the workload's complete SQL declarations and source bindings from those frozen inputs;
substituting a different query or input is rejected. This explicitly
compares complete query results across equivalent logical data, with different input costs; it
cannot establish a same-format timing comparison. Expected query values never come from the native
fixture preparation or the candidate's query output.

The comparison contract rounds floating metric cells to four decimal places and treats an empty
metric sum as zero, matching the original comparison adapters. It compares all rows and keys after
that normalization; the retained native envelopes still contain the engine's original typed
values. This scoped benchmark comparison does not replace the exact type and NULL checks in the
native correctness suite.

## Run Integrity And Timing

Each run uses a fresh timestamped workspace, lock, logs, input fixture directory, and report. The
harness refuses an existing run directory and opens reports and receipts without overwrite. It
records hashes for the executable, harness source files, and inputs, plus complete reference values
in its report and worker outputs. It checks that the executable and input contents remain unchanged
during the run. Resource admission
requires a local POSIX workspace, checks free-space/workspace/log budgets, prevents overlapping
native workloads, and samples process-tree RSS and host load.

Prepared-input creation is outside query timing. Native query timing covers the public workflow
request through process exit, including a requested sink write. Output readback validation is
recorded separately and included in actual run wall time. Fixture generation, source hashing, and
baseline work are also distinguished from candidate query timing. These boundaries are evidence
metadata, not a performance claim.

The report is complete only when every expected engine/format/scenario/repeat record exists and has
status `passed`. A native diagnostic, unsupported baseline, failed process, or value mismatch makes
the report incomplete; a run-level guard or integrity error makes it failed. Both outcomes return a
nonzero process status. Only a complete correctness-matching report may be considered for later
timing review, and this harness has not yet passed that live acceptance review.

## Acceptance Before Any Claim

Before relying on measurements, validate at least one small raw-input run, one prepared-input run,
and a requested-output run against an independent reference. Review the full report and receipts for
source and executable hashes, complete result values, timing boundaries, resource samples,
no-fallback evidence, and all expected cases. Confirm that unsupported or failed cases produce an
incomplete/failed report and a nonzero exit. Record hardware, operating system, runtime and baseline
versions, dataset shape, formats, cold/warm cache context, and all relevant configuration. No
performance, production-readiness, or engine-superiority conclusion follows from `--list`, a fixture,
or an unreviewed run.
