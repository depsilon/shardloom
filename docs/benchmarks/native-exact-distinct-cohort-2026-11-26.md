# Native exact DISTINCT affected-owner cohort

Status: prospective protocol, with a separate 100-row API preflight. No 5M-row
timings are supplied here. This is a newly declared cohort for the dictionary
visitor, scalar validity-count, and exact parallel DISTINCT changes; it does not
reconstruct the unpublished four-query comparison of #1722 and #1723.

The executable protocol is
[`scripts/native_exact_distinct_cohort.py`](../../scripts/native_exact_distinct_cohort.py).
Its `prepare` command freezes all SQL, references, source bytes, driver bytes,
Python client source, dependency versions, binary manifests, resource settings,
and ordering into `protocol.json` **before any cohort query executes**. `run`
consumes that protocol once. A failed or incomplete run is retained and cannot
receive a complete-cohort score.

## Declared scope

Run these eight cases against both Parquet and matching native Vortex inputs:
16 cases, two binary roles, one warmup and three measured observations per role
and case, for 128 complete result checks. The code contains the exact SQL.

| Case | Operation | Owner exposure / control |
| --- | --- | --- |
| Q01 | Inner join on `fact.grp = dim.key`, grouped by `region`, `COUNT(*)`, `SUM(value)` | Join and integer reduction control |
| Q02 | 50K groups, `COUNT(*)`, `SUM(value)` | Existing grouped integer reduction control |
| Q03 | `COUNT(value)` where `flag = true` | Filtered selection and source validity intersection |
| Q04 | Scalar `COUNT(value)` | Direct validity cardinality |
| Q05 | `SELECT DISTINCT low_tag` | Dictionary codes and exact text identity |
| Q06 | Scalar `COUNT(DISTINCT high_tag)` | Shared exact distinct state on Parquet; existing specialized scalar-provider control on Vortex |
| Q07 | 50K groups, `COUNT(DISTINCT low_tag)`, `COUNT(*)`, `SUM(value)` | Shared group remapping and exact distinct merge on Parquet; existing grouped-provider control on Vortex |
| Q08 | Scalar `SUM(float_value)`, `COUNT(DISTINCT high_tag)` | Floating reduction serial-order control |

Parquet is required; the default and intended full cohort includes Vortex too.
`--formats parquet` can declare a separate eight-case scope **at preparation**;
it cannot change an existing protocol. Report the actual frozen scope explicitly
and never combine partial runs into the declared 16-case result.

The public CLI's caller-owned execution context can already use parallel native
Vortex scalar DISTINCT and general grouped-aggregate providers. Preserve those
admitted providers as controls. Separate native session tests preserve ordinary
provider-driver ownership and exercise shared exact merging with caller-driven
provider progress, so an additional CPU owner is not forced. Source/operator worker counts alone
cannot establish which reducer owner ran. This distinction was reviewed before
any full-size timing; the original eight SQL statements remain unchanged.

SQL Q05 lowers to a grouped aggregate with no measures, whose shared membership
owner is `RowSet`. The primitive `DistinctRows`/`DropDuplicateRows` path in
`parallel_distinct_rows.rs` is separate. Q05 can exercise the changed RowSet
dictionary traversal only when its incoming representation preserves a supported
dictionary domain; this eligibility does not establish runtime activation.

## Inputs and independent references

The fact table has 5,000,000 rows, with zero-based row number `i`:

| Column | Logical type | Definition |
| --- | --- | --- |
| `id` | non-null i64 | `i` |
| `grp` | non-null i64 | `i % 50000` |
| `value` | nullable i64 | `((i * 7) % 100) - 20`, null when `i % 17 == 0` |
| `float_value` | nullable f64 | The same value multiplied by `0.5`, with the same null mask |
| `low_tag` | non-null UTF-8 | `low-` plus four-digit `i % 1024` |
| `high_tag` | non-null UTF-8 | `high-` plus five-digit `i % 50000` |
| `flag` | non-null bool | `i % 10 == 0` |

The dimension contains one row per key `0..49999`, with non-null i64 `key` and
`region = key % 16`. NumPy constructs numeric input; Arrow constructs the string
columns as dictionary arrays. Parquet uses 262,144-row groups, Zstandard
compression, dictionary encoding for the two string columns, and a 16 MiB
dictionary-page limit. Every string column chunk must report `RLE_DICTIONARY`.
The driver records the Arrow schema and all Parquet dictionary encoding metadata.

Native Vortex files are prepared once with the **base** binary through
`sl.read_parquet(...).prepare_vortex(...)`. Both roles subsequently read the same
Vortex bytes. Successful preparation and explicit no-fallback/no-external-engine
evidence are mandatory; full preparation reports are retained. Preparation is
outside the cohort timing boundary.

References are formula-derived Arrow files, independent of either ShardLoom
binary. A group has 100 input rows in the full cohort. Its numeric value is
constant because 50,000 is divisible by 100; its valid-row count is independently
computed from the modulo-17 null pattern. Region totals sum exact integer group
counts and sums. The low-tag period within each group is
`1024 / gcd(50000, 1024) = 64`, giving 64 exact distinct values per full-size group.
Scalar valid count is `N - floor((N - 1) / 17) - 1`; filtered valid count is
`floor((N - 1) / 10) - floor((N - 1) / 170)`. Integer references are exact; sum
results use the public f64 result contract, and all magnitudes and half-integers
here are exactly representable. This correlated fixture intentionally exercises
the changed owners; it does not represent general data distributions.

Every observation includes public `Result.to_arrow()` export and checks the
**entire** result against the reference: logical schema, nullability, exact
values, and cardinality. Rows are sorted only for order-insensitive comparisons,
outside all reported query clocks. There is no rounding, tolerance, sampling,
or aggregate checksum substitution. The original Arrow results are retained.

## Execution, clocks, and acceptance

Each case has one fresh persistent subprocess worker per binary role, configured
with `memory_bytes=2 << 30` and `max_parallelism=4`. The runner calls public
`sl.sql(..., client=...).collect()` and `Result.to_arrow()`. It attaches no Session;
the direct `_frame_action` path executes on each call. Read-only checks confirm
that no session cache is attached, the requested worker remains alive, any
emitted answer-cache flag is false, and native execution time is positive.
Prepared source reuse and OS page-cache warmth are permitted. This is a warm
protocol; it makes no cold-cache claim.

For each case, role order alternates by `(case_index + repetition) % 2`, including
the warmup, and only one query runs at a time. All work unrelated to the cohort
must stop before the scored `run` command. The script holds an advisory cohort
lock, records process listings, CPU model, CPU affinity, load, system/cgroup
memory, and the worker's `/proc` resource counters before/after observations.
It cannot enforce an otherwise idle host, so review that recorded context.

The authoritative time is `native_query_execution_nanos`, parsed with the existing
[`query_timing_report.py`](../../scripts/query_timing_report.py) helper. It must
declare `monotonic_wall_not_cpu` and the prepared native execution boundary,
including I/O generation validation and certification, excluding result
serialization. Missing or malformed engine timing fails the run. The wrapper
collect duration and Arrow-inclusive outer duration are retained separately and
are never substituted for engine time.

The primary metric is the **sum of the warm engine medians across every declared
case**, separately for base and candidate. Each median contains exactly three
measured samples; warmups are excluded. The secondary metric is the geometric
mean of per-case candidate/base median ratios. All case medians and all raw
observations remain available, including regressions. A strictly lower complete
total is the timing gate; correctness, resource ownership, activation evidence,
and regression review remain required before accepting a performance claim.

Every full report is retained, including native route, worker count, dictionary
activation, decoded-byte and peak-memory fields when emitted. Extracted evidence
is an index into these complete reports. Every full-size candidate Q06/Q07
observation must report at least two source workers and two operator workers.
Every Q08 observation, for both roles and formats, must report exactly one
operator worker. Missing or malformed counters fail the full cohort, after the
complete raw report and Arrow output have been retained; the 100-row API smoke
is exempt from these size-dependent activation gates. Requested parallelism is
not proof of parallel execution, and passing the worker gates is not proof of a
new RowSet owner: an admitted native Vortex provider can also satisfy them.
Retain and inspect the primitive strategy fields and the source-bound dispatch
rules. File dictionary metadata is not proof that a particular runtime dictionary
path activated. A missing runtime dictionary counter remains missing evidence
and must not be described as observed activation.

## Reproduction and artifact bounds

First obtain a matched sealed pair of release CLI artifacts. The supplied
provenance must identify their source revisions/trees, binary SHA-256 and byte
counts, original manifests, compiler, Cargo lock digest, build profile/features,
and platform. A CI test-merge revision is admitted only when its recorded full
Git tree exactly matches the requested candidate tree. Both source trees must
be available in the local Git object database.

Example commands from the repository root, with artifacts outside the checkout:

```sh
export PYTHONPATH=/workspace/scratch/8e7542fb5468/bench-python
export TMPDIR=/workspace/scratch/8e7542fb5468/test-tmp
python3 scripts/native_exact_distinct_cohort.py smoke \
  --work-dir /workspace/scratch/8e7542fb5468/exact-distinct-api-smoke \
  --provenance /workspace/scratch/8e7542fb5468/bench-artifacts/provenance.json
python3 scripts/native_exact_distinct_cohort.py prepare \
  --work-dir /workspace/scratch/8e7542fb5468/exact-distinct-5m \
  --provenance /workspace/scratch/8e7542fb5468/bench-artifacts/provenance.json
# Review and retain protocol.json and its printed SHA-256; stop other work.
python3 scripts/native_exact_distinct_cohort.py run \
  --work-dir /workspace/scratch/8e7542fb5468/exact-distinct-5m
```

Create `TMPDIR` before starting if it does not exist. The runner also creates a
dedicated temporary directory under each work directory and passes it to the CLI.
The smoke uses 100 rows, one observation per case/role, no warmups, and produces
no timing score or performance disposition. It tests API and exact-result wiring
only. Preserve failed smokes; fixes use a new work directory.

Preparation and execution refuse work directories inside Git. Source, binary,
manifest, reference, driver, and client identities are sealed; input/binary/
reference file generations are checked between queries, with full digests checked
before and after the run. Never swap candidate binaries after preparation.
Use a new protocol when the candidate changes.

The total artifact ceiling is 2 GiB, individual Arrow/JSON result ceiling is
64 MiB, and at least 1 GiB of free storage plus pending output is required. A
violation stops the run. Outputs include inputs, formula reference Arrow files,
preparation reports, frozen protocol and provenance, host context, each raw query
report, each complete Arrow result, each observation record, and `summary.json`.
No raw performance artifacts belong in Git; retain them alongside the reviewed
summary in the benchmark evidence location.
