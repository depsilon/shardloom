# Native exact DISTINCT affected-owner cohort

Status: protocol declared before measurement; the first complete 5M-row candidate
result below is **rejected by the complete-cohort performance gate**. A separate
100-row API preflight preceded measurement. This is a newly declared cohort for
the dictionary visitor, scalar validity-count, and exact parallel DISTINCT
changes; it does not reconstruct the unpublished four-query comparison of #1722
and #1723.

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

## First candidate result, 2026-11-26: rejected

The first full run completed all 16 declared cases: **128/128 exact typed Arrow
checks passed**, comprising 32 warmups and 96 measured observations. Required
worker-count and no-fallback gates also passed. The complete performance gate
**failed**: the sum of case medians increased from **27.995989544 s to
28.535106007 s**, or **+1.9257%**. The secondary geometric mean of candidate/base
case ratios was **1.0177039315**, or **+1.7704%**. Correctness success does not
override the failed timing gate; this candidate is rejected as measured.

The baseline production revision is `9074900d6cff8fd97be43336fbf151f5cfffc25e`
(merged #1723). The measured candidate production revision is
`adb8afca562927d77f3e1a65b11fc76cb871f964`. Validation/protocol revision
`1bbab19c784fef2ac9231e6ec54294f0d3b0839d` has the same production implementation:
its intervening changes are documentation, the cohort driver, and a test fixture.
The candidate release binary was sealed at CI test-merge revision
`bd92e015b7d87f3709ee7b7af7d158910a7e7049`, whose full source tree exactly matches
the measured candidate tree. The original manifests remain preserved.

### All declared case medians

Values below are engine milliseconds, rounded only for presentation. Scoring uses
the retained nanosecond measurements. Positive changes are regressions.

| Format | Case | Base median, ms | Candidate median, ms | Change |
| --- | --- | ---: | ---: | ---: |
| Parquet | Q01 inner join control | 1,537.655 | 1,574.930 | +2.424% |
| Parquet | Q02 grouped 50K control | 740.822 | 719.096 | -2.933% |
| Parquet | Q03 filtered `COUNT(value)` | 129.354 | 125.664 | -2.852% |
| Parquet | Q04 scalar `COUNT(value)` | 80.221 | 94.703 | +18.053% |
| Parquet | Q05 `SELECT DISTINCT low_tag` | 356.736 | 378.981 | +6.236% |
| Parquet | Q06 scalar exact DISTINCT | 1,255.788 | 820.036 | -34.699% |
| Parquet | Q07 grouped exact DISTINCT | 5,733.285 | 4,988.311 | -12.994% |
| Parquet | Q08 float serial control | 1,324.794 | 1,666.288 | +25.777% |
| Vortex | Q01 inner join control | 1,299.085 | 1,135.170 | -12.618% |
| Vortex | Q02 grouped 50K control | 2,748.330 | 3,147.854 | +14.537% |
| Vortex | Q03 filtered `COUNT(value)` | 7.169 | 6.013 | -16.133% |
| Vortex | Q04 scalar `COUNT(value)` | 0.921 | 1.056 | +14.646% |
| Vortex | Q05 `SELECT DISTINCT low_tag` | 419.336 | 399.456 | -4.741% |
| Vortex | Q06 scalar exact DISTINCT | 192.132 | 280.300 | +45.890% |
| Vortex | Q07 grouped exact DISTINCT | 10,033.001 | 10,959.017 | +9.230% |
| Vortex | Q08 float serial control | 2,137.361 | 2,238.232 | +4.719% |

For diagnosis, the Parquet subtotal decreased from 11.158654721 s to
10.368008740 s (-7.09%), while Vortex increased from 16.837334823 s to
18.167097267 s (+7.90%). Neither subtotal replaces the complete gate. The largest
positive contributions were Vortex Q07 (+926 ms), Vortex Q02 (+400 ms), and
Parquet Q08 (+341 ms).

### Observed execution and resource constraints

Parquet Q06/Q07 changed from one to four operator workers. Their faster medians
are accompanied by higher work and memory for grouped DISTINCT. Parquet Q07's
maximum tracked execution reservation rose from **326,078,979 to 789,715,748
bytes (2.42x)**. Median worker process CPU rose from approximately **6.04 to
9.92 CPU-seconds (1.64x)**, and final worker high-water RSS rose from 405,656 to
963,764 KiB. Process CPU is diagnostic context, separate from the engine-wall
timing gate. The measured engine samples were:

| Parquet Q07 | Sample 1, s | Sample 2, s | Sample 3, s |
| --- | ---: | ---: | ---: |
| Base | 5.480426 | 5.733285 | 5.999782 |
| Candidate | 4.029380 | 4.988311 | 7.987181 |

The candidate's slow third sample consumed 13.33 worker CPU-seconds. Exact
worker-local sets and their overlapping merged state create a concrete resource
constraint; the existing reports do not count every intermediate distinct entry.
The tail remains part of the retained result.

The source-backed exposure of the validity-count shortcut is Parquet Q03/Q04.
Q03 consistently decodes 500K filter-selected rows. Q04's prepared-source cache
coverage varies: base measured calls reread 8/9/12 row groups, candidate calls
11/8/13. Its samples therefore include different admitted cache coverage, which
limits isolated attribution without changing its contribution to the gate.
Vortex Q04 instead reports `exact_file_footer_scalar_completion` and bypasses
the new mask path. SQL Q05 uses the shared RowSet owner, but no release counter
establishes activation of its dictionary-specific traversal; the observed Q05
results must not be attributed to that traversal merely from input encoding.

### Serial specialization risk and unresolved provider regressions

Parquet Q08 is slower in all three measured pairs, with increased process CPU.
Both roles retain four source workers, one operator worker, 20 row groups, and
5M decoded rows, with similar memory peaks and no major page faults. Floating
SUM prevents parallel range merging; this query contains no ordinary COUNT
shortcut, and its two-field `(group, value)` distinct membership cannot enter
the changed single-field dictionary visitor.

Static inspection of the frozen release binaries nevertheless confirms a shared
code-generation change: `accumulate_distinct` changes from 5,430 to 903 bytes;
the candidate introduces an out-of-line
`RowSet::insert_rows<DistinctMeasureRows>` of 4,076 bytes and a 2,554-byte callback
closure; `publish_selected_rows<DistinctMeasureRows>` changes from 3,483 to 905
bytes. The new merge call site shares the generic row adapter used by serial
COUNT DISTINCT and changes its specialization/inlining. This is a confirmed
code-generation difference and a concrete regression risk. Its contribution to
the measured Q08 slowdown has not yet been isolated. These symbol sizes can be
reproduced with `nm -S --defined-only -C` on the frozen base/candidate binaries;
the instruction comparison used bounded `objdump` output for the same symbols.

Vortex Q02/Q06/Q07 retain their existing native primitive strategies in both
roles: general grouped updates, bounded native UTF-8 partition DISTINCT, and
direct grouped DISTINCT updates respectively. Inspected hot instruction streams
match after address normalization. Their CPU-visible regressions have no proved
cause in the new RowSet/mask/merge branches. Host snapshots alone do not resolve
that attribution. These regressions remain in the rejected complete result;
they are not excused as noise.

A second candidate is in progress to isolate the new merge's generic row-type
instantiation from the existing serial owner, based on the observed binary
specialization change. There is no second-candidate performance result or
speedup claim here. The failed first result and original cohort remain preserved.

### Frozen identities and evidence location

Run interval: 2026-11-26 19:33:20–19:37:53 America/Chicago
(2026-11-27 01:33:20–01:37:53 UTC). Full workspace evidence is retained
at `/workspace/scratch/8e7542fb5468/exact-distinct-5m-final`, including all raw
reports, complete Arrow results, observations, formula references, input files,
host/worker counters, protocol, and provenance. A durable result archive is
pending; its link will be added after upload. Raw artifacts are not committed to
Git. The exact source trees are
`1c5032e99a2d58302495118d186673eb5020fddd` (base) and
`afff25b0186d7728bb622ca0c302d6552b6885eb` (candidate).

| Artifact | Bytes where applicable | SHA-256 |
| --- | ---: | --- |
| `protocol.json` | — | `510d314215744a60cfe851789d272b3dbb95d3809e70c788c47dbbd02728c53b` |
| `observations/summary.json` | — | `8642352c05e3ce4848639a63c6fc5d851c01afb4d31d9d3af3ab46242858854c` |
| `provenance.json` | 14,594 | `d16b49cc21894bc9d05c2305d15e25962f1d630f003d5ca8bd9ce24fba836cf0` |
| Base CLI | 102,731,832 | `6d9e0d28fc172d1657c7319a58679b098c78cfb6c22c9fac8155145c1bf6b0c2` |
| Candidate CLI | 102,744,200 | `c94e7007f9eae9294f60b7bce15750ce283cf08dfeff3b911e3fceefa8afee68` |
| `inputs/fact.parquet` | 26,343,602 | `5fbf7ee27f10d491e57c320c49932ec95dbca9c78f64b9c165bed0460ebc7362` |
| `inputs/dim.parquet` | 146,097 | `f8df97b0b1fde14184e4399921e4c3d745baf93a8cdf393366e2a1ec79eb9ca8` |
| `inputs/fact.vortex` | 57,411,638 | `c8240188a73a8c43882036a4b35419493e15b1b154531733fba516311b0e3540` |
| `inputs/dim.vortex` | 28,813 | `5235ccbd01d4570b39e8ea8b0d82d45dde439da26707f9a42c075aa97e9a7493` |

The frozen driver SHA-256 is
`dedb07eb6116eca4e81e7c1a1ab27626847f845b8a6ddec8e09b3d8a23d70401`;
the timing helper is
`1b0fd51d1ef359ee6603f8ffddad9223df3b10e2ee5195f17acaf4a3ad86d5d1`.
NumPy is 2.3.5 and PyArrow is 23.0.1. Full manifest identities, the Python-client
source hash, build configuration, and before/after digest checks are retained
in the frozen protocol and provenance.
