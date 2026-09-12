# Owned aggregate representation screen

`owned_aggregate_cost` measures fresh prepared grouped integer `COUNT DISTINCT`
through the existing public `execute()` and `execute_owned()` methods. The first
returns JSON report rows; the second returns typed native columns. It does not
introduce a query implementation or compare persistence formats.

The fixed input has 262,144 rows: 32,768 groups with eight contributions each,
two to four distinct values per group, duplicate identities across writer
batches, `i64::MIN`/`i64::MAX` keys, and `u64::MAX`/values above 2^53. A separate
standard-library `BTreeMap`/`BTreeSet` builds the complete expected result from
the generated pairs. Every execution must match every requested value and rank.

The source is written once to a new file under an existing directory inside
`~/LocalData/shardloom`. The example records its SHA-256, exact schema, bytes,
writer input batch size and actual physical row ranges, then verifies the hash
again after all cases. It retains this small fixture for inspection. It never
overwrites an existing file. The writer is the pinned upstream default native
writer, with 8,192-row input batches; physical topology is recorded rather than
assumed to equal those batches.

Run it only after the native validation/benchmark owner has released the machine.
Resolve the target directory with the repository's normal storage workflow; do
not run concurrent Cargo, tests, ingest or other benchmark processes. For example,
from the source tree being measured:

```sh
cargo metadata --offline --no-deps --format-version 1
cargo build --offline --release -p shardloom-vortex --example owned_aggregate_cost --features vortex-local-primitives,vortex-write
```

Then run the resolved `release/examples/owned_aggregate_cost` binary, supplying
the actual revision and build configuration. These values are explicitly labelled
as caller-supplied metadata in the receipt. Keep the binary hash, host profile,
Rust version, exact build command, source revision and working-tree diff with
the raw receipt; the example does not infer a clean checkout or claim to measure
host resource ownership.

```sh
RESOLVED_BINARY --workspace /Users/dylan/LocalData/shardloom/perf-all-20260906 --source-revision ACTUAL_HEX_COMMIT --build-label 'release; vortex-local-primitives,vortex-write; exact local build configuration' > /Users/dylan/LocalData/shardloom/perf-all-20260906/owned-aggregate-cost-UNIQUE.json
```

Replace `RESOLVED_BINARY`, `ACTUAL_HEX_COMMIT` and `UNIQUE` with the resolved
values. Use a new receipt path. No timing result is checked into this note.

Each of four cases uses a fresh prepared source/session: requested P1/P4 crossed
with LIMIT 32/32,768, offset zero and a 1 GiB session envelope. Each arm receives
three warmups and twenty measurements, with ten JSON→owned and ten owned→JSON
measurement pairs. Requested and observed worker ownership remain distinct in
the receipt. Each call executes fresh aggregate state; source/layout metadata
and the uncontrolled OS cache may remain warm.

For each sample, the driver measures result construction through native report
and certificate return, pauses timing for complete value/evidence validation,
then times result destruction. The reported total is the sum of those two
intervals. Verification touches result buffers before destruction and can change
cache behavior; this sequencing is the same for both arms. Preparation, fixture
writing, validation, transport and persistence are excluded. Every raw warmup and
measurement, pair position, median and nearest-rank p95 is retained.

The required work check is that `materialized_group_value_count` equals K for
JSON reports and zero for owned columns, while exact results, certified native
execution and the absence of fallback all hold. Every sample must return shared
session credits to zero after result destruction. The receipt also records:

- Owned logical result-buffer bytes and serialized report-text bytes, with their
  different scopes. Neither is total allocation or unique retained memory.
- Current shared session reservations at return/after drop and the cumulative
  session peak. The peak combines both arms; it is not an isolated per-arm peak.
- Input arrays, maximum input chunk size and actual worker counters.

Judge materiality from the full raw paired distribution, separately for the two
worker grants and two K values. Row-rendering work reduction alone does not prove
a material latency gain. A positive screen still needs a separately recorded
retention decision; this fixture cannot establish Full43, ingest, process-RSS or
product-wide gains. It also cannot attribute native/Arrow/Parquet versus JSONL
export timings to representation alone: the existing sink families have different
sync, reopen, checksum and publication boundaries.
