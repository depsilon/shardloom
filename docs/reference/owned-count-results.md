# Owned COUNT results

The integer and nonnullable UTF8 COUNT(*) paths passed combined-source UAT at
`4f2c7b97`. The [acceptance report](../benchmarks/combined-performance-uat-2026-09-12.md)
records 552 complete owned-result executions, persistence/lifetime tests and
nine public CLI/Python session calls. Historical timings below retain their
original source scope.

`PreparedVortexAggregate::execute_owned()` accepts one identity integer or UTF8
group key and one COUNT(*) measure, ordered by count descending with an optional
key-ascending tie term. The source Struct and key must be nonnullable. The
existing integer COUNT DISTINCT admission remains available. Each call performs
fresh native aggregation against the held generation-checked source.

The output contains the group column followed by the count alias. Integer keys
preserve their original width and signedness; UTF8 keys use native VarBin arrays
with U64 offsets. Counts are U64. Exact key comparison determines ties without
floating conversion. Offset plus a positive limit cannot exceed 65,536 rows;
complete native output buffers cannot exceed 8 MiB. UTF8's initial empty offset
is included in that limit.

The finalizer borrows complete native group state through bounded selection and
native allocation. Existing COUNT worker partitions and exact heavy-hitter
refinement finish before it sees the result. Only selected UTF8 bytes are copied
to output, once. No grouped JSON or StatValue output rows are built in the owned
arm. Metadata and payload reservations survive their corresponding owners;
denial drops earlier buffers and restores credits. Nullable keys, COUNT(column),
other measures, transforms, HAVING and explicit spill remain outside this owned
COUNT admission.

`execute()` retains the ordinary JSON report boundary. Returned owned arrays,
cloned children and slices may outlive the prepared handle and source. The
existing explicit `write()` boundary persists a completed result to Vortex or,
with `universal-format-io`, Arrow IPC or Parquet. This does not add aggregate
result projection, compound owned COUNT, a reader cache or a new scheduler.

Vortex-first provider check: the output reuses pinned Vortex 0.85 Struct,
Primitive and VarBin arrays, native allocator buffers and existing native and
compatibility sinks. ShardLoom controls admission, complete-state ranking,
ownership and certificates. Arrow is an explicit compatibility boundary;
there is no external query-engine execution or fallback.

Historical evidence is retained under
`/Users/dylan/LocalData/shardloom/perf-all-20260906`:

- `owned-cost-resume-607db50a-count-r1.json`: 184 exact integer COUNT calls;
  large-output JSON/owned medians 35.714833/5.652708 ms at P1 and
  36.771832/4.295791 ms at P4.
- `owned-cost-resume-607db50a-count_distinct-r1.json`: 184 unchanged integer
  DISTINCT control calls.
- `owned-cost-resume-b40fd02a-count-utf8-r1.json`: 184 exact UTF8 COUNT calls;
  large-output JSON/owned medians 137.740875/52.095292 ms at P1 and
  122.152666/33.099834 ms at P4.

These compare two result APIs in one binary, including execution and result
release but excluding preparation and oracle validation. They are representation
cost observations, not Full43 or revision-against-revision speedups. The copied
cost example preserves the 184-call protocol, independent full-cell oracle,
physical DType checks and immutable pre-execution memory baseline.

The port's focused source tests cover all integer widths and extrema, exact
UTF8 bytes and dictionary domains, ties/offset/empty results, fresh execution,
source replacement, complete-state and heavy-hitter refinement, allocation
denial, source-error precedence, cloned owners and native/compatibility sinks.
They passed on the assembled source in the focused and broad native suites.
New large-output JSON/owned median ratios are 6.354×/8.630× for integer COUNT
and 2.743×/3.505× for UTF8 COUNT at P1/P4; see the combined report for small-output
controls, all raw samples and clock boundaries.
