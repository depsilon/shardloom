# Exact footer aggregate completion

The scalar-footer implementation passed combined-source UAT at `4f2c7b97`:
12 focused footer tests (including four native-file tests), broad native checks,
and a new exact Q7/Q19 paired screen. The
[combined acceptance report](../benchmarks/combined-performance-uat-2026-09-12.md)
records Q7 medians 42.049459 → 13.536334 ms with zero query payload arrays.
Historical predecessor evidence below retains its original source scope.

An unfiltered global aggregate containing identity integer MIN, MAX or COUNT
columns, optionally accompanied by COUNT(*), can complete from the already held
Vortex file footer before constructing a scan. Every measure must be admitted.
Missing or inexact required statistics leave the complete existing native scan
in place. The existing standalone COUNT(*) metadata primitive is unchanged.

The provider is pinned Vortex 0.85 `VortexFile::file_stats`, `row_count` and
`Precision::Exact`, inside the existing Unix held-file generation boundary.
The root must be a nonnullable Struct. The pinned writer collects unmasked
top-level fields and rejects nullable roots; child nonnullability alone therefore
cannot prove parent validity. Integer fields retain all eight signed/unsigned
widths through the existing typed scalar conversion, including i64/u64 extrema.

COUNT on a nonnullable field uses the row count. Nullable COUNT requires exact
NullCount and checked subtraction. Exact non-null integer extrema prove their
respective result; absent extrema alone cannot prove an all-null column. Empty
files and exact all-null facts produce ordinary COUNT=0 and MIN/MAX=NULL.
Contradictory exact null counts, empty-input extrema or a requested minimum above
its requested maximum are errors. These checks inherit the native file trust
boundary; they do not authenticate forged file statistics.

All proven values are staged in a fixed bounded array before fresh scalar state
is changed. The existing finalizer then applies HAVING and measure aliases.
Ordinary reports, retained prepared execution and JSONL/CSV export share that
path. Owned global scalar result admission is unchanged. No answer is cached,
no source is reopened for statistics, and generation validation still brackets
execution. Partitioned and non-Unix paths retain their current scans.

The execution mode is `metadata_preserving_aggregate`; the result summary records
the native provider, exact-statistic uses, covered source rows and zero source
rows visited. The historical `rows_scanned` report field remains a source row
count for metadata execution, as in metadata COUNT. No source arrays are read,
no scan/projection pushdown runs, and source read/decode/materialization flags
are false. File opening may read footer bytes and is outside the query-work
claim. Text export still materializes and writes its final scalar result row.
Completed filesystem reads in tests are distinct from segment requests and OS
device I/O. No artifact-wide bytes-saved estimate is inferred.

SUM and AVG retain ordered floating arithmetic, even for integer input.
Floating extrema, string extrema, transforms, predicates, grouping, DISTINCT,
spill and partial-measure scan elimination remain on the existing native path.
There is no external query-engine execution or new metadata registry.

Required tests cover exact/inexact/absent metadata, every integer width and
extreme, nullable/empty/all-null cases, parent validity, all-or-nothing decline,
real statistics-on/off native files, complete scalar values, observed query
payload reads, aliases/HAVING/text export and source mutation. Shared aggregate
output projection is outside this port. All 12 focused tests passed on the
assembled source, alongside the prepared, metadata and broad native checks.

The predecessor `phase-resume-focused-r21` packet passed 12 footer tests, including
four native-file tests with an observed same-descriptor read control, plus 51
prepared-aggregate, 161 metadata and the empty-aggregate tests. The statistics-on
fixture completed exact results with zero post-preparation reads; the disabled
control read actual payload. Feature Clippy passed in `r23` after test API and
lint corrections. Exact command logs and receipts are retained under
`/Users/dylan/LocalData/shardloom/perf-all-20260906`.

The predecessor `phase-query-8acb1263-p12-r1.json` packet contains 16 exact calls
across Q7 and Q19. Eight are Q7 calls: one excluded warmup per arm and three
measured alternating pairs at P12/24 GiB on the protected Vortex artifact.
Q7 elapsed medians were 40.515333 ms for the earlier control and 12.845041 ms
for the footer-enabled candidate; the candidate reported zero payload arrays.
Receipt SHA-256 is
`d4014ad4f0cf385a6d8a54c24ea4d2a28d0e323e1e5487c56047c66a8a02677c`.
These are prior observations, not measurements of this PR assembly. The roughly
27.7-ms absolute saving is a small fraction of Full43; it establishes no new
whole-workload control, stability or competitive claim. Missing exact statistics
retain the existing native scan and never authorize changing a saved artifact.

The executed focused command is:

```text
cargo test -p shardloom-vortex --features release-user-surfaces --lib footer_aggregate -- --test-threads=1
```

Prepared aggregate, empty aggregate, metadata certificate, formatter and required
workspace checks passed on the assembled tree. No broad metadata epic, PERF
gate or CG gate is closed by this finite acceptance.
