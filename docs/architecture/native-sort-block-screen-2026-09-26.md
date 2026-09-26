# Native sort block consumption — R6.a

Status: retained under the complete-query gate after paired Full43 and bounded
regression follow-up. No suite, zero-decode or memory-gate win is claimed.
This experiment belongs to PERF-INTAKE / PERF-10 / RFC 0044 and follows the
R1.a storage change. It adds no phase or competitive gate. All broader native
operator, spill, public-call and serving obligations remain open.

## Attribution and gate

The retained artifact has dictionaries in its two derived domain columns, not
numeric source columns. Existing native numeric aggregate accessors already
avoid adapter payload copies. Their recorded setup spans total 4.224499589 s in
the 35 instrumented candidate queries; the other eight queries lack these
counters. This is neither total decode CPU nor an exclusive wall saving bound.

Sort execution still constructs column-sized `Vec<StatValue>` buffers and owned
UTF8 strings, then clones row vectors while testing the existing Top-K cutoff.
A guarded Q27 stack sample confirms this remaining intermediate. The sample is
at `/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/dictionary-proof-20260926-q27-stack-admission`;
its perturbed timing is attribution only, never retention evidence.

Freeze Q26/Q27 on the retained 15,682,956,116-byte artifact, P12 / 24 GiB, ordinary
portable release builds. Control is frozen runtime `c79aa89a`, unchanged by the
later R1.a test/documentation commits. Acceptance requires at least one second
saved on a complete target query under the symmetric fastest-valid rule, keeping
every sample. Then require complete paired Full43, independent correctness and
resource tests, and normal/native validation before retention and PR. No new
ingest is needed for this read-only consumer change.

## Provider and semantic contract

Vortex-first decision: `use_vortex_native_provider`. Pinned Vortex 0.85.0 already
provides `PrimitiveArray`, `VarBinViewArray` and `Mask`; ShardLoom already owns
exact numeric access, Top-K comparison and late output materialization. Consume
those native decoded blocks without constructing row-wide intermediate values.
Copy only keys that can survive the retained cutoff. Native decompression still
occurs; provider buffers are not claimed to be entirely reservation-owned.

Initial admission covers integer/UTF8 columns, no residual predicate or spill,
the existing bounded Top-K range, and First/Last ties. Other cases keep the
existing path. The existing metadata pass must establish equal source schemas
across every partition before any native cutoff pruning: the old comparator is
not transitive across unlike signed/unsigned numeric variants. Reopened scan
files must still match the admitted schema, or execution fails before scanning
that partition; switching algorithms cannot recover earlier discarded rows.
This is an admission check, not a general source-generation consistency claim. Local and
partitioned scans share the helper. Validate complete
column lengths and valid UTF8 before pruning; preserve explicit errors, exact
integer/null/string ordering, direction, offsets, source ordinals, dictionary
epochs, and final payload addressing. Floating-point and All-ties cases remain
on their existing path. Native owners survive the chunk and release on error.

No dependency, custom encoding, foreign execution, cached answer or new public
operator is introduced. `fallback_attempted=false` and
`external_engine_invoked=false` remain required. Test renamed Unicode/NUL keys,
nulls, mixed directions, ties, offsets, later-partition winners, empty/invalid
inputs, and public outputs before measuring a candidate.

Focused validation passed: six new native-block tests, the 18 existing
`sort_rows` tests, and the final 48-test `sort` selection including native ingest
and prepared-sort consumers before the final schema-replacement regression was
added. All six new tests also passed after that addition. Native CLI/Vortex all-target Clippy passed after
extracting column decoding and work accounting into their existing scope.
Logs are `r6a-*.log` under the September 26 performance evidence root. Earlier
fixture compile errors and a timestamp-name collision remain recorded; the
collision was fixed with per-case/per-partition labels and its 2,168-byte failed
fixture was removed.

## Acceptance

The final ordinary portable release binary is frozen from clean source
`870a9b8b2a6771004244b46545a770c4d3651dc4`, SHA-256
`9dc4633dde151a596a0c20b9eea1536a7be4bf9af4ab9b4a0e813dc9d117f3d4`.
Control is `c79aa89aea02fcfe785b60130d0033ad9d5370c2`, SHA-256
`fac1e9a2ca394a9c20c61fb8a39063c9e67da353fd9c7b5a65a2816dd3a700c4`.
The changes between control and merged R1.a `c305aad8` are tests/docs only.
Both roles read the same 99,997,497-row, 112-column native artifact, with
15,682,956,116 bytes and recorded SHA-256
`31cc61cfc347cf19a0328c196d59cd1eb431679311294cdc92263fef31062b35`.
The runner verified file generation throughout and binary/harness identities at
completion. The host was Apple M5 / 16 GiB / macOS 27, Rust 1.98.0, Vortex 0.85.0,
P12 / 24 GiB policy. The policy is not an OS allocation ceiling.

| Final paired Full43 target | Control best of three | Candidate best of three | Complete-query saving |
| --- | ---: | ---: | ---: |
| Q26 | 3.782044 s | 2.296441 s | **1.485603 s** |
| Q27 | 3.331786 s | 2.066199 s | **1.265587 s** |

Both targets meet the frozen one-second gate. Their selected calls also use less
user+system CPU: Q26 4.108285 → 2.592261 s; Q27 3.675386 → 2.359583 s.
Actual RSS does not meet the separate 30% memory gate. Q26 peak RSS across all
three calls is 247–275 MB control and 258–261 MB candidate; Q27 is 292–313 MB
control and 300–316 MB candidate. These are decimal MB and observed process
peaks, not reservations or an allocation bound.

The candidate consumes 13,172,392 selected rows in 1,548 native blocks per target.
It owns 8,839 possible-survivor rows / 456,468 UTF8 bytes for Q26 and 10,431 rows /
571,112 UTF8 bytes for Q27. Those counters exclude provider allocations and final
output copies; they do not describe all query memory or only the final ten rows.
Native decoding still occurs.

All **258 Full43 complete-result comparisons passed**. The Full43 best-sums are
101.924693 s control and 100.175627 s candidate; geometric means are 0.864157 and
0.822745 s. They do not meet the suite gate and are not combined with other
cohorts. Q35 crossed the frozen regression screen (10% and 150 ms): its best calls
were 10.245398 / 12.890422 s. A six-call reverse-order follow-up passed every result
and did not reproduce that regression: 9.368116 / 7.461157 s. All six follow-up
times remain in the receipt; they are not substituted into the Full43 score.
Q35 does not enter the changed sort family.

The initial twelve-call screen used prototype `4e0adecde6d0`, before the partition
schema recheck: Q26 2.527206 → 1.647685 s and Q27 2.658913 → 1.656843 s. It passed
all results. That cohort is kept separately from final-runtime acceptance. Shared
OS cache and concurrent host load are uncontrolled; no cold-cache, dedicated-host,
official-rank or production-latency claim follows. Historical faster observations
remain historical evidence rather than being overwritten by this cohort.

## Verification and reproduction

Workspace formatting, Clippy and **3,425 tests** passed. With
`release-user-surfaces`, native Vortex/CLI all-target Clippy, **1,916 native library
tests** (10 existing ignored tests), and **1,496 CLI tests** passed. Independent
runtime review found no actionable issue after the type-admission and reopened
schema checks. Independent fixtures cover renamed/Unicode/NUL keys, dictionary
epochs, null parents, exact integer extrema, mixed directions, offsets, First/Last
ties, cross-partition winners, source addresses, unsupported float/All-ties
admission and malformed values that would otherwise lose the cutoff. Native
ingest and prepared-sort consumers are covered by existing tests. No ingest rerun
is needed for this consumer-only change.

CI's native debug lane exposed an invalid fixture-construction assumption: the
UTF8 builder rejects malformed bytes before the sort test runs when debug
assertions are enabled. The fixture now supplies owned binary buffers through
the native buffer-handle boundary, then verifies checked UTF8 rejection and
null masking in the consumer. No measured runtime code changed. All 1,916 native
debug tests pass (10 existing ignored), as do the six focused release tests,
formatting and native release Clippy. The original failing CI job is
`108466759957` in run `36264603042`. Additional validation is recorded in
`r6a-fixture-final-validation.json` under the local performance evidence root.

Build each clean source revision with:

```sh
cargo build --offline --release -p shardloom-cli --features release-user-surfaces
```

Resolve the output directory with Cargo metadata and freeze separate binaries.
Run `scripts/run_clickbench_paired_query_uat.py` with those binaries/revisions,
the same retained input, all 43 queries, `--memory-gb 24 --max-parallelism 12
--max-workspace-gib 100 --timeout 120`, and the retained complete references.
Use `--query-ids 35 --reverse-order` for the recorded regression follow-up.
The [machine-readable receipt](../benchmarks/native-sort-block-2026-09-26.json)
preserves all samples, identities, complete-result verification and activation.

Local evidence roots:

- Initial screen: `paired43_20260926T182420766545Z`.
- Final Full43: `paired43_20260926T183451559780Z`.
- Q35 follow-up: `paired43_20260926T184948503879Z`.

These are under `/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/`.
Build, source verification and validation transcripts are under
`/Users/dylan/LocalData/shardloom/performance-candidates-20260926/r6a-*`.
No large dataset copy was created for this experiment; verified completed logs
are compressed without discarding evidence. R7 cross-column residuals and
conditional dictionaries are next.
