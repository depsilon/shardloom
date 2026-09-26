# Native sort block consumption — R6.a

Status: admitted for a bounded prototype; no retained speedup claim.
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
not transitive across unlike signed/unsigned numeric variants. Local and
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

Focused validation passed: five new native-block tests, the 18 existing
`sort_rows` tests, and the final 48-test `sort` selection including native ingest
and prepared-sort consumers. Native CLI/Vortex all-target Clippy passed after
extracting column decoding and work accounting into their existing scope.
Logs are `r6a-*.log` under the September 26 performance evidence root. Earlier
fixture compile errors and a timestamp-name collision remain recorded; the
collision was fixed with per-case/per-partition labels and its 2,168-byte failed
fixture was removed. Performance and complete Full43 acceptance remain pending.
