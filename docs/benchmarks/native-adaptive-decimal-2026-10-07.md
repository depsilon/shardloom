# Adaptive exact decimal experiment

Disposition: **dropped** under PERF-04/10/12. The first complete-operation
screen improves its primary score by 1.09%, below the frozen 3% requirement.
None of its five primary cells reaches 3%. Exact results and resource checks
pass, but the component saving is insufficient to retain this implementation.
There is no confirmation run, shipped optimization, memory saving or version bump.

The [decision index](evidence/native-adaptive-decimal-2026-10-07.json),
[portable packet](evidence/native-adaptive-decimal-2026-10-07.tar.xz) and
[restoration proof](evidence/native-adaptive-decimal-restoration-2026-10-07.json)
retain the experiment and prove all 941 active runtime assets match accepted
builder runtime `53cd1582`. The [design](../architecture/native-adaptive-decimal-2026-10-07.md)
describes the removed candidate; the
[conditional work campaign](../architecture/native-conditional-work-campaign-2026-10-07.md)
records the subsequent conservative membership drop and remaining gated tracks.

## Candidate and exactness

The candidate starts the existing `Total { sum: DecimalValue, count: u64 }`
at I128 zero. Checked add, remove and merge retain narrow state while exact;
overflow retries the unchanged operands in the existing I256 domain. Wide
state remains wide after cancellation or emptying. AVG widens before scaling
the numerator. Output precision, scale, exact division and diagnostic order
remain fixed. No dependency, allocator, additional state field or external
executor is introduced. Both internal representations occupy the same 64-byte
state in the component executable.

All 28 focused decimal tests pass. Coverage includes positive/negative promotion,
removal that itself promotes, every merge-width combination, cancellation,
oversized-intermediate means, scaled AVG, atomic failure and deterministic
add/remove/merge sequences checked against independently recomputed I256 totals.
Eight complete native workloads also match independent Python integer reference
rows, declared schemas and output hashes in both builds before timing.

The implementation and two test-only harness modules were removed after the
decision. Their complete source snapshots remain in the packet. This experiment
does not reopen previous directory, locality, reservation-transition or composed
COUNT drops.

## Complete-operation result

Each cell runs 15 alternating control/candidate process pairs. Each process binds
one ordinary native prepared relational plan, then performs three warmups, an
exact precheck, five timed complete calls and an exact postcheck. Operator state
is fresh for every call. Collection, full result hashing, certificate/schema
checks and result destruction are inside the call clock; fixture construction,
binding, source prehashing and independent reference comparison are outside it.

All 240 measured processes pass, covering 1,200 complete timed calls. The table
reports median paired five-call ratios, not ratios of unpaired medians. Positive
changes mean slower candidate calls. Every pair and individual call remains in
the packet, including outliers.

| Workload | Role | Candidate/control | Change |
| --- | --- | ---: | ---: |
| Dense grouped SUM | Primary | 0.978316 | −2.17% |
| Dense grouped AVG | Primary | 0.982477 | −1.75% |
| Many groups, SUM and AVG | Primary | 0.979870 | −2.01% |
| Rolling SUM | Primary | 1.002966 | +0.30% |
| Rolling MEAN | Primary | 1.002166 | +0.22% |
| Maximum-width AVG | Control | 1.002785 | +0.28% |
| Independently promoted/cancelled groups | Control | 1.002717 | +0.27% |
| Unchanged Int64 SUM | Control | 0.996898 | −0.31% |

The geometric mean of the five primary median ratios is **0.989097**. Its
fixed-seed, 10,000-resample stratified paired bootstrap 95% interval is
**[0.987176, 0.993484]**. The upper bound is below one, but the score is above
0.97 and zero primary cells meet the individual 3% gate. No cell crosses both
regression limits: more than 3% and more than 100 microseconds per complete call.
The first screen therefore fails two retention conditions. Thresholds, workloads
and scoring were not revised after seeing measurements.

## Mechanism and resources

The same candidate binary separately starts `Total` at I128 or I256, consumes
every finalized value and count, and compares its full SHA-256 with an independent
reference. Five cells each run three warmups per width and 15 alternating pairs.
Actual state transitions are counted on an untimed pass.

| Component | Narrow/initial-wide median ratio | Narrow operations | Promotions | Wide operations |
| --- | ---: | ---: | ---: | ---: |
| Narrow add/merge | 0.785365 | 532,480 | 0 | 0 |
| Wide add/merge | 0.999582 | 16,384 | 16,384 | 499,712 |
| Cancellation groups | 0.997881 | 16,384 | 16,384 | 40,960 |
| Narrow rolling state | 0.965164 | 524,224 | 0 | 0 |
| Promoted rolling state | 1.000049 | 1 | 1 | 524,282 |

The arithmetic mechanism works and can reduce its isolated cost. Initial-wide
execution in this component test is not the historical control machine code.
The separately built old/new complete-operation comparison decides retention;
the component's 21.46% narrow add/merge reduction does not replace that gate.

Both builds use a single CPU policy and a 536,870,912-byte query grant. Every
complete call returns reservations to its retained-source baseline after output
release. The largest reported native reservation peak is 18,118,880 bytes;
measured process RSS spans 24,182,784–132,907,008 bytes. Physical RAM is
17,179,869,184 bytes, separate from the query grant and OS RSS observations.
These figures do not establish a whole-process allocator ceiling.

Every measured Native I/O certificate is certified with
`fallback_attempted=false`. Its schema does not expose `external_engine_invoked`;
the harness records that field as null with an explicit unavailability note.
The source diff introduces no external execution engine.

## Reproduction and limitations

The host is arm64 macOS, Darwin 27.0.0, with ten logical CPUs. Both matched
release test executables use Rust 1.99.0 (`b940084d7`), LLVM 23.1.1 and
`release-user-surfaces`, built offline with locked dependencies and two build
jobs. The pinned native provider is Vortex 0.85.0.

- Control executable SHA-256:
  `a99a729daec0785471829a4703b78f8a66caddcda2cc771fca2f787c059ae109`.
- Candidate executable SHA-256:
  `a4b221dd019ec1f8d87e4335d17a149649bfe21cdaf3853b0ccb18c498d11bb5`.
- Frozen protocol/build/fixture manifest SHA-256:
  `92fb43bfe95a30337d71201a3bf8df29fd6cb6afa89bb34295da69dbdba89c04`.

The packet contains both 943-asset candidate snapshots, their old-production
overlays, build recipes and logs, exact fixture/reference recipes and values,
driver sources, raw process receipts, CPU/RSS observations, all samples and the
unchanged scoring rules. Its content-addressed manifest retains original bytes
for 3,389 paths in 2,152 unique payloads; every member was reopened and hash-checked.
The four local executables and native fixture payloads remain retained outside
the packet by full hashes, sizes and generation records. A rebuilt executable is
a new artifact and must not be presented as the originally measured binary.

For replay, restore the archived second source snapshot into an isolated local
checkout, rebind the drivers to a new unsynced experiment directory and keep the
shared native-work lock. Build the two recorded production variants with their
identical test overlay, run the candidate correctness tests and both fixture
validations, then freeze the new identities before following the archived screen
protocol. Original completed output paths must never be reused.

Grouped fixtures have 262,144 rows and either 64 or 32,768 groups. Rolling fixtures
have 32,768 rows with a trailing 64-row window. Native files use 8,192-row chunks.
The archived protocol fixes every coefficient formula, nullability, output order,
warmup, pairing order and exact result. Prehashing and repeated reads warm files;
this is not a cold-cache claim. Binding is separate, and the complete native
prepared-operation clock is not a public CLI startup or first-ingest clock.

Builds and native workloads ran serially with the existing storage, workload,
memory and process supervision. The outer guard's reused external WorkloadGuard
module is preserved at finalization but was not individually hashed in the
pre-timing freeze. The bounded process supervisor and in-repository timing/storage
helpers were pinned. This provenance limit does not justify discarding observations.

Failed compiler/lint checks and the two unscored fixture corrections remain
preserved: an invalid dtype constructor and a rolling request with an extra
projected column. Both builds were rebuilt with the corrected identical harness
before any performance sample. The restoration verifier also preserves its first
failure: all per-file hashes matched, but the aggregate digest used different
JSON separators from the older manifest. Matching the original serialization
resolved that bookkeeping error without changing source or measurements.

Independent confirmation, full candidate workspace/public/golden checks and
Full43 acceptance were not run because the first retention gate failed. Existing
builder acceptance remains attached to the exactly restored runtime. No candidate
acceptance or broader PERF/CG completion is claimed.
