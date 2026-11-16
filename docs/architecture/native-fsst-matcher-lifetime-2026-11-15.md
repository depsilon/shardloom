<!-- SPDX-License-Identifier: Apache-2.0 -->

# Operation-scoped FSST matcher lifetime investigation

Status: finite implementation and validation complete under item 20/RFC 0044 and the
[ordered consumer queue](native-metadata-encoded-consumer-investigations-2026-11-13.md).
The operation-scoped implementation is retained as useful behavior within the
maintainer's later propagation/PR instruction. Its combined performance gate is
**not admitted**, and no independent matcher speedup is established. The
[propagation report](../benchmarks/native-optimization-propagation-2026-11-15.md)
preserves that gate and the separate integration judgment. The accepted
[prefilter](native-fsst-consumer-2026-11-15.md) is complete. Intake used source
`7c34815a0d99eb53afff7e11432682c096ad12cf`; the later PR #1692 review correction
`f326e6328010b89cecbd7d5d70cc73d9f09a8b69` limits filtering to active slice bytes
and is integrated before further candidate admission. Its source-bound
[correctness packet](../benchmarks/evidence/native-fsst-slice-correction-2026-11-15/README.md)
does not replace or reinterpret the original measured runtime.
This independent experiment precedes required item-19 spatial execution; it does
not extend the earlier prefilter decision or authorize a writer change.

## Provider and ownership check

Decision: `use_vortex_native_provider`, conditional on the admission evidence
below. Pinned Vortex 0.93.0 already supplies FSST LIKE and the exact
`FlatContainsDfa`. Its table construction is a function of symbol contents,
symbol lengths and literal pattern. The existing ShardLoom host allocator's
`reserve_external` contract admits bounded provider heap storage and retains
credit until that storage is freed. A new allocator or matcher algorithm is
unnecessary.

The source provider creates a fresh matcher in each LIKE invocation. The
matcher is not itself safe to cache by table and pattern: its `NoMatch` variant
can depend on the invocation's compressed backing. The same table and pattern
can have an absent candidate in one backing and a match in the next. Per-call
debug counters likewise must not become shared result state. Executable
counterexamples must preserve this distinction before reuse is admitted.

Vortex session clones share the live session-variable map. `ExecutionCtx`
snapshots the execute-parent kernel registry, not arbitrary session-variable
state. A session-wide strong cache would outlive individual requests and could
cross concurrent operation boundaries. The existing native exclusive-operation
gate and weak scoped comparison observer show the relevant lifetime boundary:
drain execution/I/O, clear the weak registry and release scoped state before
unlocking admission. Serving operations require their own attribution contract
and are outside the initial reuse candidate.

Primary review checked the actual LIKE, matcher, DFA, session, execution-context,
host-reservation and native-owner source. A bounded inventory preserves nine
complete source identities and 22 exact excerpts. Static observations establish
design constraints; they do not prove activation or a runtime saving.

## Implemented finite candidate

Retain at most one positive-contains table per exclusive native operation.
The cache is optional, bounded and initially empty. Prefix, negated,
case-insensitive, unsupported-pattern and shared projection paths keep their
existing behavior. A request uses existing native execution when reuse admission
is unavailable; this is not external execution fallback.

The implementation must satisfy all of these conditions:

- Separate immutable compiled tables from per-backing necessary-condition
  rejection, validity, offsets, output values and diagnostic counters. Never
  store a block's `NoMatch` result as a table/pattern result.
- Compare complete symbol contents, lengths, literal pattern and options.
  Pointer equality or a hash alone cannot establish identity. Bound key storage
  before copying; identity scans and copies remain inside the operation clock.
- Keep at most one retained entry. A changed identity must miss, even if the
  old table allocation remains live. Do not cache Boolean results, source bytes,
  offsets, row selections or decoded strings.
- Reserve fixed control/key storage and the conservative table/construction
  bound before allocating cache-owned objects. Use the existing host reservation
  owner; do not allocate a second payload merely to hold credit. Verify actual
  retained vector capacities against the admitted bound. Ordinary provider
  infrastructure and allocator overhead exclusions must remain explicit rather
  than becoming a full-provider accounting claim.
- Keep credit attached to the last live compiled-table owner, including a
  transient user after registry removal. Clear operation state after success,
  errors, cancellation and panic unwinding. The next operation starts empty.
- Decline reuse before cache allocation on denied credit or unsupported policy.
  Do not turn an optional cache into an unbounded spill/cache system, swallow a
  matcher error, or retry failed execution through a different engine.
- Attribute only actual lookup, hit, construction and retained-memory work.
  Distinguish eligible requests, provider calls and logical rows; do not infer
  decoded bytes or hardware behavior from these counters.

The expert comparator is a native encoded-execution maintainer reviewing exact
identity, concurrent ownership, cancellation and low-memory behavior. The initial
fixed limits must follow provider shape and a written allocation bound, not a
timing-tuned threshold. An Apple Silicon observation cannot establish x86 results.

The initial executable provider admission passed eight focused controls after
one test lint was repaired. The operation installs its charged registry only
while admitted and restores an empty session marker on drop. This avoids a new
persistent charge between requests; retained provider snapshot guards still own
their registry credit. Session type-map storage remains existing provider
infrastructure outside this finite claim. The table reservation follows the
actual constructor shape: `(512 + symbol_count) * (needle_bytes + 1) +
needle_bytes + 256`, bounded below 256 KiB. It conservatively retains construction
scratch credit with the table. Registry and inline operation state separately
reserve 512 and 4,096 bytes. These are admission bounds, not measured RSS.
The five initial native ownership controls pass, including request cleanup,
low-budget decline, cancellation and unwind behavior. Native activation is
explicit at certificate-producing prepared count, relational, aggregate and
sort-consumer boundaries; nested operators borrow the same scope. Serving,
metadata, ordinary provider callers and report-only legacy entry points do not
install reuse. The sort owner captures the drained scope counters before its
certificate is built. Complete public requests and source/package proof pass in
the combined propagation comparison, but its work/performance gate remains not
admitted. The final native suite and unchanged-provider identity bindings retain
these ownership controls; they do not turn the failed comparison into a speedup.

## Admission and correctness proof

Before a release comparison, require focused provider and native-operation proof
for identical tables across slices, different backing contents with equal tables,
changed symbol bytes/lengths/pattern/options, escapes and symbol boundaries,
empty/all-null/Unicode values, dense candidates, malformed-input/error behavior,
credit denial, exact refunds and last-owner lifetime. Prove successive requests
and concurrent/serving contexts cannot acquire another operation's entry.

Reuse the existing 54 plain flat/dictionary/FSST inputs and frozen independent
Boolean vectors where their exact identities still apply. Record public dispatch
and construction reductions through the existing admitted source LIKE route.
An unvalidated scalar projection bridge remains declined. Vortex-native input,
execution and output, explicit unsupported behavior, `fallback_attempted=false`
and `external_engine_invoked=false` remain unchanged.

Run focused controls before the required workspace format, default/native strict
lint and tests, provider checks, feature boundaries, supported-Rust checks and
installed-wheel proof. Preserve any failed admission, source/build observation or
validator attempt. Do not use a favorable micro-test as a complete acceptance
decision.

## Separate finite comparison and next work

The initial prospective matcher-only 9,040-case comparison was superseded before
execution by the 11,579-case combined propagation protocol, with the same 270
two-call prepared plans. Both runtimes include PR #1692's active-slice correction.
The corrected `f326e6328` baseline is freshly observed; older pre-correction
timings are not substituted. Construction, identity, credit, dense-candidate and
nonpredicate controls stay in the declared population.

The combined candidate `3cec5a410` passes complete values/resources but fails
60 selected-dictionary work witnesses, and both complete engine totals increase.
The original **not-admitted** decision remains unchanged. The selected-dictionary
follow-on closes those work misses; its separate performance gate still drops.
The maintainer permits retaining useful candidate behavior, so source/package
and portable proof support integration without a matcher performance claim.

No Full43/full10k repeat, warmup, cache manipulation or competitor run is implied.
Keep other task-owned work idle during observations and use the established
lightweight supervisors; full hashing and inventories stay outside execution.
Portable full-result replay closes this finite evidence unit. The later
maintainer-prioritized propagation queue next attributes resident join-to-consumer
costs; required spatial execution remains scheduled regardless of a reuse
performance win. Hardware breadth, optional spatial summaries, codec/fusion work, broader item 20,
RFC 0044 and CG-1 through CG-23 remain open.
