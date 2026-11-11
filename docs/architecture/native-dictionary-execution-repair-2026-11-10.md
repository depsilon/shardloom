<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native dictionary execution repair

Status: local correctness acceptance passed; hosted integration pending,
November 10, 2026. This repairs the provider callback exposed by the requested
current-runtime observation before the shared operator-state investigations.
It does not close item 19, item 20, RFC 0044 or CG-1 through CG-23.

## Observed failure and cause

The isolated current-runtime observation at Variant merge
`8f426b3d2e774be2a9d30a731c33b8c830378d99` executed each of the frozen 10,000
requests once: 9,904 passed, 76 panicked with `DictSlots codes slot`, and 20
reported a CLI response without JSON. Independent saved-result verification
preserves every original outcome and confirms complete correct values and
finite engine clocks for the 9,904 successes. The complete observation is
invalid as a performance result; its total engine time remains unavailable.

A separate unscored reproduction of original request 433 captured the stack
through `execute_shared_dictionary`, Vortex's execute-parent callback and native
sort gathering. The callback was introduced during the Vortex 0.93 provider
integration. It ignored the supplied child and read `dict.codes()` from the
parent before checking the supported dtype. The pinned executor's `ExecuteSlot`
step detaches that codes slot and can invoke the callback against the suspended
parent. Reading its missing slot panicked even for text dictionaries that the
fixed-width callback should decline. The original cause of the 20 CLI no-JSON
responses was not independently captured; all 20 pass after the repair.

The 58 checks on PR #1676 and 55 checks on its exact main merge passed. Those
checks establish their finite coverage and do not turn the failed 10,000-case
observation into a success.

## Provider decision and implementation

Vortex-first decision: `use_vortex_native_provider`. The pinned Vortex 0.93
executor, `ArrayParts`, `DictSlots` and dictionary data already provide the
required concepts. Keep the change inside the existing feature-gated
`shardloom-vortex` provider boundary.

The callback checks the supplied child's index and encoding and the parent's
dtype before reading child slots. An eager, complete parent is reused when its
codes pointer matches the current child. For a suspended parent, the safe native
constructor validates a complete dictionary from the current codes child and
remaining values, preserving dtype, length, dictionary metadata and statistics.
Both paths use the existing fixed-width decoder and its output/scratch owners.

No dependency or vendored executor change is needed. Native input/output,
residual execution and allocation admission remain in the existing owners;
there is no new Arrow materialization, unsafe slot restoration or external
query-engine execution. Native certificates retain `fallback_attempted=false`
and `external_engine_invoked=false`. The finite buffer/scratch accounting proof
does not claim a bound on total process memory.

## Correctness evidence

The installed-session text regression fails on the original provider with the
same missing-codes panic. The repaired path passes for shared and exclusive
parents. Additional fixtures let Vortex itself detach the child, with eager
kernels disabled and an explicit callback counter. They cover primitive,
Boolean and decimal output, null codes/values, all-null values, empty inputs,
duplicate codes, one-byte-short denial, exact peak credit and release after the
last output buffer drops. Tests never fabricate an incomplete array.

Formatting and strict default/native lint pass. The default workspace suite
passes 3,323 tests; the native suite passes 7,068, including all three new
regressions. Its 37 existing opt-in ignores remain explicit. A fresh release
build supplies both the CLI and Python library used by the repair cohort.

Before candidate execution, the correctness selection froze all 96 failed
requests plus 421 passing controls. Controls are the first passing request in
each affected-archetype/surface/profile/native-format cell and each
family/surface/native-format cell, deduplicated in original manifest order.
Selection does not use timings. The 517 requests span primary, holdout, smoke
and scale profiles and preserve their original requests, sources, independent
expectations, Python client, worker, clocks and per-query limits.

One isolated execution of each request passes. All other local task work stays
idle during query execution; full input/source hashing and recursive inventories
remain before and after it. The existing active-output, free-space, concurrency,
deadline, resource and process-cleanup guards remain enforced. Post-run hashes
and process drain pass. Independent offline verification checks all 517 complete
answers and native certificates, including every original failure and all
controls, without executing another query.

The [portable evidence packet](../release/evidence/native-dictionary-execution-repair-2026-11-10/README.md)
retains source/check/runtime bindings, original and repaired observations, the
byte-exact frozen comparator and independent expectations. Its verifier
recomputes the original 10,000 outcomes and the separate 517 repaired outcomes
from saved records. The original failures are never replaced. Archive parts
are bounded, and runtime/input/compiler payloads remain represented by their
captured identities rather than copied into the packet.

## Original failures and remaining work

The first regression attempt stopped at the unchanged 12 GiB storage floor.
Verified retirement of reproducible debug output preserved all current input
generations, protected runtimes/wheels and release cache; the second attempt
reproduced the old code's panic. A test-counter `Clone` compile error and a
Clippy documentation-backtick failure are also retained with their subsequent
passing checks. Earlier passing default/focused checks differ from final source
only by that exact test-comment correction. Final native checks use final bytes.

This is a correctness repair, not an optimization acceptance or a replacement
full10k result. The historical complete 1,513.095098453-second observation stays
historical; it does not measure the changed provider/runtime. Later experiments
must freeze relevant cohorts before candidate execution, reuse saved cases only
with exact compatible source/runtime/request contracts, and obtain a predefined
baseline where that proof is missing. Do not combine repaired failures with old
successes into a synthetic full10k pass or repeat the whole suite routinely.

After hosted integration, recommendations 8 and 9 remain independent experiments,
followed by recommendation 10's separate resource objective and the shared
pipeline program in the [phased plan](phased-execution-plan.md#planned). Useful
spatial support and resource-admitted deep traversal remain required under item
19. No package publication or broad performance claim is included.
