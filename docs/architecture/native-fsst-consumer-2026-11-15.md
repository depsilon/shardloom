<!-- SPDX-License-Identifier: Apache-2.0 -->

# FSST consumer investigation

Status: active feasibility work under item 20/RFC 0044 and the
[ordered metadata/consumer queue](native-metadata-encoded-consumer-investigations-2026-11-13.md).
No performance result or retained production candidate is admitted yet.
The accepted source at intake is `873e60bca36f5030cec27c3baf59017ca49ad8ab`.
The [survivor screen](native-survivor-admission-2026-11-15.md) leaves existing
batch boundaries in force. The prior [FSST writer drop](fsst-consumer-screen-2026-09-26.md)
and the later [decoder validation correction](../benchmarks/native-fsst-admission-2026-10-07.md)
remain authoritative.

## Provider check and current boundary

The pinned Apache-2.0 Vortex 0.93.0 provider already has FSST arrays, symbol
tables, compressed codes, a native LIKE kernel and a symbol-aware exact matcher.
The matcher is private to `vortex-fsst` and is reconstructed by each LIKE call.
Literal contains and prefix patterns have finite byte-length limits; nullable
results and pattern escapes remain part of its contract. This work must reuse
or explicitly wrap that provider before considering another matcher.

The shared relational scalar evaluator currently prepares text arguments as
native UTF-8 views. Its value owner validates every valid value in the requested
domain. A separate legacy contains helper can call the compressed LIKE kernel.
Static call sites do not prove dispatch through a particular public request.
Vortex input and execution stay native; decoded native text is an explicit
existing route, not external query-engine fallback.

The native decoder admits integer metadata and checks every selected encoded
row's declared length, escape boundaries and populated symbols before payload
allocation. Generic text preparation then validates UTF-8. The raw LIKE kernel
does not invoke those decoder checks. Its temporary tables and Boolean output
are not proven to be entirely accounted by the existing two-row, zero-payload-
credit test. A new shared connection must not bypass validation or infer full
allocation enforcement from that narrow observation.

The initial classification is `blocked_until_vortex_or_shardloom_evidence` for
a new prefilter/reuse admission, while existing scoped provider calls keep
their present classification. No unsupported path is delegated to another
engine; `fallback_attempted=false` and `external_engine_invoked=false` remain
requirements. The source-based concerns below are hypotheses until the
executable screen records their outcomes.

### Admission observations and source-pushdown correction

The finite diagnostic now passes 864 physical/shared declarations and 378
complete native-core requests over all 54 existing inputs. Independent replay
checks every output against the 288 frozen nullable Boolean references, exact
schemas, complete declaration keys and every reported owner-close refund. The
original failed harness and replay attempts are preserved. This is correctness
and route evidence without engine timing.

The source caller map corrects the scalar-only view above: `contains` filter
expressions lower to `PredicateExpr::StringContains` and an escaped Vortex LIKE
scan filter. Contains count also has a native scalar-count route. In contrast,
the selected prefix and projection requests prepare shared text. The diagnostic
observes 3,536 direct FSST LIKE calls, but these are explicit kernel witnesses,
not proof of invocation inside a public request. Zero legacy helper calls do
not mean zero upstream FSST calls. Raw malformed input mismatches rule out an
unvalidated projection bridge; they do not by themselves reject an optimization
inside the already-admitted source LIKE path.

Before selecting a candidate, add a debug-only, explicitly registered Vortex
session observer at the successful FSST LIKE return. It counts successful calls
and logical operand rows, with overflow detection; it is not a decode metric.
Use one fresh prepared session per request, no global mutable state, and read
counts only after the request drains. Its small harness-owned allocation is
outside the production resource claim and it compiles out of release builds.
The finite dispatch check selects both existing sizes, the selective profile,
and flat/FSST representations: seven complete consumers per input, 28 total.
Contains filter/count/scan requests are positive route candidates; projection,
prefix and flat inputs are controls. Reuse frozen values, schemas and owner
refund assertions. Do not rerun the full 864-declaration diagnostic for this
observer and do not record timing or choose a performance candidate from it.

The dispatch check passes all 28 complete requests. The six FSST contains
filter/count/scan requests invoke the upstream compressed kernel: 17 calls for
each filter/scan request and nine for each count request at both sizes. Each
route covers its 67 or 8,195 logical operand rows. All 22 flat, projection and
prefix controls record zero calls. Independent replay verifies complete results,
schemas, certificate identities and refunds; source and input identities are
preserved. These are logical call counts, without a physical decode claim.

## Selected prefilter experiment, before timing

Implement one conservative necessary-byte filter inside the existing Vortex
FSST LIKE provider. This is `use_vortex_native_provider` at the existing native
scan boundary, with a ShardLoom-authored local provider modification. It does not
admit a new scalar projection route, writer policy or persisted metadata format.

For positive, nonempty, supported literal contains patterns, inspect the actual
symbol table once. Choose the first pattern byte represented by the fewest
populated symbol codes, and admit the prefilter only when that number is at
most two. The two-code limit comes from the existing three-byte search primitive:
the escape code is always the third possible candidate. Every escape retains
exact verification, irrespective of its following byte. Raw byte search may
mistake escaped literals for symbol codes; that only adds false positives.
Symbol boundaries and Unicode cannot suppress a true match because every exact
match must contain the selected literal byte in a populated symbol or escape.

Use the already-locked `memchr` 2.8.3 provider (MIT option under
`Unlicense OR MIT`) for one-, two- or three-byte search. This adds a direct
dependency edge to the vendored FSST crate, not a new resolved package. Preserve
original registry provenance and update the local modification notice. No
external implementation code, new unsafe block or execution fallback is added.

The filter table uses bounded stack storage; retained state is at most two code
bytes and its discriminator. If the complete compressed backing has no candidate,
skip exact matcher construction. Otherwise preserve its construction and reject
individual rows only when the same necessary-byte test is negative. Execute
offset metadata and visit every row's original slice even for a complete backing
rejection, preserving bounds/error ordering. Shared text validation is untouched.
Prefix, empty, unsupported, nonconstant, case-insensitive and negated requests
retain the existing path. Dense candidates pay the filter and exact-match cost;
no data-trained thresholds, samples or speculative reuse are introduced.

Before measurement, require exhaustive small code/escape counterexamples against
the original exact matcher, every frozen existing-file result, nullable and
selected-slice behavior, malformed-value parity, resource denial/cancellation,
and actual candidate/construction counts. The existing explicit debug-only
session observer records these counts in the untimed admission screen; it adds
no production counter allocations or per-row counter work. Actual performance
requests retain their ordinary certificates, and must not be represented as
having measured per-request prefilter counters. Any later production counters
would require the exclusive-operation observer/credit contract. Existing
provider scratch exclusions remain explicit and
are not upgraded to a full memory-accounting claim. Freeze the complete public
comparison only after final validation and exact runtime/input/reference binding.
Reuse prior observations only with full identity equality; do not silently repeat
successful baseline requests. All regressions and failures remain in the decision.

## Finite feasibility screen

First reuse the existing 18 FSST inputs from the necessary text-signature
experiment, selecting its ordinary layout without the declined summary metadata.
Their nine logical profiles and two sizes have complete independent readbacks
and source hashes. Flat and dictionary fixtures provide representation controls.
New fixtures are reserved for missing malformed, pattern-boundary and resource
cases; no repeated writer experiment or full10k/Full43 observation is requested.

Before selecting a performance candidate:

1. Check complete results through the actual shared and source-predicate routes,
   identify the physical FSST inputs seen by each consumer, and distinguish
   direct kernel witnesses from public dispatch. Preserve nulls, Unicode,
   escaped literals, empty/over-limit patterns, selected slices and nonpredicate
   consumers. Record actual decode/preparation work where observed.
2. Compare shared text behavior and direct compressed matching on incorrect
   row lengths, truncated escapes, unpopulated symbols and invalid UTF-8.
   Preserve the first required error and selected-domain validation. Successful
   Boolean output on malformed data does not admit a bypass.
3. Bound matcher construction, metadata execution, output ownership and scratch
   overlap. Exercise denial, cancellation, empty/null input and retained output.
   A count of payload bytes avoided is not complete resource accounting.

Keep each original attempt, exact source, input identity and complete output.
This screen records no engine timing and cannot establish a performance win.
The expert comparator is a native encoded-execution maintainer checking errors,
allocation lifetime and actual dispatch before interpreting reduced work.

## Subsequent independent experiments

If admission is feasible, freeze a conservative candidate prefilter followed by
the existing exact symbol-aware matcher. Compare it with direct compressed
matching and the admitted native decoded route. False negatives are forbidden;
false positives retain exact verification. Candidate selection must use actual
pattern/table/data characteristics and include construction, validation, scratch,
dense-candidate and nonpredicate costs. An empty or unsupported pattern retains
an explicitly admitted route rather than silently changing its semantics.

Only after that decision, separately evaluate request-scoped matcher lifetime
across existing slices. Exact symbol contents, lengths, pattern and options must
match, all retained allocations and identity checks must be charged, and changed
tables or requests must invalidate reuse. No global cache or writer default is
authorized. Each candidate gets its own complete comparison and retain/drop
decision; neither can inherit a kernel-only timing claim.

The parent queue, broader I, required item-19 spatial execution, item 20,
RFC 0044 and CG-1 through CG-23 remain open. Missing FSST admission must not
hold required spatial execution indefinitely.
