<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native typed unary state

Status: implemented with local functional acceptance under PERF-02/03/07/10/11/12 and
CG-3/5/19/20/21. The [phase plan](phased-execution-plan.md) owns sequencing.
This continues the locally accepted [typed expressions](native-typed-expressions-2026-10-03.md),
[unary composition](native-unary-composition-2026-10-02.md) and
[dynamic pivot](native-dynamic-pivot-composition-2026-10-03.md) contracts.
The source version is 0.4.0; source preparation does not establish publication.
The [acceptance report](../benchmarks/native-typed-unary-full43-2026-10-03.md)
records complete public, resource and retained-result regression evidence.
Hosted review remains open; the Q9 timing/RSS observation is inconclusive.

## Decision and reuse

The native result boundary already carried Binary, Decimal128, Date32 and
timezone-free TimestampMicros, while retained unary values and several binders
required the narrower statistics scalar representation. This unit completes
that boundary in the existing unary operators. Direct file calls and
relational composition share type admission, state transitions and output
ownership. Existing primitive, boolean and UTF8 behavior remains compatible.

Vortex-first decision: `implement_shardloom_kernel`, extending the existing
ShardLoom unary state machines. Pinned Vortex 0.85 already supplies logical
DTypes, binary/decimal arrays, temporal extension storage, validity, native
scalar access, selections and allocator-backed result construction. Reuse those
providers through the existing source, native key and result owners. Vortex
scalar/array ownership alone does not account retained payload copies against
the operation grant, and a slice can retain unrelated source buffers. Selected
state therefore needs compact, admitted ownership with explicit lifetime tests.
Do not construct extreme timestamps through the narrower upstream calendar
scalar validator; preserve their admitted native extension/storage boundary.

The implementation must not add a second query evaluator, decode-to-Arrow
execution, intermediate files, a source replay or an external-engine fallback.
Retained scalar state is an operator materialization boundary, not a promise
of zero-decode execution. Copy only values the existing state policy retains;
leave ordinary source access and final column construction with their shared
owners. Reuse existing checked casts, decimal helpers and logical type rules.

| Existing owner | Responsibility in this unit |
| --- | --- |
| Unary schema binding and `BoundUnary` | Admit the four exact scalar domains before execution, including empty input; keep unsupported parameter/type combinations deterministic. |
| Native batch/result values and memory leases | Own selected variable bytes and scalar state without retaining unrelated source domains; preserve logical type through the bound schema. |
| Exact unary keys and selector state | Add typed key identity while retaining first/last/remove-all, source ordinal and floating-bit behavior. Hash matches never replace full key equality. |
| Rewrite and pivot declarations | Use the existing core literal model for typed parameters; preserve existing JSON spellings and scalar semantics. Statistics and legacy predicate values keep their own narrower contract. |
| Shared native cast/arithmetic and pivot state | Reuse checked type/value rules; keep state, discovered schema and complete output owned by one execution. |
| Native completion, writers and operation resources | Preserve complete values, output admission, source generations, cancellation, memory release and failed-publication behavior. |

## Operator scope

- DISTINCT, drop-duplicates and duplicate masks admit Binary, Decimal128,
  Date32 and TimestampMicros keys, including composite keys. NULL identity,
  first/last/remove-all behavior, ordered delivery and existing floating-bit
  identity remain unchanged. Decimal precision/scale and temporal logical types
  remain distinct; no implicit storage-integer or decimal-scale key coercion.
- Tail and sampling retain typed payloads with the same requested suffix,
  candidate, replacement, fraction, seed, tie and weight rules. Typed payloads
  do not widen the numeric weight domain. A produced input is consumed once;
  file-backed tail retains suffix-range avoidance.
- Scalar rewrites carry typed untouched columns and preserve typed forward-fill
  state across batches. Mask and replacement literals bind to the target's
  declared type before execution. NULL rows do not determine the type. Checked
  typed arithmetic uses the shared expression semantics; string/regex rewrites
  remain UTF8 operations. Row-number state remains UInt64 and execution-local.
- Melt preserves typed ID fields and admits value columns with one lossless
  common admitted scalar type. Decimal common-type derivation follows the
  existing declared-domain rule and selected values use checked exact rescaling.
  Incompatible mixed domains, including temporal/storage-integer mixtures, are
  rejected during binding rather than serialized into a mixed scalar container.
  The existing primitive-only heterogeneous Variant behavior remains unchanged;
  it does not authorize Variant coercion for the four new typed domains.
- Rolling COUNT uses validity over the admitted scalar domains without copying
  variable payload. Existing numeric rolling policies remain intact. New exact
  decimal or temporal rolling arithmetic is outside this unit and must remain
  a deterministic unsupported operation.
- Pivot admits typed index/domain keys and typed first/first-unique payloads,
  with its existing COUNT and primitive numeric aggregate behavior. Typed fill
  values use a lossless common output type and checked selected conversion.
  Domain names remain deterministic, bounded and collision-safe; the index,
  discovered schema and retained values stay in the same completed owner.
  This does not introduce decimal SUM/MEAN or temporal arithmetic. Unsupported
  aggregate/type and margin/schema combinations fail explicitly, including
  empty inputs. Existing primitive margin behavior is preserved.
- Explode retains its separately accepted typed/nested payload contract; this
  unit must not regress that path or infer nested-key equality from it.

Typed filtering before or after a unary stage continues through the shared
native expression binder. Legacy primitive predicate declarations do not gain
implicit decimal, binary or temporal conversion merely because those values can
now be retained. Unsupported predicate/type combinations must fail at binding,
including empty input; unrelated typed payload columns must remain usable.

## Declaration compatibility

Use `ScalarValue`, the existing expression literal model, for expanded rewrite
and pivot literal declarations. Do not broaden the statistics-oriented
`StatValue` domain throughout the engine. Provide explicit mappings where
existing reference/legacy helpers still require their admitted primitive values.
Those mappings may reject an unsupported legacy path; they must not execute
the work through the decoded expression evaluator as a native fallback.

Existing CLI JSON objects, field names, aliases and primitive literal meanings
remain valid. Typed additions must reuse the engine's existing binary,
decimal and calendar parsing/validation rules and preserve exact integer text
where JSON numeric precision would be insufficient. Python only declares the
literal; query calculation remains in the native engine. Both SQL table
expressions and DataFrame methods must lower through the same request and state.

Changing the unpublished Rust primitive request's literal fields from
`StatValue` to `ScalarValue` is an intentional 0.4 source API change. Document
the conversion and update in-repository callers together; keep stable report
schema identifiers and compatible CLI/Python declarations unchanged. This is
not permission to rename unrelated public APIs or add a new public scalar type.

The additive CLI literal forms are:

| Domain | Typed object example | Exact value contract |
| --- | --- | --- |
| Binary | `{"type":"binary","value":"00ff"}` | Even-length hexadecimal text, including an empty string. |
| Decimal128 | `{"type":"decimal128(20,2)","value":"1.23"}` | Exact decimal text validated against declared precision/scale; JSON floating numbers are rejected. |
| Date32 | `{"type":"date32","value":"1969-12-31"}` | Admitted ISO date or signed Int32 storage text. |
| TimestampMicros | `{"type":"timestamp_micros","value":"1970-01-01T00:00:00.000001Z"}` | Admitted ISO timestamp or signed Int64 microsecond storage text. |

These objects are accepted by mask/replacement/arithmetic literal fields and
pivot `fill_value`, subject to the operation's bound type rules. Existing
untyped primitive pivot fills and the six original typed literal spellings keep
their meaning. Typed scalar additions do not widen structured/nested literal
construction or legacy primitive predicates. Python bytes/bytearray, Decimal,
date and datetime declare these forms; Decimal metadata is checked before
formatting, independently of the active Python decimal context.

In Rust, replace `StatValue` constructors with their `ScalarValue` counterparts
in `MaskScalar.replacement`, `ReplaceScalar.to_replace/replacement`,
`NumericScalarArithmetic.operand` and `VortexPivotProjectionRequest.fill_value`
(including `with_output_policy`). Predicate values still use `StatValue`.
Pivot fill replaces missing cells; a present NULL first/first-unique cell remains
NULL. Decimal rewrite arithmetic retains the existing kernel's type rules,
including its same-scale rule for decimal/decimal arithmetic; replacement and
lossless common-type reshape conversion use their separate exact-rescale rules.

## Ownership, admission and failure

Reserve metadata, container capacity and retained variable payload before
allocation. Count capacity rather than logical byte length where the owner can
grow. Copies, replacement overlap, retained fill values, pivot key/name growth
and buffer transfer must keep their credits until the last dependent owner is
dropped. A tiny selected value must not keep an unselected large dictionary or
binary/text backing buffer alive through retained unary state.

Operation cancellation, narrow grants, source replacement, slow/failing consumers
and writer failures keep their existing shared behavior. Unsupported unary
state spill remains an explicit resource denial; surrounding native sort runs
do not authorize a new spill path. Collection retains its existing row/field/byte
limits, while admitted writers consume complete bounded native batches.

## Acceptance

Freeze independent complete values and declared schemas before accepting a new
path. Cover every changed family directly and composed with transformed inputs
and downstream consumers. Include empty/all-null inputs, repeated/null values,
binary zero bytes and invalid UTF8, long variable values, decimal precision/scale
boundaries, full Date32 and microsecond timestamp storage boundaries, renamed
schemas, composite keys, floating signed zero, seeded sampling and source order.
Test parameter/type denials on empty as well as populated input.

Cross batch boundaries for forward-fill, tail, sampling and rolling. Test
repeated execution, source/producer/session drop, cloned retained output,
unselected large domains, constrained memory, cancellation, failing consumers
and failed writer cleanup. Compare complete typed values after every
representable writer reopens; keep format-specific fidelity and denials explicit.

Run required formatting, strict default/native lint and workspace/native tests,
Python declaration/runtime suites, lean/MSRV and affected documentation gates.
Freeze the final source and executables, run the expanded complete public and
direct-unary matrices, then paired Full43 regression under the existing serial
storage/process guards. Retain initial failures and any prescribed repeats.
Performance observations establish only their recorded scope; they do not
establish a general speedup, process-RSS bound or competitive superiority.

### Local acceptance

Frozen source `948551d4` passes 9,300 public checks and 14,125,745 complete row
comparisons. The new unary subset contains 2,700 checks and 5,412 rows from 135
independent declarations; all 6,600 prior cases and their row counts are retained.
The separate direct-unary matrix passes 202 checks/131,734 rows. All 25 selected
local gate categories pass, with 2,526 route-specific resource proofs for the new
successful calls and 174 explicit ORC denials. Source/binary identity, complete
oracles, output hashes, raw envelopes and failed observations are preserved in
the [immutable packet](../benchmarks/evidence/native-typed-unary-2026-10-03.json.xz).

All 258 paired Full43 results and 18 prescribed reversed-order calls match their
complete retained references. The initial aggregate crosses neither threshold.
Q15 timing and Q34 RSS flags do not reproduce; Q9 reverses timing direction and
remains flagged for timing/RSS. Its performance conclusion stays inconclusive,
with both cohorts retained and no speedup or uniformly unchanged-performance
claim. The report distinguishes these observations from functional acceptance.
Hosted runtime review and checks remain pending. The earlier review exhausted
the bot's quota; local review evidence is retained. No package, tag or release
is published.

Broader aggregate/window semantics, nested keys, adapters, general state
spill/recovery, native Python binding experiments, paused large text/format
campaigns and package publication retain their existing owners. CG-1 through
CG-23 remain visible in the phase plan. Every new successful execution must
retain `fallback_attempted=false` and `external_engine_invoked=false`, with real
Vortex payload proof kept distinct from placeholder artifact status.
