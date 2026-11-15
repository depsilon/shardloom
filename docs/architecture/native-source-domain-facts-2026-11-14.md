<!-- SPDX-License-Identifier: Apache-2.0 -->

# Exact source facts for native floating predicates

Status: local experiment in progress. This extends item 20 C for the named
ALP/FastLanes comparison consumer in the November 13 investigation sequence.
It does not close the remaining optimization queue or establish a speed claim.

The expert comparator is an encoded-execution maintainer reviewing preservation
of typed comparison errors, source generations, nullable populations, signed
zero, and actual provider dispatch rather than planned encoding labels.

## Vortex-first decision

`use_vortex_native_provider`: the pinned Vortex 0.93.0 file footer already owns
typed exact statistics; ALP and FastLanes already provide comparison kernels.
The held `PreparedVortexSource` is opened before alias-only filter admission.
No new statistics scan, persisted format, global fact cache, expression engine,
codec, or execution dependency is needed. Source guards bind footer facts to the
same file generation used by execution. The existing shared typed tree retains
its validated declaration when a file kernel replaces its physical operation.

The change belongs only at fully bound alias-only file filter admission.
Ordinary binding happens first; computed projections, limit barriers, nullable
struct roots, unsupported domains and missing proofs retain their existing
native evaluator. No external engine is invoked and fallback remains false.

## Semantic contract

F32/F64 comparison requires exact zero NaN count and finite exact extrema from
the held file footer. Min/max alone are insufficient because Vortex extrema
skip NaNs. Exact all-null counts or an empty source prove an empty valid domain.
Inconsistent facts decline the optional rewrite without introducing earlier
errors. The existing dtype-only integer, Boolean and text gate is unchanged.

Literal conversion must be exact: finite F64 values, exactly representable F32
values, and integer-to-float round trips checked in i128. F16, nonfinite literals,
lossy literals, null-containing IN and negated IN remain outside this unit.
Conjunction admission is complete, never partial. Typed equality identifies
both signed zeros, whereas the provider distinguishes them: zero equality is
the interval [-0,+0], inequality excludes both, and range bounds choose the
appropriate signed endpoint. Positive IN expands zero to both representations.
The legacy primitive predicate API keeps its existing semantics.

## Observation and ownership contract

Fixed optional session counters observe successful ALP, FastLanes bitpacked
and frame-of-reference comparison adaptor dispatches. They count invocations
and logical operand rows, including repeated/nested work; they do not count
unique query rows, avoided physical reads, bytes decoded, or all-column decode.
Empty/null shortcuts and declined kernel attempts do not count as dispatch.
The fixed counter owner is reserved through the resident memory pool for the
operation lifetime; the session registry holds only a weak pointer. Draining
and dropping the operation releases its credit. Ordinary exclusive scopes can
report their completed counters. Concurrent
serving scopes report attribution unavailable. Counter overflow also makes
evidence unavailable without altering query results.

## Required evidence and decision

Tests cover exact/inexact/absent/contradictory facts, signed zeros for all six
operators, F32 narrowing, integer endpoints, nulls, NaNs, infinities, source
replacement, alias barriers and actual compressed kernel dispatch. Required
default/native workspace checks and relevant provider checks follow focused
validation. Reuse compatible complete accepted N observations and version any
missing encoding/selectivity controls before the paired experiment. Preserve
every failure and regression. Retain only after complete-operation engine time
decreases and correctness, resource, ownership and no-fallback evidence pass.
No new Full43, broad 10k, competitor repeat, publication or release is requested.
