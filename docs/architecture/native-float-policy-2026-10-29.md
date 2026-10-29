# Explicit native floating domains

Status: implemented with local public, native/default workspace and installed-package
acceptance under active item 20. The [acceptance report](../benchmarks/native-float-policy-2026-10-29.md)
defines the tested scope and remaining boundaries. The default remains strict;
this unit does not close item 20 or establish hosted or release acceptance.

## Caller contract

An immutable `float_policy="strict" | "extended"` belongs to the native session.
`strict` is the default and preserves existing observed-nonfinite refusals.
`extended` admits valid F32/F64 NaN and positive/negative infinity. NULL validity
is independent of every floating bit pattern. Resource overrides cannot change
semantics, and prepared execution retains its original session policy.

The policy travels through the existing embedded owner, configurable resident
session, runtime owner and native execution context. It is not an execution
resource, a process global or a thread-local setting. Native reports and
certificates must identify the selected policy. Existing constructors stay strict;
explicit constructors and public options select extended behavior.

## Carriage and observation

Selected copies, retained arrays and admitted Vortex/Arrow IPC/Parquet roundtrips
preserve source F32/F64 bits, including negative zero and NaN payload/sign.
Nonfinite bits are valid floating storage. Extended intake bypasses only the
finiteness restriction; dtype, bounds, validity, nested parent masks, ownership,
memory and cancellation checks remain mandatory. Null slots stay unobserved.

`IS_NAN`, `IS_INFINITE`, `IS_FINITE` and `SIGNBIT` classify valid values and
propagate NULL. They may classify a carried native value before strict
arithmetic/key refusal. Strict Arrow intake still rejects nonfinite values.
The small Rust `MemoryColumnValues` and owned-Float64 convenience intakes retain
their finite-only, bounded-JSON contract. Extended Rust columnar intake uses
`ResidentMemorySource::from_arrow_record_batch` with an explicitly extended
`ResidentVortexSession`; public typed/Python columnar sources use that owner.

Arithmetic-generated NaNs have canonical positive quiet-NaN bits:
F64 `0x7ff8000000000000`, F32 `0x7fc00000`. Plain selection and copying never
canonicalize input bits. Negation and absolute value canonicalize NaN results;
`SIGNBIT` observes the original sign. NaN never becomes NULL or a missing row.

## Equality, ordering and keys

Under extended policy all NaNs compare equal and sort above positive infinity.
Signed zeros compare equal. Other values retain numeric order. NULL ordering
remains the operator's explicit choice. Scalar comparisons, hashing, grouping,
DISTINCT, joins, sets, sort ties and window peers use the same normalization,
including nested keys. Normalization affects identity, not selected output bits;
stable representatives retain the existing tie behavior and original width.

Mixed integer/float comparisons preserve existing exact finite boundaries.
NaN and infinities must never be coerced into integer endpoints. Widening F32 for
comparison does not change its carried representation.

## Arithmetic and reductions

Extended floating `+`, `-`, `*`, `/` and `%` use their floating outcomes,
including infinity and canonical NaN for division by zero or invalid operations.
Integer and decimal checked behavior is unchanged. Conversion to an exact
integer/decimal rejects nonfinite input; existing `TRY_CAST` behavior can return
NULL. Probabilities, weights, time bounds and resource controls remain finite.

SUM/AVG retain finite state plus checked counts of NaN and each infinity sign.
NaN or opposing infinity signs produce NaN; one infinity sign produces that
infinity. Existing finite-only accumulation errors remain. Moving windows remove
special counts separately so finite results recover when specials leave.
DISTINCT uses the shared normalized identities.

Moments, covariance and correlation similarly separate nonfinite participation
from finite state. A participating special produces NaN only after ordinary
minimum-cardinality rules permit an output. NULL pairs remain ignored.

MIN/MAX, ordered selectors and discrete quantiles use the total value order and
preserve selected bits. Continuous quantiles return the existing Float64 domain:
exact ranks select the endpoint, equal infinite endpoints retain that infinity,
a strictly interior mix with one infinite endpoint takes its sign, opposite
infinities produce NaN, and a NaN endpoint produces canonical NaN. Temporal
linear interpolation continues to require finite anchors and finite results
under both policies.

## Persistence and evidence

Vortex remains the highest-fidelity output. Admitted Arrow IPC and Parquet output
preserve floating bits. Raw nonfinite JSON/JSONL or CSV numeric fields fail
explicitly before successful publication. A caller may explicitly cast to UTF8
to choose `NaN`, `inf` and `-inf` text, without preserving a NaN payload.
Reports encode classifications and hex bits rather than invalid JSON numbers.
A failed text consumer must leave a later binary action usable.

Independent validators distinguish NULL, classification, sign and raw bits.
Carriage checks are bitwise; arithmetic checks require canonical generated NaN;
semantic key checks use the documented equivalence classes. Ordinary finite
error tolerances remain specific to each operation.

## Vortex-first provider check

The pinned Vortex 0.90 primitive/scalar implementation uses Rust `total_cmp`
and bitwise equality/hash in `vendor/vortex-array/src/dtype/ptype.rs` and
`vendor/vortex-array/src/scalar/typed_view/primitive/pvalue.rs`. These distinguish
zero signs and NaN payloads/signs, so they do not supply this analytical identity
policy directly. The decision is to wrap Vortex concepts: retain existing
arrays, encodings, storage, native memory owners and compatible compute, and
implement shared ShardLoom semantic normalization at the existing key/expression
boundary. No alternate float representation, dependency or query engine is added.
Specialized providers must prove equivalence or decline to the existing shared
native relational owner with explicit provider evidence. They must not silently
execute strict semantics for an extended session.

The existing native aggregate, exact finite moving-window total, moments,
quantile, source and selected-output owners are extended in place. Native I/O
and execution certificates retain `fallback_attempted=false` and
`external_engine_invoked=false`; materialization and decode remain reported.

## Acceptance gate

Both policies require independent F32/F64 fixtures for zeros, infinities,
signed/payload quiet and signaling NaNs, subnormals, finite extrema, NULL and
empty data. Cover exact mixed-integer boundaries, equality/hash/order agreement,
nested parent validity, stable representative bits, joins/sets, FILTER/DISTINCT,
reducers, windows and recovery after special values leave a frame. Resident and
spilled results must agree.

Exercise typed native, SQL, Python SQL/DataFrame, workflow and CLI through shared
execution; include dictionary/encoded and Vortex/Arrow/Parquet input, source and
result transfer, producer close, source disposal, reuse, streaming EOF and late
errors, resource/cancellation faults, retry and binary/text sinks. Complete
values, types, certificates, source/product identity, replay corruption controls,
default/native workspace checks and installed-wheel checks precede completion.
No benchmark or package release belongs to this work unit.

Primary semantic references reviewed October 29, 2026:

- [Rust floating total order and NaN guarantees](https://doc.rust-lang.org/std/primitive.f64.html#method.total_cmp).
- [PostgreSQL analytical NaN ordering](https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-FLOAT), as a semantic reference only.
- [RFC 8259 numeric grammar](https://www.rfc-editor.org/rfc/rfc8259#section-6).
