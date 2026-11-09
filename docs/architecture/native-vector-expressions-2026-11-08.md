<!-- SPDX-License-Identifier: Apache-2.0 -->

# Shared exact vector expressions

Status: merged in PR #1671 with all 58 PR checks and all 55 exact-main checks
passing. Verified artifact retirement and post-retirement replay are complete.
The selected contract was frozen before native implementation and
observations. This resumes item 19 after the
[provider upgrade and dependency closeout](native-provider-upgrade-2026-11-08.md).
It does not complete item 19, item 20, RFC 0044 or CG-1 through CG-23.

SQL and `shardloom.functions` use the existing shared scalar-function registry,
typed native binder and evaluator. No external engine, model call, ANN index,
implicit flattening, densification or dequantization is introduced. The default
build remains lightweight; Vortex-native execution stays behind its existing
feature gates. The independent decoded expression evaluator reports this family
as unsupported until it implements the same contract; it is never a fallback.

## Shared surface

A dense vector is a native List or FixedSizeList with a direct F16, F32 or F64
component type, or an admitted dense embedding extension. All dense positions,
including inspection and selection, use that admission. Nested lists and
multi-vector collections, scalar values, integer/Boolean/decimal/text lists,
tensors, sparse vectors and quantized vectors are not implicitly dense vectors.
Arithmetic widens admitted component widths exactly to F64; widths may differ.

| SQL function | Arguments | Result |
| --- | --- | --- |
| `VECTOR_DIMENSION` | Dense, sparse or quantized vector | U64; parent null propagates |
| `TENSOR_SHAPE` | Fixed/variable tensor; optional literal `'logical'` or `'physical'` | FixedSizeList of nonnull U64, with the tensor's rank and parent validity |
| `VECTOR_VALUES` | Dense vector or embedding | Same native list storage; embedding metadata explicitly removed |
| `VECTOR_ELEMENT` | Dense vector, integer index | Nullable original component type |
| `VECTOR_SLICE` | Dense vector, integer start, optional integer end | Variable List of original component type; parent/control null propagates |
| `VECTOR_DOT` | Two dense vectors | Nullable F64 |
| `VECTOR_L2_DISTANCE` | Two dense vectors | Nullable F64 |
| `VECTOR_COSINE_SIMILARITY` | Two dense vectors | Nullable F64 |
| `VECTOR_NORM` | Dense vector | Nullable F64 Euclidean norm |
| `VECTOR_NORMALIZE` | Dense vector | List/FixedSizeList of nonnull F64 with nullable parent |

Python names are lowercase. Strings in column positions retain their ordinary
column-reference meaning. `array([...])` composes as an inert literal expression
without losing source or resource bindings from other arguments. Float literals
remain explicit. Only empty/all-null literal ARRAYs in dense argument positions
receive contextual nullable F64 type; unrelated ARRAY inference is unchanged.
Integer-valued ARRAYs remain outside this family. Python omission of slice end
emits two arguments; explicit `None` emits a null third argument. Boolean values
are not integer index/bound controls. Shape layout is a literal string.

Normalization preserves variable versus fixed list shape and fixed width, but
returns ordinary list storage even for embeddings. It carries no embedding
identity or normalization metadata claim. Slicing always returns an ordinary
variable List because selected coordinates no longer identify the original
space. `VECTOR_VALUES` explicitly removes identity without reconstructing a
sparse or quantized representation.

## Dimensions, shape and selection

Known unequal fixed dimensions refuse at binding, including empty-input plans.
Variable lengths are compared only when both selected parents are present;
unequal lengths fail before component-null handling. Empty present dense vectors
have dimension zero and dot/norm/distance zero.

Element indices are zero-based, with negative indices counted from the end.
Null controls and out-of-range indices return null. Slices have inclusive start,
exclusive end, clamped bounds and negative indexing from the end. An omitted
end means length; an end before start produces an empty list. All existing signed
and unsigned integer control widths are handled without overflow. Selection
preserves component nulls, original widths, signed zeros and NaN payload bits
under either float policy. Carried values are not arithmetic observations.

Dense dimension uses native coordinates and parent validity. Sparse dimension
reads the selected required U64 `size`; quantized dimension reads the selected
code-list length. Both enforce declared `uniform_size`. These inspectors do not
reconstruct or certify unused coordinates, codes or parameter payloads. Existing
intake validation is unchanged. A collection has no implicit single dimension.

Tensor shape retains existing admitted tensor component types, including integer
and Boolean components. Fixed shape comes from metadata; variable shape comes
from the selected Int32 shape row. Logical dimension `i` equals physical
dimension `permutation[i]`; omission is identity. Dimension names do not change
sizes. Rank zero returns an empty shape list; zero dimensions remain zero.
Variable shape reuses the native rank, nonnegative dimension, uniform dimension
and product-equals-data-length validator, including validation after a zero
dimension. Shape inspection reads data-list coordinates, never data components.
This kernel boundary alone establishes no file-I/O saving.

## Embedding identity and observation order

Binary arithmetic between embeddings requires equal fixed widths and equal,
nonempty `space_id`. `model_id`, `model_version`, `representation_version` and
`preprocessing_id` must then match exactly, treating absent/null as unknown.
Unknown secondary values may match each other only after the known space anchor
matches. Known-versus-unknown or unequal values refuse. No case, whitespace or
Unicode normalization is invented. `normalization` and `source_provenance` are
not coordinate-space identity fields. Mixed embedding/bare-list arithmetic
requires explicit `VECTOR_VALUES`. Unary operations need no pairwise identity
comparison. Invalid metadata and static dimension/space conflicts remain binding
errors even for null or unselected rows.

Arguments evaluate under their own contracts. Vector null shielding never
suppresses an argument-expression error or a rejected nonfinite literal. For
each arithmetic row: a null parent yields null; two present parents have their
dimensions compared; any null component yields a null result before any numeric
payload observation; only then are valid components observed under float policy.
Components are neither skipped nor replaced with zero.

Strict policy rejects observed nonfinite components and nonfinite final results.
Overflow of an ordinary intermediate product/subtraction must not falsely reject
a representable final result. Extended policy has these outcomes, with canonical
F64 NaNs for newly computed NaN results:

- Dot: NaN or zero-times-infinity produces NaN; opposing signed infinite
  products produce NaN; otherwise an infinite product retains its sign.
- Norm: NaN takes precedence over infinity.
- L2 distance: NaN input or equal-signed infinity subtraction produces NaN;
  any other infinite difference produces positive infinity.
- Cosine: any nonfinite component produces NaN. With finite components, either
  zero norm produces null, including empty vectors.
- Normalization: a finite zero norm produces null. A NaN norm produces NaN
  components. An infinite norm produces signed zero for finite components and
  NaN for infinite components. Component-null rows already became parent null.

Exact-zero dot/cosine is positive zero; negative nonzero underflow keeps its
negative sign. Norm/distance zero is positive. Normalization preserves the sign
of original zero components. Finite cosine is clamped to `[-1,1]`.

## Finite numerical contract

Finite products use a scoped exact signed accumulator with two 68-word banks
and quantum `2^-2148`. Binary64 significand products fit U128; the banks cover
all 32/64-bit component counts, including `a*a - 2*a*b + b*b`. This is bounded
native kernel state, not a general arbitrary-precision runtime or dependency.

Dot, norm and L2 distance receive one nearest-even F64 rounding of the exact real
result, including subnormal and overflow boundaries. Norm/distance use an
integer-square-root candidate and exact midpoint comparison, not a rounded
squared sum followed by square root.

Cosine and normalization retain separate exponents and rounded normal mantissas
from exact accumulators, avoiding intermediate full-value overflow/underflow.
For `u=2^-53` and `eta=2^-1074`, the declared finite error bounds are:

- Normalization: `abs(result - exact) <= 3*u*abs(exact) + eta/2`.
- Cosine: `abs(result - exact) <= 5*u*abs(exact) + eta/2`.

Normal-mantissa rounding has relative error at most `u`. Correctly rounded
square root and division bound normalization's pre-scaling ratio between
`(1-u)/(1+u)^(3/2)` and `(1+u)/(1-u)^(3/2)`, inside `1 +/- 3u`.
The two rounded norm-square mantissas, their product, dot mantissa, square root
and division bound cosine between `(1-u)^2/(1+u)^(5/2)` and
`(1+u)^2/(1-u)^(5/2)`, inside `1 +/- 5u`. Exact rational squared inequalities
check those bounds. Normal scaling is exact; one final subnormal rounding adds
at most `eta/2`. The independent reference tests the resulting rational bounds
directly. Corpus ULP screens are additional checks, not universal ULP claims.

Exact filtered retrieval scores every eligible candidate. Rounded scores may
tie; examples and tests declare deterministic secondary ordering. This is not
an infinite-precision ranking promise.

## Native providers, ownership and acceptance

The Vortex-first review used pinned 0.93.0 list length/sum, extension storage,
ListView, FixedSizeList, Take, native numeric owners and row/component slicing.
ListSum skips null components and has different NaN/empty semantics; it is not
the selected arithmetic contract. The unstable row visitor's cosine example is
an ignored sketch, not a registered kernel. No suitable named exact arithmetic
provider was found in the scoped array/scalar/compute sources.

Use Vortex-native coordinate, validity, storage, selection and allocation
providers. Implement the scoped arithmetic and semantic binding in ShardLoom.
Selected component spans may execute to native primitive storage under admitted
expansion credits. Do not construct a decoded Arrow or row-object execution
substrate, or materialize the entire child merely for a selected span. The
existing execution/Native I/O certificates retain their owner and no-fallback
fields; `fallback_attempted=false` and `external_engine_invoked=false` remain.

Literal floating ARRAYs store one allocator-owned child and broadcast through
native ListView coordinates. Retain the existing 65,536-byte/4,096-node/24-depth
declaration limits and 128-element bound for nonliteral constructors. Constant
query vectors may exceed 128 components within declaration/resource limits.
Retained buffers own their credits through plan drop, extraction, clones,
slices, empty results and final release. The shared constructor work also fixes
the pre-existing flat UTF8/Binary literal credit-lifetime gap without removing
conservative binding/plan-copy charges.

Element access and slicing use the existing credited compact selected-copy path
with raw floating carriage. Slice spans first use native ListView coordinates;
the final result does not retain an oversized original child. Value projection
retains the native storage owner. Arithmetic scratch, selected expansion,
temporary metadata, growing output and retained buffers are admitted before
allocation. Cancellation is checked inside wide component loops. Existing
16-prior-output/64-node expression reuse bounds remain unchanged.

Explicit vector-value, element and slice outputs carry their unobserved-value
policy by bound field through aliases, ordinary projection/filter/range chains
and ordering. Terminal detachment and ordering/spill copying use that policy
only for those fields; sibling payloads keep their existing validation. This
closes the public result-copy boundary that otherwise re-observed nonfinite
selected components under strict policy. Numeric consumers and ordering keys
still observe their inputs, and other operators retain their existing payload
validation contracts. The policy creates no new dtype or persisted marker;
source intake is unchanged. Prefix ordering optimizations requiring uniformly
validated fields remain unselected for these explicit mixed-policy outputs.

Acceptance must execute all ten functions through SQL and Python with F16/F32/
F64, mixed widths, actual wide constant queries, fixed/variable/empty vectors,
parent/component nulls, metadata conflicts, control extremes, encoded/chunked
children and duplicate selections. The pre-implementation independent reference
contains 270 finite numerical cases, corroborated with independent Decimal
calculations, plus semantic/binding controls and a 1,536-component query fixture.
Run exact rounding, rational error-bound and deterministic retrieval checks;
resource denial, cancellation and final-owner cleanup; stream/spill, native
write, separate-process reopen and independent Arrow/Parquet readback; required
default/native/feature/MSRV/installed-package checks; portable replay; and hosted
review/PR/main acceptance. Original failures remain evidence. No benchmark,
performance claim, package publication or whole-item completion is included.

## Local verification

The [portable evidence packet](../release/evidence/native-vector-expressions-2026-11-08/README.md)
binds the final source, frozen references, native runtime and isolated wheel.
Its source policy retains each original input inventory. Only named status prose
may differ after runtime checks; the one later Python binding-test expansion
is covered by the subsequent complete public and installed runs. Final document
checks and the contract-test crate use the completed prose.

| Check | Observed result |
| --- | --- |
| Default workspace | 3,322 passed; zero failures or ignores |
| Native workspace | 7,021 passed; zero failures; 37 opt-in ignores |
| Formatter and strict lint | Default and native checks passed |
| Feature configurations | Seven checks passed, including 1,198 write-only CLI tests |
| Minimum supported Rust | Both existing Rust 1.96.0 default/native checks passed |
| Selected public Python | All 21 tests passed with zero skips |
| Python core | 1,263 discovered cases; 862 reported runs; 59 optional/environment skip records; zero failures/errors |
| Isolated installed wheel | All 397 tests passed with zero failures, errors or skips; isolated load and uninstall passed |
| Guide examples | Two Python examples and one SQL example passed |

The public matrix checks 6,480 finite arithmetic observations against the frozen
reference through SQL and DataFrame execution under both float policies. Native
tests also check 4,918 rounding cases, semantic/null and embedding-identity
controls, retained credits, denial, cancellation and raw-value delivery through
actual spill. The 1,536-component retrieval fixture exercises all admitted widths,
fixed/variable storage and deterministic secondary ordering. Persistence checks
write and reopen in a separate process and use independent Arrow readback.

PyArrow 25.0.1 cannot reopen every nullable fixed-list Parquet parent. Complete
native Parquet and independent Arrow results remain checked; present-parent
Parquet subsets receive independent PyArrow readback. This limitation does not
become a claim of universal external-reader compatibility. Selected native-buffer
credits likewise do not establish a whole-process RSS bound, zero-copy,
zero-decode or file-I/O savings.

Original failures, including the demonstrated literal-credit and public raw-copy
defects and their passing regressions, remain in the packet. At that local
checkpoint, hosted review, PR checks, exact-main checks and post-merge retirement
remained subsequent closeout steps. The frozen local record is unchanged; the
later outcome is recorded separately below.

The [proof-supervision follow-up](../release/evidence/native-vector-proof-supervision-2026-11-08/README.md)
preserves that packet byte-for-byte. Both original PR #1671 Linux Python 3.13
attempts stopped at the aggregate integration deadline after 321 completed tests
and no observed failed assertions; neither is accepted. The same wheel passed
397 tests under Linux Python 3.10. The follow-up gives integration a finite
1,200-second budget, retains 600 seconds for each setup/package step, and records
the selected budget in each step observation. Test discovery, zero-skip policy,
package/runtime inputs and the outer CI job limit are unchanged. All 21 focused
proof/package tests and the fresh local installed run of 397 tests pass, including
load and uninstall. The original runtime checks remain bound to unchanged inputs.
These local observations preceded the fresh hosted checks below.

## Hosted integration and cleanup

[PR #1671](https://github.com/depsilon/shardloom/pull/1671) merged as
`9c4ada03e1c30ed20fd2118c78a8be3fb69b0dd8` after all 58 checks passed at
`3c632441e37b038720aa9045e6f461628f8a776a`, with no unresolved review findings.
Each of the three fresh PR installed-wheel lanes passed all 397 tests with
zero failures, errors or skips, plus isolated loading and uninstall. The
[exact merged-main run](https://github.com/depsilon/shardloom/actions/runs/42547270332)
then passed all 55 required checks. A local status-fetch interruption was
preserved and recovered without restarting hosted jobs or changing their
contract; it was not a test failure.

Verified closeout retired 27,810 generated entries, including 25,117 regular
files and six environment links. Repeated identities and closed-handle checks
preceded removal. Current input generations, the accepted runtime and tested
wheel, both committed packets and all original observations remained unchanged.
Complete source-bound replay passed after retirement. The earlier unexecuted
retirement plan remains historical; the executed inventory covers the proof
follow-up as well as the original vector work.

The separate storage-maintenance pass retired reproducible debug output and
losslessly compacted older result records. It preserved original failures and
current inputs; see the [storage record](local-development-storage.md).
No package was published and no performance run was added. This closes the
selected vector-expression delivery, while wider nested-schema admission,
deep traversal and the remaining item-19 lifecycle obligations stay open.
