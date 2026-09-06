# Encoded Numeric Reductions

## Decision and provider check

Continue PERF-10 through the existing scalar aggregate boundary. Pinned Vortex
0.85 supplies Constant and RunEnd arrays, primitive child execution and validity.
ShardLoom supplies its exact aggregate semantics, selection order and evidence.
Classification: use native Vortex arrays and implement ShardLoom reduction kernels;
no external execution, new dependency, unsafe code or Arrow conversion.

The pinned RunEnd MinMax kernel delegates to the complete run-value child.
ShardLoom must additionally clip the logical window, retain caller selection
order/multiplicity, count only selected non-null rows, and preserve its sum and
distinct semantics. The scoped visitor supplies these missing contracts rather
than assuming that an unqualified child aggregate answers every selected query.

The initial full-suite candidate exposed a dense additive-only regression: Q30
rose from 0.190227 to 0.591538 seconds. Selecting nonfused states once per column
instead of checking all 90 measures per run reduced a targeted warm best to
0.263660 seconds, still above the repeated control's 0.194261 seconds. Preserve
these intermediate measurements; the optimization is not accepted on overall
suite gains alone.

Dense RunEnd columns with two or more measures consisting only of SUM/AVG use
the retained native typed consumer. Ordered addition still visits every logical
row, so this operator class does not benefit from weighted reduction and pays
additional run-table traversal. COUNT(*) does not change that admission decision.
The encoded probe declines the entire update before state mutation or child
execution. Constants, selected runs, single additive measures and run consumers
with count/distinct/extrema retain their existing admission. This choice depends
on operator semantics, never query text or column names. Final retained-candidate
measurements are recorded in the continuation benchmark report.

Admit identity numeric measures when every referenced column has an admitted
constant or run-end representation. Inspect all shapes before changing state.
Keep the existing typed path for other representations and transforms. Native
provider failures after admission propagate; they do not trigger another route.

Real file scans retain lazy GetItem/Select projections; native RunEnd slices are
also deferred. Only resolve a bounded projection chain rooted directly in a
physical nonnullable Struct, using the configured native executor and stopping
at its first leaf. A root matcher alone does not constrain native child execution;
unknown structural inputs must therefore miss before execution. Flatten up to
16 nested Constant/RunEnd slices using checked window metadata without executing
an unknown slice child. Admit only these known leaves with unchanged shape. Native
structural-resolution calls and their time are included in the admitted work
record. The persisted-file test requires this route to execute, as well as
checking every returned scalar; in-memory kernel tests alone are insufficient.

Constant values and run values are consumed with logical weights. COUNT,
COUNT DISTINCT, MIN and MAX update per visited value/run. SUM and AVG preserve
the exact existing floating-point addition sequence, including per-array fusion
and argument-offset behavior; they do not multiply a value by its run length.
Selected rows retain order and duplicate multiplicity. Run-end offsets, unsigned
end widths, strict end ordering, null values and the complete logical window
are validated before use. Only native run children are canonicalized, never the
expanded logical numeric array on this admitted route.

Run children can still allocate through native execution. The existing allocator
coverage exclusions apply. Counters distinguish logical rows, child primitive
executions, child rows and weighted visits; they do not imply zero decode,
hardware memory traffic or a process-memory bound. Existing aggregate state
reservations and outer cancellation boundaries remain in effect.

## Acceptance and later encodings

Compare complete results with the prior native primitive route for all supported
integer widths and floats, nulls, empty input, sliced runs, unordered/repeated
selections, extrema, offsets and cancellation-sensitive/error cases. Prove that
large constant/run inputs avoid logical-array expansion. Measure elapsed work
through real native aggregation before making a speed claim.

Frame-of-reference and bit-packed kernels remain a subsequent experiment under
the same packet. Their bounded native slicing and operation-specific algebra
must preserve overflow and existing sum order. In particular, base-times-count
arithmetic is not automatically equivalent to the engine's floating accumulator.
No broader encoded algebra or whole PERF-10 completion is claimed by this stage.
