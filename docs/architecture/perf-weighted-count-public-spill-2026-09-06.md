# Public weighted COUNT spill integration

This integration checkpoint follows the private weighted native-run adapter in
`perf-weighted-count-spill-2026-09-06.md` and RFC 0044. Public dispatch and its
correctness tests are registered for the next serial validation gate. An absent
aggregate spill policy keeps current behavior. Validation and performance results
must be recorded separately; registration alone is not a performance claim.

The first public family is exactly COUNT(*) over one nonnullable identity UTF8
column, or one nonnullable identity integer plus one nonnullable identity UTF8
column in either declared group order. All integer widths and native dictionary
domains remain supported. Output requires a positive limit, checked offset,
count descending, and optionally an ascending prefix of the declared group-key
order. NULL, transformed keys, HAVING, other measures, arbitrary secondary order
and residual predicates fail explicitly. Predicate work must use the existing
native scan pushdown. The existing `--vortex-aggregate.spill` object supplies
workspace, quota and operator memory; weighted COUNT requires at least 4 MiB and
uses an internal 64 KiB maximum individual UTF8 key, not another public knob.

One source-native accumulator reserves the entire operator envelope from the
query pool before constructing its finite child pool or temporary workspace.
Source projection/typed numeric and dictionary execution use the configured
source session. Run conversion/read/write uses the same registry and borrowed
runtime, with its child allocator. It does not launch partition workers or a
second runtime beside spill state. Its immutable final owner retains the full
parent envelope until the enclosing prepared source validates its generation.
Only then may a public result and certificate escape. Output JSON allocation and
provider paths outside the allocator contract remain explicitly separate.

The direct public route streams full source chunks into this accumulator. A
separate test-only adapter entry accepts a drained epoch for later worker integration:
the caller must stop admission and join all jobs first, then visit every committed
partition key and every unconsumed deferred contribution once. The accumulator
checks the sum of committed and suffix weights against the declared epoch rows
before resuming source input. A failed visitor, mismatch, cancellation or file
error makes the attempt terminal and destroys all owned runs on drop. No partial
result, local top-K output, retry into an old registry or hash-only key can pass
this boundary. Tests use the actual retained partition and partial visitors.

The shared changes promote the weighted namespace/module, extend the pure explicit
aggregate-spill family gate, and select the family inside the existing held-source
spill branch. The immutable finalized weighted result constructs its own aggregate
scan/report while its parent owner survives final source validation. A separate
`VortexWeightedCountSpillReport` and `native_weighted_count_spill` field carry
the weighted-specific evidence/certificate branch. Existing
DISTINCT `complete_pairs` and `buffer_capacity_pairs` fields retain their meaning;
weighted records and byte arena evidence must not masquerade as pair counts.
The CLI uses separately prefixed weighted fields. No parser change is needed.
Certificate admission matches exactly one family, policy, source weight, complete
group count, offset/limit, owned cleanup and actual run geometry. The policy's
explicit `cleanup_abandoned_weighted_count` method accepts only this namespace;
the existing cleanup method still names exact DISTINCT. Generic joins and broader
aggregate spill remain outside this behavior class.

The 64 KiB admission bound retains worst-case head/output reservations. Initial
runs use their actual bounded-buffer maximum key; merged runs propagate maxima.
Typed evidence reports minimum/maximum written block rows and maximum run-key
bytes, zero when no run is needed, plus separate source/native/merge/selection
copy counters. This is owned operator work, not complete provider allocation or
process RSS coverage. The public direct route visits every source row and does
not claim worker-epoch reuse; that transfer seam is tested separately.

Before public admission, native fixtures must exercise the actual held-file
scan with renamed/reordered fields, both group orders, nullable rejection,
native filtering, dictionary domains, long keys, exact integer extrema, ties,
offsets and empty input. Tests must prove no-run small behavior, actual multiple
native runs, full independent returned values, stable failures before source
open for malformed effect payloads, cancellation/quota cleanup, source mutation
at the final boundary, and parent credits retained through the last result owner.
The registered tests include actual SQL/DataFrame full-value execution and
side-effect-free routing, native dictionary sources with different code domains,
both composite key orders, all integer widths at the accumulator boundary,
unchosen filter columns, empty and unprunable zero results, forged certificates,
actual multiple runs, quotas, cancellation, oversize keys, final source mutation,
and parent credits retained after source/session release. The full test/build
matrix and measurements remain pending at this source checkpoint.
