# Retained native simple aggregates

Status: integrated implementation and tests; compilation, validation and matched
retention measurements are pending. This extends the existing PERF-02 prepared reader work and
PERF-10 reusable physical lowering under
[RFC 0044](../rfcs/0044-resident-runtime-resource-ownership.md).
The [phase plan](phased-execution-plan.md) remains authoritative for wider gates.

## Contract

`PreparedVortexAggregate` retains one immutable request, its projection/predicate
lowering, one `PreparedVortexSource`, and the owning `ResidentVortexSession`.
Every `execute()` runs the current native aggregate scan with fresh scalar,
grouped, distinct, worker, and output state. It returns the ordinary complete
execution report and an actual native I/O certificate. The adapter retains no
answer, query-result batch, or aggregate partial between executions. Source identity checks
bracket every execution, including metadata-pruned zeros and same-file pressure
replay. Replacing a file invalidates its prepared handle.

The initial API admits one local Struct source with integer group/measure fields,
up to two identity keys, 1–64 identity COUNT, COUNT DISTINCT, or SUM measures, and
existing native predicates/order/HAVING/offset/positive output-limit semantics.
Nullable integer fields retain existing null semantics. Integer keys and distinct
values preserve their exact identities, including adjacent values above 2^53.
SUM retains the existing ordered floating-point accumulation contract; this API
does not introduce exact arbitrary-width integer sums or a new reduction order.
Spill is rejected before opening or creating a workspace. Constructed measures,
residual predicates, other measure families, multiple sources, and noninteger
group/measure fields remain outside this initial API.

The ordinary file-reading entrypoint keeps its current admission and execution
decisions. An immutable `AggregateLowering` factors only its existing rewrite,
projection, and predicate planning. The retained entrypoint passes that lowering
to the same execution body. That body still binds expressions, consults native
file statistics, selects existing kernels, and completes all keys before global
ordering and LIMIT. Its existing pressure replay remains bounded to the same
held file, with fresh state and no external executor.

`prepare_aggregate()` uses the ordinary request-based CPU ownership policy. The
explicit in-session form never starts a second runtime. When the supplied session
already owns provider drivers, it does not also admit the dedicated aggregate
worker pool. A caller-only session can use the existing worker family or the
existing scoped temporary-provider-driver helper after schema rejection. All
such execution is inside the retained source admission gate. Reported worker
limits are also capped by that retained session's grant when the request allows
more CPUs; the physical report preserves the original request and selected cap.
Worker limits and memory remain their current scoped native ownership contracts; no
claim covers arbitrary provider allocations or process RSS.

The native certificate describes this actual execution, not an independently
rerun oracle. An unprunable predicate can return zero arrays after native filter
work. The common aggregate report therefore records conservative scan-side
read/decode/materialization scope in that case, while actual row and Arrow
output flags remain false. It does not infer observed bytes from an empty output.

## Validation and acceptance

- Repeat complete scalar COUNT/COUNT DISTINCT/SUM values against the checked
  five-row fixture, proving one open and an advancing execution count.
- Preserve grouped order, offset and LIMIT after all input keys contribute;
  compare complete native nullable/extreme-key results with independent Rust
  maps and sets across repeated calls and worker counts 1, 2 and 4.
- Check metadata-pruned and unprunable empty results, certificates, replacement
  invalidation, admission before spill effects, resource grants, and released
  owned bytes after handles drop.
- Confirm an existing provider session and a dedicated worker session retain
  separate, nonoverlapping CPU ownership policies.
- Run the focused prepared-aggregate tests, the ordinary
  aggregate/replay tests and native feature gates. No checks have run yet.
- Measure fresh versus retained execution through complete reports separately
  from preparation, joined close and verification. Use the same source/request,
  alternating order, raw repeated samples and complete independent values.
  Until that comparison passes, no latency or throughput improvement is claimed.

## Persistent public execution

The CLI's existing persistent Python worker now retains the matching aggregate
handle for admitted integer COUNT/COUNT DISTINCT/SUM requests. Its key includes
the effective public request, normalized primitive with bound source, and native
resource policy. Every call executes the core operation and renders its complete
report and actual native I/O certificate. No correctness oracle runs inside the
request. Changed requests, source paths, limits, predicates, memory or CPU grants
discard the previous operation. An execution or source-generation error clears
the handle and fails that call; only a later explicit call may prepare again.

Optional preparation itself never executes a query. Request-shape rejection
returns before opening and leaves the broader ordinary route in place. A schema
rejection returns an `UnretainedVortexAggregate` containing the already opened
source and lowered ordinary operator. The CLI explicitly executes that object
once and drops it, avoiding a second footer open while keeping broader text,
floating-point and residual shapes out of cross-call retention. Tests check a
zero execution counter after preparation and one after that consuming call.

The public latency harness adds three SQL cases using the existing Python
`public_workflow_run` interface: scalar integer aggregates, filtered aggregates,
and globally ordered exact distinct groups with offset/limit. All nine cases
run through fresh CLI processes, the persistent worker and the Python client
with alternating baseline/candidate order. It validates complete independent
values, one source open, advancing executions, actual native certificates and
reused lowering on subsequent candidate calls. Measurements are pending.

No new dependency, binding package, runtime fallback, query-specific answer
cache, or whole-PERF completion follows from this packet.
