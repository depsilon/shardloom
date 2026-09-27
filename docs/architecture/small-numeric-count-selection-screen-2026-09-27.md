# Small-input numeric COUNT selection — C2.b

Status: bounded experiment under PERF-INTAKE / RFC 0044, after the C3/C6
admission batch. No production selector, disposition or speedup is established.

Compare the existing single-integer COUNT worker/partition route with the
existing direct native aggregate route. Both receive the same public request,
source, complete values, 1 GiB policy and requested parallelism of twelve. A
one-shot test-only choice is consumed before worker admission. Declining workers
retains the existing provider-driver restoration on the same runtime before
reading any input. Admitted failures do not trigger another execution route.
The existing admission-pressure hook is unsuitable for this comparison: single
numeric admission can keep ordinary workers when optional reservations fail.

The initial matrix uses 128, 1,024, 8,192, 65,536, 262,144 and 1,048,576 rows,
with skewed, uniform and nearly unique UInt64 keys. Native flat chunks contain
at most 65,536 rows. The renamed `alias_key` groups use COUNT(*) descending and
LIMIT 10. Five alternating pairs preserve every observation. Route fields must
prove both worker activation and direct execution with restored provider drivers.
An independent ordered-map tally checks every result value and key tie.

The clock includes fresh public source/session preparation, native execution,
complete result-summary formation, native I/O certification and report cleanup.
Fixture construction, oracle validation and benchmark-record output are outside
the clock. Fixture sources are hashed and shared by both roles; the guarded
runner owns the child process and storage limits. Each fixture removes only its
successfully created path. This screen does not measure CLI startup or a
persistent prepared session, nor does the flat input establish behavior for all
encodings.

Admit a production selector only after a measured crossover using physical and
request characteristics. Retention needs held-out sizes/distributions, encoded
and nullable inputs, signed extremes, low-memory/cancellation/source-generation
acceptance, complete Full43 and the normal workspace/native checks. Preserve
existing large-input workers unless new evidence supports changing them. No
minimum gain percentage or seconds cutoff applies. Vortex-native execution and
explicit no-fallback contracts remain unchanged.
