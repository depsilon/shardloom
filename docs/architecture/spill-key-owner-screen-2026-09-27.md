# Spill merge key ownership — R5.b

Status: bounded candidate under PERF-INTAKE / RFC 0044, following merged C2.a PR #1467.
No runtime change or performance claim is retained yet.

The source audit finds existing payload transfers in ReservedHostAllocator,
resident owned intake and worker partials. Frozen ByteBuffers carry their leases;
numeric Vecs and UTF8 offsets/data move into native owners; partials retain their
arrays and credited count storage. R5.a already avoids memory-file composition.
Do not implement these transfers again or claim its measured gain twice.

A distinct copy remains in weighted COUNT spill merging. ReadBlock copies each
bounded complete UTF8 key into an independent Row so the source block can be
released. RunReader then copies that same key into `previous` for sorted-run
validation. RunMerge already owns the first Row in its heap, pops it, reads that
reader's successor, and only then returns the popped Row. The predecessor owner
therefore remains available throughout the next read.

Screen borrowing that owner for the complete-key order check instead of creating
a second predecessor copy. Preserve the first independent key, current block
release order, native Vortex runs, signatures, positive weights, signed integer
bits, declared group order, EOF validation, terminal errors, cancellation and
work leases. No new shared row allocation, source-block pinning, page abstraction,
format, admission expansion or fallback engine is proposed. Pinned Vortex's
existing native arrays/readers remain the provider; this duplicate belongs to
ShardLoom's merge consumer, not a missing Vortex buffer-transfer facility.

Freeze three bounded public native-call cases using the existing renamed-field
spill fixture: 65,536 rows with 128-byte keys and repeated compound groups in both
integer/text orders, then 131,072 rows with 64-byte unique keys. Keep the existing
8 MiB explicit operator envelope and 64 MiB disk quota. Compute complete scalar
expected values and source SHA-256 outside timing. Use fresh native execution
through the public Rust primitive call, with two requested CPU lanes, complete
report output, owned cleanup and report release. Logical request/fixture creation,
oracle verification and observer output are outside the clock. Preserve source
bytes and all route/copy/resource counters for each sample.

Run three repetitions per case in each fresh process, then three counterbalanced
process pairs for a positive candidate. Actual process RSS includes fixture and
oracle construction and is separate from credited native memory. The complete
operation and copied-byte counters decide whether removing the duplicate is
useful; there is no arbitrary percentage or one-second rejection cutoff. Do not
claim a ClickBench improvement from a pressured workload with explicit spill.

Before retention, verify exact complete results, sorted-run failures within and
across blocks, signed compound ordering, cancellation/corruption/quota failures,
terminal iteration and owner refunds. Complete required Rust/native checks,
applicable public UAT and independent review. Failed prototypes are removed while
their evidence remains. R5.c overlap stays a separate experiment.

The initial frozen control (`098934fcb6a9f9c3fe1d086c89445cdfd60a1d00`,
test executable SHA-256
`dd8363896310f5561db799622b7d96dc2de65b9ed8c4b4929c0b6dbcc8a86ae4`)
passes all nine complete calls and cleans every owned workspace. The repeated-key
cases record 20,910,592 merge-head copied bytes per call; the unique-key case
records 33,292,544 bytes. Best complete calls are 51.360 / 48.263 / 92.228 ms.
The control commit predates the C2 squash; its tree is identical to the rebased
observer commit `bc5762b1`. These measurements admit a small ownership candidate,
not a performance claim for that candidate or a production-scale spill benchmark.

The prototype removes the redundant `RunReader.previous` payload. Initial heads
have no predecessor; successor reads borrow the popped heap row, still owned by
the merge through that read. New coverage checks complete-key regression at the
native block boundary for all three key orders, terminal failure and refunds,
plus one independent head copy per key in a single native run. The existing
within-block corruption, signatures, weights, cancellation and quota tests remain.
