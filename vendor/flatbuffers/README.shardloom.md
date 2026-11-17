# FlatBuffers checked verifier continuations

This is the published Apache-2.0 **25.12.19** crate with a narrow local verifier
extension. The registry archive checksum and every original, modified and added
source identity are recorded in `upstream-provenance.json`. The separate upstream
LICENSE is from the archive's recorded source commit. No dependency version,
binary format, ordinary verifier algorithm or default limit changes.

`TableVerifier::suspend` preserves a checked table descriptor while its child is
visited iteratively. The private, non-Clone continuation resumes only against
the same immutable buffer/options, at the pending depth and with cumulative
counts at least as large as when suspended. It performs no new byte reads and
does not reset or duplicate table/depth/apparent-byte accounting. Its eventual
`finish` remains the original single depth decrement. Misordered or mismatched
resume is a programmer error; an input error aborts the verifier as before.

The existing checked vector-range and trace helpers are exported unchanged so
native continuation walkers can preserve original field order, validation and
error contexts. The new surface adds no unsafe code or allocation. Consumers
own and admit their continuation storage. Existing recursive generated verifiers
continue to behave identically unless explicitly routed through a native walker.

Strict standalone lint retains six named style/API lint exceptions on only the
upstream modules that already trigger them: length without `is_empty`, redundant
casts, reversed-index arithmetic, unused/elidable lifetimes and unit-variant
patterns. These preserve the published API and untouched algorithms. New
continuation code and tests remain checked by all other strict lints. Standard
tests and the no-default-feature build are separate required CI checks.

The [RFC 0044 amendment](../../docs/rfcs/0044-resident-runtime-resource-ownership.md#physical-verifier-continuation-amendment-november-17-2026)
defines the complete acceptance boundary. Deep verification does not itself
establish logical schema, array, resource or user-workflow support. Remove this
patch when an approved upstream surface supplies the same ordered, checked
continuation contract and passes the native and compatibility acceptance suite.
