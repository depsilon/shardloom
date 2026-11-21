# ShardLoom Arrow schema ownership checkpoint

This provider uses the Apache-2.0 `arrow-schema` 59.3.2 registry source. The
provenance records the exact registry archive, every original archive
member, and byte-identical license and notice aliases. `Cargo.lock` is omitted
from the committed provider source; workspace and reviewed standalone locks own
dependency resolution. `publish = false` keeps the local patch unpublished.

The local patch gives `DataType` explicit clone, comparison, ordering, hashing,
formatting, sizing, and release paths that can traverse deeply nested types
iteratively. `Fields` and `UnionFields` keep their public variant and sequence
serialization behavior while storing fields in shared containers that can
retain opaque owners through final release. The local additions are limited to
this schema ownership and traversal work and the narrow adapter migrations it
requires. They do not claim that arbitrary downstream Rust programs can adopt
a changed destructor contract without review.

Frozen shallow references and deep small-stack tests cover the stated ownership
and structural operations. Complete user-workflow admission, native conversion,
FFI/serde traversal and caller memory grants remain separate deferred work. This
checkpoint preserves public nesting guards and makes no performance claim.
