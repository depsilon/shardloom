# ShardLoom Arrow row schema ownership compatibility

This provider uses the Apache-2.0 `arrow-row` 59.3.2 registry source. The
provenance records the exact registry archive, every original archive
member, and byte-identical license and notice aliases. `Cargo.lock` is omitted
from the committed provider source; workspace and reviewed standalone locks own
dependency resolution. `publish = false` keeps the local patch unpublished.

List and map row construction matches
borrow the input `DataType`, clone the existing field references, and copy the
map ordering flag before calling the established constructors. This preserves
compatibility with the same-version schema provider's recursive ownership
cleanup. A redundant test import is removed for strict current Clippy. No row
encoding behavior or runtime capability expansion is claimed.

The standalone provider configuration checks the existing library tests and
strict lint against the same local schema, array and descriptor graph. This
checkpoint preserves public nesting guards and makes no performance claim.
