# ShardLoom Arrow JSON schema ownership compatibility

This provider uses the Apache-2.0 `arrow-json` 59.3.2 registry source. The
provenance records the exact registry archive, every original archive
member, and byte-identical license and notice aliases. `Cargo.lock` is omitted
from the committed provider source; workspace and reviewed standalone locks own
dependency resolution. `publish = false` keeps the local patch unpublished.

JSON schema coercion matches borrow
the input `DataType` values and return the selected values without moving
fields out of the type. This preserves compatibility with the same-version
schema provider's recursive ownership cleanup. No JSON format behavior or
runtime capability expansion is claimed here. The invalid UTF-16 diagnostic
also removes an ineffective hexadecimal width specifier; its text is unchanged
and the full library remains checked with strict current Clippy.

The standalone provider configuration checks the existing library tests and
strict lint against the same local schema, array and descriptor graph. This
checkpoint preserves public nesting guards and makes no performance claim.

The registry archive omits nine JSON test fixtures. They are preserved from
the Arrow Rust 59.3.2 tag at commit
`9ea30dfacb7003de9ffc87ef50231f18229fcdb5`, whose reader/schema/writer test source
matches the registry archive byte-for-byte. Provenance records every Git blob
and SHA-256. The repository's fixture license and notice are preserved separately;
the notice also has a `LICENSE-` alias for the existing package legal-text bundle.
