# Root protobuf trait overrides

The generated wire schema, field types and oneofs are unchanged. The `DType` and
`ScalarValue` root structs alone use `#[derive(Default)]` in place of generated
Clone, PartialEq and Message derivation, with no root-field Prost attributes.
Their structural implementations live in `../structural.rs`, their Message
implementations in `../wire.rs`, and intrinsic cleanup in `../owned_release.rs`.
Every recursive generated model cycle crosses one of those two roots. Other
generated messages and oneofs keep all their original derives and attributes.

This vendored subset does not include the upstream xtask. When regenerating in
the complete upstream source with `cargo run -p xtask -- generate-proto`, reapply
exactly those two root derive replacements and remove only their root-field
Prost attributes after copying the generated files here. Do not change model
fields, other Message derives or oneof attributes.
Review any added variants against the exhaustive traversal matches. Run the
provider structural reference tests, complete provider tests/lint, native
workspace gates and provenance/license checks before accepting regenerated code.

The immutable `../reference/` files are original generated source from ShardLoom
commit `59ffad352`. They are test-only compatibility oracles and must not be
rewritten to match the new implementation. Their source identity is checked in
the source-reference evidence. The handwritten Message implementations retain
the original generated oneof merge, error context and unknown-field skipping,
including the unchanged default decoding limit.
