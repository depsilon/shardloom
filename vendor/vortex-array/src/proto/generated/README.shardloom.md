# Root protobuf trait overrides

The generated wire schema, field types and oneofs are unchanged. The `DType` and
`ScalarValue` root structs alone use `#[derive(Default)]` in place of generated
Clone, PartialEq and Message derivation, with no root-field Prost attributes.
Their structural implementations live in `../structural.rs`, their Message
implementations in `../wire.rs`, ordered merge continuations in `../decode.rs`,
and intrinsic cleanup in `../owned_release.rs`.
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
the original primitive codecs, error contexts and partial-merge rules through
ordered owned continuations. Original boxes are reused; interrupted buffers
restore partial state without replaying reads. Checked Prost continuations
preserve supplied depth bounds and delimiter errors, including the unchanged
default decoding limit. Explicit deep provider tests are not native allocation
grants or public deep-workflow acceptance.
