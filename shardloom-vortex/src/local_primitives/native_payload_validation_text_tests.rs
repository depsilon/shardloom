//! Discarding a payload must not hide invalid text in an observed parent row.

use super::validation_tests::{check_copy_agreement, record};
use super::*;
use vortex::array::{
    ArrayParts,
    arrays::{VarBin, VarBinArray},
};

#[test]
fn payload_validation_preserves_utf8_error_coordinates_and_hidden_values() {
    let binary = VarBinArray::from(vec![&b"\xff"[..], &b"visible"[..], &b"\xfe"[..]]);
    let slots = binary.slots().iter().cloned().collect();
    let text = VarBinArray::try_from_parts(ArrayParts::new(
        VarBin,
        DType::Utf8(Nullability::NonNullable),
        3,
        binary.into_data(),
        slots,
    ))
    .unwrap()
    .into_array();
    let hidden = record(
        vec![("text", text.clone())],
        Validity::from_iter([false, true, false]),
    );
    check_copy_agreement(&hidden, None);
    let visible = record(
        vec![("text", text.clone())],
        Validity::from_iter([false, true, true]),
    );
    check_copy_agreement(&visible, Some("utf-8"));
    let nullable = vortex::array::arrays::MaskedArray::try_new(
        text,
        Validity::AllValid,
        &mut vortex::array::array_session().create_execution_ctx(),
    )
    .unwrap()
    .into_array();
    for visible in [false, true] {
        let parent = record(
            vec![("text", nullable.clone())],
            Validity::from_iter([false, true, visible]),
        );
        check_copy_agreement(&parent, visible.then_some("utf-8"));
    }
}
