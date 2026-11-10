//! Canonical Arrow declaration checks; conversion uses Vortex's UUID provider.

use arrow_schema::{DataType, Field};

pub(crate) const ID: &str = "arrow.uuid";

pub(crate) fn is_uuid(field: &Field) -> bool {
    field
        .metadata()
        .get("ARROW:extension:name")
        .is_some_and(|name| name == ID)
}

pub(crate) fn admitted(field: &Field) -> bool {
    let mut dtype = field.data_type();
    let mut depth = 0;
    while let DataType::Dictionary(keys, values) = dtype {
        depth += 1;
        if depth > 24
            || !matches!(
                keys.as_ref(),
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
            )
        {
            return false;
        }
        dtype = values;
    }
    is_uuid(field)
        && field
            .metadata()
            .get("ARROW:extension:metadata")
            .is_none_or(String::is_empty)
        && *dtype == DataType::FixedSizeBinary(16)
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
pub(crate) fn field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::FixedSizeBinary(16), nullable).with_metadata(
        [
            ("ARROW:extension:name".into(), ID.into()),
            ("ARROW:extension:metadata".into(), String::new()),
        ]
        .into(),
    )
}
