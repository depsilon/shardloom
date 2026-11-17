// Frozen original classification predicates from e621a6d94; test-only.
use vortex::array::dtype::DType;

pub(super) mod variant {
    use super::DType;

    pub(crate) fn contains(dtype: &DType) -> bool {
        match dtype {
            DType::Variant(_) => true,
            DType::List(child, _) | DType::FixedSizeList(child, _, _) => contains(child),
            DType::Struct(fields, _) => fields.fields().any(|child| contains(&child)),
            _ => false,
        }
    }
}

pub(super) mod vector {
    use super::DType;
    use crate::native_vector::storage;

    pub(crate) fn contains(dtype: &DType) -> bool {
        match dtype {
            DType::Extension(_) => storage(dtype).is_some(),
            DType::List(child, _) | DType::FixedSizeList(child, _, _) => contains(child),
            DType::Struct(fields, _) => fields.fields().any(|child| contains(&child)),
            _ => false,
        }
    }
}

pub(super) mod tensor {
    use super::DType;
    use crate::native_tensor::storage;

    pub(crate) fn contains(dtype: &DType) -> bool {
        match dtype {
            DType::Extension(_) => storage(dtype).is_some(),
            DType::List(child, _) | DType::FixedSizeList(child, _, _) => contains(child),
            DType::Struct(fields, _) => fields.fields().any(|child| contains(&child)),
            _ => false,
        }
    }
}

pub(super) mod variable_tensor {
    use super::DType;
    use crate::native_variable_tensor::storage;

    pub(crate) fn contains(dtype: &DType) -> bool {
        match dtype {
            DType::Extension(_) => storage(dtype).is_some(),
            DType::List(child, _) | DType::FixedSizeList(child, _, _) => contains(child),
            DType::Struct(fields, _) => fields.fields().any(|child| contains(&child)),
            _ => false,
        }
    }
}

pub(super) mod embedding {
    use super::DType;
    use crate::native_embedding::storage;

    pub(crate) fn contains(dtype: &DType) -> bool {
        match dtype {
            DType::Extension(_) => storage(dtype).is_some(),
            DType::List(child, _) | DType::FixedSizeList(child, _, _) => contains(child),
            DType::Struct(fields, _) => fields.fields().any(|child| contains(&child)),
            _ => false,
        }
    }
}

pub(super) mod float16 {
    use super::DType;

    pub(crate) fn contains_float16(dtype: &DType) -> bool {
        match dtype {
            DType::Primitive(vortex::array::dtype::PType::F16, _) => true,
            DType::List(child, _) | DType::FixedSizeList(child, _, _) => contains_float16(child),
            DType::Struct(fields, _) => fields.fields().any(|child| contains_float16(&child)),
            _ => false,
        }
    }
}
