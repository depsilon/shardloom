//! Bounded schema accounting for execution payloads and physical native transfer.

use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use vortex::array::dtype::{DType, DecimalDType};

pub(crate) use crate::native_temporal::temporal_storage;

/// Borrow the existing owner for schema inspection. This creates no new pool or
/// global policy. Every visited node consumes credit before visiting children,
/// bounding repeated shared subtrees by the operation's actual allocation.
#[derive(Clone, Copy)]
pub(crate) struct Scope<'a> {
    memory: &'a LiveMemoryPool,
    cancellation: Option<&'a CancellationToken>,
}

#[cfg(feature = "vortex-local-primitives")]
impl<'a> Scope<'a> {
    pub(crate) fn memory(self) -> &'a LiveMemoryPool {
        self.memory
    }

    pub(crate) fn check_cancelled(self) -> Result<()> {
        self.cancellation.map_or(Ok(()), CancellationToken::check)
    }
}

impl<'a> From<&'a LiveMemoryPool> for Scope<'a> {
    fn from(memory: &'a LiveMemoryPool) -> Self {
        Self {
            memory,
            cancellation: None,
        }
    }
}

impl<'a> From<(&'a LiveMemoryPool, &'a CancellationToken)> for Scope<'a> {
    fn from((memory, cancellation): (&'a LiveMemoryPool, &'a CancellationToken)) -> Self {
        Self {
            memory,
            cancellation: Some(cancellation),
        }
    }
}

#[cfg(feature = "vortex-local-primitives")]
impl<'a> From<&'a crate::resident_session::NativeExecutionContext<'_>> for Scope<'a> {
    fn from(context: &'a crate::resident_session::NativeExecutionContext<'_>) -> Self {
        (context.memory(), context.cancellation()).into()
    }
}

pub(crate) fn admitted_decimal(dtype: DecimalDType) -> bool {
    (1..=38).contains(&dtype.precision())
        && dtype.scale() >= 0
        && i16::from(dtype.scale()) <= i16::from(dtype.precision())
}

/// Flat values admitted by native keys and compact retained scalar state.
pub(crate) fn admitted_scalar(dtype: &DType) -> bool {
    match dtype {
        DType::Bool(_) | DType::Utf8(_) | DType::Binary(_) | DType::Primitive(_, _) => true,
        DType::Decimal(decimal, _) => admitted_decimal(*decimal),
        DType::Extension(_) => temporal_storage(dtype).is_some(),
        _ => false,
    }
}

/// Admit flat schema containers, name copies and the temporary uniqueness index
/// before constructing them. Width consumes the caller's grant, not a fixed
/// column count. The allowance also covers native field/slot wrapper metadata.
pub(crate) fn reserve_names<'a>(
    memory: &LiveMemoryPool,
    count: usize,
    name_at: impl Fn(usize) -> &'a str,
) -> Result<MemoryLease> {
    let bytes = names_bytes(count, &name_at)?;
    let credit = memory.reserve(bytes)?;
    unique_names(count, name_at)?;
    Ok(credit)
}

/// Size name/container workspace without changing the caller's uniqueness policy.
pub(crate) fn names_bytes<'a>(count: usize, name_at: impl Fn(usize) -> &'a str) -> Result<u64> {
    if count == 0 {
        return Err(failed("schemas require at least one field"));
    }
    (0..count).try_fold(4096_u64, |bytes, index| {
        let name = name_at(index);
        if name.is_empty() {
            return Err(failed("field names must be nonempty and distinct"));
        }
        u64::try_from(name.len())
            .ok()
            .and_then(|len| len.checked_mul(4))
            .and_then(|len| len.checked_add(4096))
            .and_then(|field| bytes.checked_add(field))
            .ok_or_else(|| failed("schema metadata overflow"))
    })
}

// Call only while the corresponding names_bytes reservation is held.
fn unique_names<'a>(count: usize, name_at: impl Fn(usize) -> &'a str) -> Result<()> {
    let mut names = std::collections::BTreeSet::new();
    for index in 0..count {
        if !names.insert(name_at(index)) {
            return Err(failed("field names must be nonempty and distinct"));
        }
    }
    Ok(())
}

/// Admit complete schema containers and inspection workspace before traversal.
/// Depth remains a separate provider boundary; width follows the caller's grant.
/// Returned credit must outlive every native schema/container it admits.
pub(crate) fn reserve_schema<'a>(
    dtype: &DType,
    scope: impl Into<Scope<'a>>,
) -> Result<MemoryLease> {
    reserve_schema_with_null(dtype, scope.into(), false)
}

/// Physical transfer preserves Null leaves without admitting Null execution keys.
#[cfg(any(
    test,
    all(feature = "vortex-local-primitives", feature = "vortex-write", unix)
))]
pub(crate) fn reserve_encoded_schema<'a>(
    dtype: &DType,
    scope: impl Into<Scope<'a>>,
) -> Result<MemoryLease> {
    reserve_schema_with_null(dtype, scope.into(), true)
}

fn reserve_schema_with_null(
    dtype: &DType,
    scope: Scope<'_>,
    allow_null: bool,
) -> Result<MemoryLease> {
    let mut budget = Budget::new(scope)?;
    if let DType::Struct(fields, _) = dtype {
        budget.add(names_bytes(fields.nfields(), |index| {
            fields.names()[index].as_ref()
        })?)?;
        unique_names(fields.nfields(), |index| fields.names()[index].as_ref())?;
        for child in fields.try_field_dtypes() {
            visit_dtype(
                child.map_err(|error| provider_error(&error))?,
                0,
                &mut budget,
                allow_null,
            )?;
        }
    } else {
        visit_dtype(dtype, 0, &mut budget, allow_null)?;
    }
    Ok(budget.credit)
}

/// An admitted inspection estimate. Its temporary workspace is released before
/// returning; consumers that retain schema storage use `reserve_schema` instead.
pub(crate) fn schema_bytes<'a>(dtype: &DType, scope: impl Into<Scope<'a>>) -> Result<u64> {
    Ok(reserve_schema(dtype, scope)?.bytes())
}

/// The generic result serializer also describes types outside the execution
/// payload domain. Keep its depth contract and exact wire bound separate from
/// resource-admitted traversal scratch. Force lazy fields through fallible
/// inspection before the provider's infallible serialization accessors run.
#[cfg(feature = "vortex-local-primitives")]
pub(crate) fn reserve_serialization<'a>(
    dtype: &DType,
    scope: impl Into<Scope<'a>>,
) -> Result<MemoryLease> {
    fn visit(dtype: &DType, depth: usize, budget: &mut Budget<'_>) -> Result<()> {
        if depth > 64 {
            return Err(failed("result schema exceeds 64 nested levels"));
        }
        budget.add(1024)?;
        match dtype {
            DType::Struct(fields, _) => {
                for child in fields.try_field_dtypes() {
                    visit(
                        child.map_err(|error| provider_error(&error))?,
                        depth + 1,
                        budget,
                    )?;
                }
            }
            DType::Union(fields, _) => {
                for child in fields.variants() {
                    visit(&child, depth + 1, budget)?;
                }
            }
            DType::List(child, _) | DType::FixedSizeList(child, _, _) => {
                visit(child, depth + 1, budget)?;
            }
            DType::Map(map, _) => {
                visit(&map.key_dtype(), depth + 1, budget)?;
                visit(&map.value_dtype(), depth + 1, budget)?;
            }
            DType::Extension(extension) => visit(extension.storage_dtype(), depth + 1, budget)?,
            _ => {}
        }
        Ok(())
    }
    let mut budget = Budget::new(scope.into())?;
    visit(dtype, 0, &mut budget)?;
    Ok(budget.credit)
}

/// Account every physical schema node, including Null fields and nested elements.
/// Names and depth are validated; expanded work and storage consume the grant.
#[cfg(any(
    test,
    all(feature = "vortex-local-primitives", feature = "vortex-write", unix)
))]
pub(crate) fn encoded_schema_bytes<'a>(dtype: &DType, scope: impl Into<Scope<'a>>) -> Result<u64> {
    Ok(reserve_encoded_schema(dtype, scope)?.bytes())
}

/// Reserve before cloning an already-bound field list into a native `DType`.
pub(crate) fn reserve_fields<'a>(
    fields: &[(String, DType)],
    scope: impl Into<Scope<'a>>,
) -> Result<MemoryLease> {
    let mut budget = Budget::new(scope.into())?;
    budget.add(names_bytes(fields.len(), |index| fields[index].0.as_str())?)?;
    unique_names(fields.len(), |index| fields[index].0.as_str())?;
    for (_, dtype) in fields {
        visit_dtype(dtype, 0, &mut budget, false)?;
    }
    Ok(budget.credit)
}

struct Budget<'a> {
    credit: MemoryLease,
    cancellation: Option<&'a CancellationToken>,
}

impl<'a> Budget<'a> {
    fn new(scope: Scope<'a>) -> Result<Self> {
        if let Some(cancellation) = scope.cancellation {
            cancellation.check()?;
        }
        Ok(Self {
            credit: scope.memory.reserve(0)?,
            cancellation: scope.cancellation,
        })
    }

    fn node(&mut self, depth: usize) -> Result<()> {
        if depth > 24 {
            return Err(failed("nested payload schema exceeds depth 24"));
        }
        self.add(1024)
    }

    fn add(&mut self, bytes: u64) -> Result<()> {
        if let Some(cancellation) = self.cancellation {
            cancellation.check()?;
        }
        let bytes = self
            .credit
            .bytes()
            .checked_add(bytes)
            .ok_or_else(|| failed("payload metadata overflow"))?;
        self.credit.resize(bytes)
    }

    fn names<'name>(&mut self, count: usize, name_at: impl Fn(usize) -> &'name str) -> Result<()> {
        if count == 0 {
            return Err(failed("nested structs require at least one field"));
        }
        // Admit the uniqueness table before it allocates. Keep a linear lookup
        // even for wide structs; hashing references does not copy field names.
        self.add(
            u64::try_from(count)
                .ok()
                .and_then(|n| n.checked_mul(256))
                .ok_or_else(|| failed("schema name workspace overflow"))?,
        )?;
        let mut names = std::collections::HashSet::new();
        names
            .try_reserve(count)
            .map_err(|_| failed("schema name allocation failed"))?;
        for index in 0..count {
            let name = name_at(index);
            self.add(
                u64::try_from(name.len())
                    .ok()
                    .and_then(|n| n.checked_mul(2))
                    .ok_or_else(|| failed("schema name size overflow"))?,
            )?;
            if name.is_empty() || !names.insert(name) {
                return Err(failed(
                    "nested struct field names must be nonempty and distinct",
                ));
            }
        }
        Ok(())
    }
}

pub(crate) fn metadata_bytes<'a>(dtype: &DType, scope: impl Into<Scope<'a>>) -> Result<u64> {
    Ok(reserve_metadata(dtype, scope)?.bytes())
}

pub(crate) fn reserve_metadata<'a>(
    dtype: &DType,
    scope: impl Into<Scope<'a>>,
) -> Result<MemoryLease> {
    let mut budget = Budget::new(scope.into())?;
    visit_dtype(dtype, 0, &mut budget, false)?;
    Ok(budget.credit)
}

/// Route only unsupported domains elsewhere. Admission, cancellation, malformed
/// metadata and provider failures remain errors at the selected boundary.
pub(crate) fn supported<'a>(dtype: &DType, scope: impl Into<Scope<'a>>) -> Result<bool> {
    match reserve_metadata(dtype, scope) {
        Ok(_) => Ok(true),
        Err(ShardLoomError::NotImplemented(_)) => Ok(false),
        Err(error) => Err(error),
    }
}

fn provider_error(error: &vortex::error::VortexError) -> ShardLoomError {
    failed(&format!("provider metadata inspection failed: {error}"))
}

fn visit_dtype(
    dtype: &DType,
    depth: usize,
    budget: &mut Budget<'_>,
    allow_null: bool,
) -> Result<()> {
    budget.node(depth)?;
    match dtype {
        DType::Null if allow_null => {}
        DType::Bool(_) | DType::Utf8(_) | DType::Binary(_) | DType::Variant(_) => {}
        DType::Primitive(_, _) => {}
        DType::Decimal(decimal, _) if admitted_decimal(*decimal) => {}
        #[cfg(feature = "native-binary-objects")]
        DType::Extension(extension) if crate::native_binary_type::storage(dtype).is_some() => {
            budget.add(128)?;
            visit_dtype(extension.storage_dtype(), depth + 1, budget, allow_null)?;
        }
        DType::Extension(extension) if crate::native_embedding::storage(dtype).is_some() => {
            let metadata = extension
                .metadata_opt::<crate::native_embedding::Embedding>()
                .expect("admitted embedding metadata");
            budget.add(
                (metadata.as_str().len() as u64)
                    .checked_mul(8)
                    .ok_or_else(|| failed("embedding metadata size overflow"))?,
            )?;
            visit_dtype(extension.storage_dtype(), depth + 1, budget, allow_null)?;
        }
        DType::Extension(extension) if crate::native_tensor::storage(dtype).is_some() => {
            let metadata = extension
                .metadata_opt::<crate::native_tensor::FixedShapeTensor>()
                .expect("admitted tensor metadata");
            budget.add(
                (metadata.as_str().len() as u64)
                    .checked_mul(128)
                    .ok_or_else(|| failed("tensor metadata size overflow"))?,
            )?;
            visit_dtype(extension.storage_dtype(), depth + 1, budget, allow_null)?;
        }
        DType::Extension(extension) if crate::native_variable_tensor::storage(dtype).is_some() => {
            let metadata = extension
                .metadata_opt::<crate::native_variable_tensor::VariableShapeTensor>()
                .expect("admitted variable tensor metadata");
            budget.add(
                (metadata.as_str().len() as u64)
                    .checked_mul(128)
                    .ok_or_else(|| failed("tensor metadata size overflow"))?,
            )?;
            visit_dtype(extension.storage_dtype(), depth + 1, budget, allow_null)?;
        }
        DType::Extension(extension) if crate::native_vector::metadata(dtype).is_some() => {
            let metadata = crate::native_vector::metadata(dtype).expect("admitted vector metadata");
            budget.add(
                (metadata.as_str().len() as u64)
                    .checked_mul(128)
                    .ok_or_else(|| failed("vector metadata size overflow"))?,
            )?;
            visit_dtype(extension.storage_dtype(), depth + 1, budget, allow_null)?;
        }
        DType::Extension(extension) if temporal_storage(dtype).is_some() => {
            if let Some(metadata) =
                extension.metadata_opt::<vortex::array::extension::datetime::Timestamp>()
                && let Some(zone) = &metadata.tz
            {
                budget.add((zone.len() as u64).saturating_mul(2))?;
            }
        }
        DType::List(child, _) | DType::FixedSizeList(child, _, _) => {
            visit_dtype(child, depth + 1, budget, allow_null)?;
        }
        DType::Struct(fields, _) => {
            budget.names(fields.nfields(), |index| fields.names()[index].as_ref())?;
            for child in fields.try_field_dtypes() {
                visit_dtype(
                    child.map_err(|error| provider_error(&error))?,
                    depth + 1,
                    budget,
                    allow_null,
                )?;
            }
        }
        _ => return Err(unsupported()),
    }
    Ok(())
}

#[cfg(feature = "universal-format-io")]
pub(crate) fn arrow_field_metadata_admitted(field: &arrow_schema::Field) -> bool {
    match (
        field.metadata().get("ARROW:extension:name"),
        field.metadata().get("ARROW:extension:metadata"),
    ) {
        (None, None) => true,
        (Some(name), _) if name == crate::native_variant::arrow::ID => {
            crate::native_variant::arrow::admitted(field)
        }
        #[cfg(feature = "native-binary-objects")]
        (Some(name), _) if name == crate::native_binary_type::ID => {
            crate::native_binary_type::arrow::admitted(field)
        }
        (Some(name), _) if name == crate::native_embedding::ID => {
            crate::native_embedding::arrow::admitted(field)
        }
        (Some(name), _) if name == crate::native_tensor::ARROW_ID => {
            crate::native_tensor::arrow::admitted(field)
        }
        (Some(name), _) if name == crate::native_variable_tensor::ARROW_ID => {
            crate::native_variable_tensor::arrow::admitted(field)
        }
        (Some(name), _) if crate::native_vector::Kind::from_id(name).is_some() => {
            crate::native_vector::arrow::admitted(field)
        }
        (Some(name), metadata)
            if name == "shardloom.duration.v1"
                && matches!(field.data_type(), arrow_schema::DataType::Duration(_))
                && metadata.is_none_or(String::is_empty) =>
        {
            true
        }
        _ => false,
    }
}

/// Parsing/canonicalization workspace and retained copies of logical extension
/// metadata. Inspect lengths before allocation; import performs full validation.
#[cfg(feature = "universal-format-io")]
pub(crate) fn arrow_extension_metadata_bytes(field: &arrow_schema::Field) -> Result<u64> {
    if !arrow_field_metadata_admitted(field) {
        return Err(unsupported());
    }
    field
        .metadata()
        .get("ARROW:extension:metadata")
        .map_or(Ok(0), |raw| {
            u64::try_from(raw.len())
                .ok()
                .and_then(|n| n.checked_mul(128))
                .ok_or_else(|| failed("Arrow extension metadata size overflow"))
        })
}

#[cfg(feature = "universal-format-io")]
pub(crate) fn arrow_metadata_bytes<'a>(
    dtype: &arrow_schema::DataType,
    scope: impl Into<Scope<'a>>,
) -> Result<u64> {
    let mut budget = Budget::new(scope.into())?;
    visit_arrow(dtype, 0, &mut budget)?;
    Ok(budget.credit.bytes())
}

/// Variant metadata on an outer dictionary Field describes its values. Keep
/// this scoped to admitted Variant fields instead of broadening encoded input.
#[cfg(feature = "universal-format-io")]
pub(crate) fn arrow_variant_storage_bytes<'a>(
    dtype: &arrow_schema::DataType,
    scope: impl Into<Scope<'a>>,
) -> Result<u64> {
    let mut budget = Budget::new(scope.into())?;
    visit_arrow_variant(dtype, 0, &mut budget)?;
    Ok(budget.credit.bytes())
}

#[cfg(feature = "universal-format-io")]
pub(crate) fn validate_arrow_variant_declaration(dtype: &arrow_schema::DataType) -> Result<()> {
    visit_arrow_variant(dtype, 0, &mut DeclarationBudget::default())
}

/// Classify inert caller-owned Arrow metadata when there is no execution grant.
/// This produces no size estimate or native owner. Memoization bounds shared
/// graph inspection by its actual nodes and the retained depth boundary, while
/// fallible scratch growth reports allocation failure. Execution uses the
/// resource-admitted expanded walk above, even for a previously classified type.
#[cfg(feature = "universal-format-io")]
pub(crate) fn validate_arrow_declaration(dtype: &arrow_schema::DataType) -> Result<()> {
    visit_arrow(dtype, 0, &mut DeclarationBudget::default())
}

#[cfg(feature = "universal-format-io")]
trait ArrowWalkBudget {
    fn node(&mut self, dtype: &arrow_schema::DataType, depth: usize) -> Result<bool>;
    fn add(&mut self, bytes: u64) -> Result<()>;
    fn names<'a>(&mut self, count: usize, name_at: impl Fn(usize) -> &'a str) -> Result<()>;
}

#[cfg(feature = "universal-format-io")]
impl ArrowWalkBudget for Budget<'_> {
    fn node(&mut self, _: &arrow_schema::DataType, depth: usize) -> Result<bool> {
        Budget::node(self, depth)?;
        Ok(true)
    }
    fn add(&mut self, bytes: u64) -> Result<()> {
        Budget::add(self, bytes)
    }
    fn names<'a>(&mut self, count: usize, name_at: impl Fn(usize) -> &'a str) -> Result<()> {
        Budget::names(self, count, name_at)
    }
}

#[cfg(feature = "universal-format-io")]
#[derive(Default)]
struct DeclarationBudget {
    seen: std::collections::HashSet<(*const arrow_schema::DataType, usize)>,
}

#[cfg(feature = "universal-format-io")]
impl ArrowWalkBudget for DeclarationBudget {
    fn node(&mut self, dtype: &arrow_schema::DataType, depth: usize) -> Result<bool> {
        if depth > 24 {
            return Err(failed("nested payload schema exceeds depth 24"));
        }
        let key = (std::ptr::from_ref(dtype), depth);
        if self.seen.contains(&key) {
            return Ok(false);
        }
        self.seen
            .try_reserve(1)
            .map_err(|_| failed("schema inspection allocation failed"))?;
        Ok(self.seen.insert(key))
    }
    fn add(&mut self, _: u64) -> Result<()> {
        Ok(())
    }
    fn names<'a>(&mut self, count: usize, name_at: impl Fn(usize) -> &'a str) -> Result<()> {
        if count == 0 {
            return Err(failed("nested structs require at least one field"));
        }
        let mut names = std::collections::HashSet::new();
        names
            .try_reserve(count)
            .map_err(|_| failed("schema name allocation failed"))?;
        for index in 0..count {
            let name = name_at(index);
            if name.is_empty() || !names.insert(name) {
                return Err(failed(
                    "nested struct field names must be nonempty and distinct",
                ));
            }
        }
        Ok(())
    }
}

#[cfg(feature = "universal-format-io")]
fn visit_arrow_field(
    child: &arrow_schema::Field,
    depth: usize,
    budget: &mut impl ArrowWalkBudget,
) -> Result<()> {
    budget.add(arrow_extension_metadata_bytes(child)?)?;
    if crate::native_variant::arrow::is_variant(child) {
        return visit_arrow_variant(child.data_type(), depth, budget);
    }
    visit_arrow(child.data_type(), depth, budget)
}

#[cfg(feature = "universal-format-io")]
fn visit_arrow_variant(
    dtype: &arrow_schema::DataType,
    depth: usize,
    budget: &mut impl ArrowWalkBudget,
) -> Result<()> {
    if let arrow_schema::DataType::Dictionary(keys, values) = dtype {
        if !budget.node(dtype, depth)? {
            return Ok(());
        }
        visit_arrow(keys, depth + 1, budget)?;
        visit_arrow_variant(values, depth + 1, budget)
    } else {
        visit_arrow(dtype, depth, budget)
    }
}

#[cfg(feature = "universal-format-io")]
fn visit_arrow(
    dtype: &arrow_schema::DataType,
    depth: usize,
    budget: &mut impl ArrowWalkBudget,
) -> Result<()> {
    use arrow_schema::DataType as A;
    if !budget.node(dtype, depth)? {
        return Ok(());
    }
    match dtype {
        A::Boolean
        | A::Int8
        | A::Int16
        | A::Int32
        | A::Int64
        | A::UInt8
        | A::UInt16
        | A::UInt32
        | A::UInt64
        | A::Float16
        | A::Float32
        | A::Float64
        | A::Utf8
        | A::LargeUtf8
        | A::Utf8View
        | A::Binary
        | A::LargeBinary
        | A::BinaryView => {}
        dtype if crate::native_temporal::from_arrow_dtype(dtype).is_some() => {}
        A::Decimal128(precision, scale)
            if (1..=38).contains(precision)
                && *scale >= 0
                && i16::from(*scale) <= i16::from(*precision) => {}
        #[cfg(feature = "native-encoded-exchange-experiment")]
        A::Dictionary(keys, values)
            if matches!(
                keys.as_ref(),
                A::Int8
                    | A::Int16
                    | A::Int32
                    | A::Int64
                    | A::UInt8
                    | A::UInt16
                    | A::UInt32
                    | A::UInt64
            ) && encoded_arrow_leaf(values) =>
        {
            visit_arrow(keys, depth + 1, budget)?;
            visit_arrow(values, depth + 1, budget)?;
        }
        #[cfg(feature = "native-encoded-exchange-experiment")]
        A::RunEndEncoded(ends, values)
            if !ends.is_nullable()
                && matches!(ends.data_type(), A::Int16 | A::Int32 | A::Int64)
                && encoded_arrow_leaf(values.data_type()) =>
        {
            visit_arrow_field(ends, depth + 1, budget)?;
            visit_arrow_field(values, depth + 1, budget)?;
        }
        A::List(child) | A::LargeList(child) => {
            visit_arrow_field(child, depth + 1, budget)?;
        }
        A::FixedSizeList(child, size) if *size >= 0 => {
            visit_arrow_field(child, depth + 1, budget)?;
        }
        A::Struct(fields) => {
            budget.names(fields.len(), |index| fields[index].name().as_str())?;
            for child in fields {
                visit_arrow_field(child, depth + 1, budget)?;
            }
        }
        _ => return Err(unsupported()),
    }
    Ok(())
}

/// File readers retain the existing ingest dictionary admission, independently
/// of the experimental caller-owned encoded Arrow transport boundary.
#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
pub(crate) fn arrow_file_metadata_bytes<'a>(
    dtype: &arrow_schema::DataType,
    scope: impl Into<Scope<'a>>,
) -> Result<u64> {
    let scope = scope.into();
    if let arrow_schema::DataType::Dictionary(keys, values) = dtype {
        if crate::vortex_ingest::dictionary_column_family(dtype).is_none() {
            return Err(unsupported());
        }
        // The ingest contract permits scalar dictionaries only at the top
        // level. Nested layouts still use the ordinary bounded schema visitor.
        return arrow_metadata_bytes(keys, scope)?
            .checked_add(arrow_metadata_bytes(values, scope)?)
            .and_then(|bytes| bytes.checked_add(1024))
            .ok_or_else(|| failed("columnar schema metadata overflow"));
    }
    arrow_metadata_bytes(dtype, scope)
}

// The experimental transport control does not broaden the logical-value
// validator. These leaves have no finite-float or decimal-precision condition;
// Arrow's full validation checks their buffers before the owned copy/import.
#[cfg(feature = "native-encoded-exchange-experiment")]
fn encoded_arrow_leaf(dtype: &arrow_schema::DataType) -> bool {
    use arrow_schema::DataType as A;
    matches!(
        dtype,
        A::Boolean
            | A::Int8
            | A::Int16
            | A::Int32
            | A::Int64
            | A::UInt8
            | A::UInt16
            | A::UInt32
            | A::UInt64
            | A::Utf8
            | A::LargeUtf8
            | A::Utf8View
            | A::Binary
            | A::LargeBinary
            | A::BinaryView
    )
}

fn unsupported() -> ShardLoomError {
    ShardLoomError::NotImplemented(
        "native payload schema: payload requires bool, integer, F16/F32/F64, UTF8, binary, decimal128(precision 1..38, scale 0..precision), admitted date/time/timestamp/duration metadata or static list/struct fields; no fallback execution was attempted".into(),
    )
}

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native payload schema: {reason}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex::array::dtype::{Nullability, StructFields};

    #[test]
    fn encoded_null_schemas_do_not_admit_execution_payloads() {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        for dtype in [
            DType::Null,
            DType::List(DType::Null.into(), Nullability::NonNullable),
            DType::FixedSizeList(DType::Null.into(), 2, Nullability::Nullable),
            DType::Struct(
                StructFields::from_iter([("null", DType::Null)]),
                Nullability::NonNullable,
            ),
        ] {
            assert!(metadata_bytes(&dtype, &memory).is_err());
            assert!(schema_bytes(&dtype, &memory).is_err());
            assert!(reserve_schema(&dtype, &memory).is_err());
            let credit = reserve_encoded_schema(&dtype, &memory).unwrap();
            assert!(credit.bytes() >= 1024);
            assert_eq!(
                credit.bytes(),
                encoded_schema_bytes(&dtype, &memory).unwrap()
            );
            drop(credit);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
        assert!(!admitted_scalar(&DType::Null));
    }

    #[test]
    fn encoded_null_schemas_keep_depth_and_name_boundaries_with_admitted_width() {
        let memory = LiveMemoryPool::new(16 << 20).unwrap();
        let mut dtype = DType::Null;
        for _ in 0..24 {
            dtype = DType::List(dtype.into(), Nullability::NonNullable);
        }
        assert!(encoded_schema_bytes(&dtype, &memory).is_ok());
        dtype = DType::List(dtype.into(), Nullability::NonNullable);
        assert!(encoded_schema_bytes(&dtype, &memory).is_err());
        let duplicate = DType::Struct(
            StructFields::from_iter([("same", DType::Null), ("same", DType::Null)]),
            Nullability::NonNullable,
        );
        assert!(reserve_encoded_schema(&duplicate, &memory).is_err());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        let nested = DType::List(duplicate.into(), Nullability::NonNullable);
        assert!(encoded_schema_bytes(&nested, &memory).is_err());
        let wide = DType::Struct(
            StructFields::from_iter((0..1025).map(|index| (format!("n{index}"), DType::Null))),
            Nullability::NonNullable,
        );
        assert!(encoded_schema_bytes(&wide, &memory).is_ok());
        assert!(
            encoded_schema_bytes(&DType::List(wide.into(), Nullability::NonNullable), &memory)
                .is_ok()
        );
    }
}

#[cfg(test)]
#[path = "native_payload_schema_tests.rs"]
mod resource_tests;
