//! Selected coordinate/offset views. No decoded geometry collection is built.

use super::{NativeNumericOwner, native_list, result_batch, vortex_error};
use crate::{
    native_spatial::{
        Kind,
        values::{failed, point, sequence},
    },
    resident_session::NativeExecutionContext,
};
use shardloom_core::{FloatPolicy, Result};
use shardloom_exec::live_memory::MemoryLease;
use vortex::{
    array::{
        ArrayRef, VortexSessionExecute as _,
        arrays::{
            ExtensionArray, PrimitiveArray, StructArray, extension::ExtensionArrayExt as _,
            struct_::StructArrayExt as _,
        },
        dtype::DType,
    },
    mask::Mask,
};

fn credit(rows: usize, width: usize, context: &NativeExecutionContext<'_>) -> Result<MemoryLease> {
    let bytes = rows
        .checked_mul(width)
        .and_then(|n| n.checked_add(4096))
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(|| failed("spatial view admission overflow"))?;
    context.memory().reserve(bytes)
}
pub(super) fn check(index: usize, context: &NativeExecutionContext<'_>) -> Result<()> {
    if index.is_multiple_of(1024) {
        context.check_cancelled()?;
    }
    Ok(())
}

pub(super) struct Coordinates {
    x: NativeNumericOwner,
    y: NativeNumericOwner,
    valid: Mask,
    count: usize,
    // Native owners are released before the selected expansion allowance.
    _credit: MemoryLease,
}
impl Coordinates {
    pub(super) fn new(
        array: &ArrayRef,
        start: usize,
        count: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        context.check_cancelled()?;
        let end = start
            .checked_add(count)
            .filter(|end| *end <= array.len())
            .ok_or_else(|| failed("spatial coordinate range exceeds child values"))?;
        let lease = credit(count, 32, context)?;
        let mut execution = context.native_session().create_execution_ctx();
        let source = array.slice(start..end).map_err(vortex_error)?;
        let values = source
            .clone()
            .execute::<StructArray>(&mut execution)
            .map_err(vortex_error)?;
        if values.len() != count || values.dtype() != source.dtype() {
            return Err(failed("spatial coordinate provider changed its domain"));
        }
        let valid = values
            .struct_validity()
            .execute_mask(count, &mut execution)
            .map_err(vortex_error)?;
        let mut axis = |index| {
            let source = values.unmasked_field(index);
            let primitive = source
                .clone()
                .execute::<PrimitiveArray>(&mut execution)
                .map_err(vortex_error)?;
            if primitive.len() != count || primitive.dtype() != source.dtype() {
                return Err(failed("spatial numeric provider changed its domain"));
            }
            NativeNumericOwner::new_with_float_policy(
                primitive,
                &mut execution,
                FloatPolicy::Extended,
            )
        };
        Ok(Self {
            x: axis(0)?,
            y: axis(1)?,
            valid,
            count,
            _credit: lease,
        })
    }
    pub(super) fn len(&self) -> usize {
        self.count
    }
    pub(super) fn at(&self, index: usize) -> Result<(f64, f64)> {
        if index >= self.count || !self.valid.value(index) {
            return Err(failed("spatial coordinates must be nonnull"));
        }
        self.x
            .numeric_value(index)?
            .zip(self.y.numeric_value(index)?)
            .ok_or_else(|| failed("spatial coordinates must be nonnull"))
    }
    pub(super) fn validate(&self, ring: bool, context: &NativeExecutionContext<'_>) -> Result<()> {
        sequence(self.count, ring, |index| {
            check(index, context)?;
            self.at(index)
        })
    }
}

pub(super) struct List {
    elements: ArrayRef,
    coordinates: native_list::Coordinates,
    count: usize,
    _credit: MemoryLease,
}
impl List {
    fn new(array: ArrayRef, context: &NativeExecutionContext<'_>) -> Result<Self> {
        context.check_cancelled()?;
        let count = array.len();
        let lease = credit(count, 64, context)?;
        let mut execution = context.native_session().create_execution_ctx();
        let (elements, coordinates) =
            native_list::Column::new(array, &mut execution)?.into_indexed(&mut execution)?;
        Ok(Self {
            elements,
            coordinates,
            count,
            _credit: lease,
        })
    }
    fn at(&self, row: usize) -> Result<(usize, usize)> {
        self.coordinates
            .at(row)?
            .ok_or_else(|| failed("present geometry requires nonnull rings and vertices"))
    }
    pub(super) fn len(&self) -> usize {
        self.count
    }
    pub(super) fn ring(
        &self,
        index: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Coordinates> {
        let (start, count) = self.at(index)?;
        Coordinates::new(&self.elements, start, count, context)
    }
}

pub(super) struct Column {
    storage: ArrayRef,
    valid: Mask,
    offsets: Option<List>,
    kind: Kind,
    _credit: MemoryLease,
}
impl Column {
    pub(super) fn new(array: &ArrayRef, context: &NativeExecutionContext<'_>) -> Result<Self> {
        let metadata = crate::native_spatial::metadata(array.dtype())
            .ok_or_else(|| failed("spatial expression requires a geometry dtype"))?;
        let DType::Extension(declared) = array.dtype() else {
            unreachable!("spatial extension");
        };
        context.check_cancelled()?;
        let mut execution = context.native_session().create_execution_ctx();
        let extension = array
            .clone()
            .execute::<ExtensionArray>(&mut execution)
            .map_err(vortex_error)?;
        let storage = extension.storage_array();
        if storage.len() != array.len() || storage.dtype() != declared.storage_dtype() {
            return Err(failed(
                "spatial extension provider changed its storage domain",
            ));
        }
        Self::from_storage(storage.clone(), metadata.kind(), context)
    }
    pub(super) fn from_storage(
        storage: ArrayRef,
        kind: Kind,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let lease = credit(storage.len(), 8, context)?;
        let mut execution = context.native_session().create_execution_ctx();
        let valid = storage
            .validity()
            .map_err(vortex_error)?
            .execute_mask(storage.len(), &mut execution)
            .map_err(vortex_error)?;
        let offsets = (kind != Kind::Point)
            .then(|| List::new(storage.clone(), context))
            .transpose()?;
        Ok(Self {
            storage,
            valid,
            offsets,
            kind,
            _credit: lease,
        })
    }
    pub(super) fn row(&self, row: usize, context: &NativeExecutionContext<'_>) -> Result<Row> {
        check(row, context)?;
        if row >= self.storage.len() {
            return Err(failed("spatial row exceeds its source"));
        }
        if !self.valid.value(row) {
            return Ok(Row::Null);
        }
        match self.kind {
            Kind::Point => Ok(Row::Point(Coordinates::new(
                &self.storage,
                row,
                1,
                context,
            )?)),
            Kind::LineString => {
                let offsets = self.offsets.as_ref().expect("line offsets");
                let (start, count) = offsets.at(row)?;
                Ok(Row::Line(Coordinates::new(
                    &offsets.elements,
                    start,
                    count,
                    context,
                )?))
            }
            Kind::Polygon => {
                let offsets = self.offsets.as_ref().expect("polygon offsets");
                let (start, count) = offsets.at(row)?;
                let end = start
                    .checked_add(count)
                    .filter(|end| *end <= offsets.elements.len())
                    .ok_or_else(|| failed("polygon offsets exceed rings"))?;
                Ok(Row::Polygon(List::new(
                    offsets.elements.slice(start..end).map_err(vortex_error)?,
                    context,
                )?))
            }
        }
    }
}

/// Bounded native views for one selected row. Coordinate buffers stay in their
/// primitive owners; there is no Vec<Point> or geometry-library conversion.
pub(super) enum Row {
    Null,
    Point(Coordinates),
    Line(Coordinates),
    Polygon(List),
}
impl Row {
    pub(super) fn validate(&self, context: &NativeExecutionContext<'_>) -> Result<()> {
        match self {
            Self::Null => {}
            Self::Point(values) => {
                let (x, y) = values.at(0)?;
                point(x, y, true)?;
            }
            Self::Line(values) => values.validate(false, context)?,
            Self::Polygon(rings) => {
                for ring in 0..rings.len() {
                    check(ring, context)?;
                    rings.ring(ring, context)?.validate(true, context)?;
                }
            }
        }
        context.check_cancelled()
    }
    pub(super) fn point(&self) -> Result<Option<(f64, f64)>> {
        match self {
            Self::Null => Ok(None),
            Self::Point(values) => {
                let (x, y) = values.at(0)?;
                Ok((!point(x, y, true)?).then_some((x, y)))
            }
            _ => Err(failed("operation requires Point geometry")),
        }
    }
    pub(super) fn is_empty(&self) -> Result<Option<bool>> {
        Ok(match self {
            Self::Null => None,
            Self::Point(values) => {
                let (x, y) = values.at(0)?;
                Some(point(x, y, true)?)
            }
            Self::Line(values) => Some(values.len() == 0),
            Self::Polygon(rings) => Some(rings.len() == 0),
        })
    }
    pub(super) fn visit(
        &self,
        context: &NativeExecutionContext<'_>,
        mut visit: impl FnMut(f64, f64) -> Result<()>,
    ) -> Result<()> {
        let mut vertices = |values: &Coordinates| -> Result<()> {
            for index in 0..values.len() {
                check(index, context)?;
                let (x, y) = values.at(index)?;
                visit(x, y)?;
            }
            Ok(())
        };
        match self {
            Self::Null => {}
            Self::Point(_) if self.is_empty()? == Some(true) => {}
            Self::Point(values) | Self::Line(values) => vertices(values)?,
            Self::Polygon(rings) => {
                for index in 0..rings.len() {
                    check(index, context)?;
                    vertices(&rings.ring(index, context)?)?;
                }
            }
        }
        Ok(())
    }
}

pub(super) fn validate_selection(
    source: Option<&ArrayRef>,
    rows: &[Option<usize>],
    kind: Kind,
    context: &NativeExecutionContext<'_>,
    allocator: &vortex::array::memory::HostAllocatorRef,
) -> Result<()> {
    use super::result_batch::Value;
    use vortex::array::dtype::{Nullability, PType};
    context.check_cancelled()?;
    if !rows.iter().any(Option::is_some) {
        return Ok(());
    }
    let source = source.ok_or_else(|| failed("spatial source is absent"))?;
    let indices = result_batch::build_column(
        &DType::Primitive(
            PType::U64,
            Nullability::from(rows.iter().any(Option::is_none)),
        ),
        rows.len(),
        allocator,
        |row| {
            check(row, context)?;
            Ok(rows[row].map_or(Value::Null, |index| Value::UInt(index as u64)))
        },
    )?;
    let column = Column::from_storage(source.take(indices).map_err(vortex_error)?, kind, context)?;
    for index in 0..rows.len() {
        column.row(index, context)?.validate(context)?;
    }
    context.check_cancelled()
}
