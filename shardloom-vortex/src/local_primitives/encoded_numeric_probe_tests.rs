//! The optional encoded probe must not execute an unsupported child merely to
//! discover its shape. This test encoding counts its real `VTable` executions.

use super::{NativeNumericAccessorWork, SimpleAggregateStates, update};
use crate::{VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest};
use shardloom_core::ColumnRef;
use std::{
    fmt::{Display, Formatter},
    hash::Hasher,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use vortex::{
    VortexSessionDefault as _,
    array::{
        Array, ArrayEq, ArrayHash, ArrayId, ArrayParts, ArrayRef, ArrayView, EqMode, ExecutionCtx,
        ExecutionResult, IntoArray as _, NotSupported, VTable, ValidityVTable,
        VortexSessionExecute as _,
        arrays::{PrimitiveArray, SliceArray, StructArray},
        buffer::BufferHandle,
        dtype::{DType, FieldNames},
        serde::ArrayChildren,
        validity::Validity,
        with_empty_buffers,
    },
    error::{VortexResult, vortex_err},
    expr::{root, select},
    session::{VortexSession, registry::CachedId},
};

#[derive(Clone, Debug)]
struct ObservedNative;

#[derive(Clone, Debug)]
struct Observation {
    calls: Arc<AtomicUsize>,
}

impl Display for Observation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("observed-native-test-child")
    }
}
impl ArrayHash for Observation {
    fn array_hash<H: Hasher>(&self, state: &mut H, _: EqMode) {
        std::ptr::hash(Arc::as_ptr(&self.calls), state);
    }
}
impl ArrayEq for Observation {
    fn array_eq(&self, other: &Self, _: EqMode) -> bool {
        Arc::ptr_eq(&self.calls, &other.calls)
    }
}
impl ValidityVTable<ObservedNative> for ObservedNative {
    fn validity(_: ArrayView<'_, Self>) -> VortexResult<Validity> {
        Ok(Validity::NonNullable)
    }
}
impl VTable for ObservedNative {
    type TypedArrayData = Observation;
    type OperationsVTable = NotSupported;
    type ValidityVTable = Self;

    fn id(&self) -> ArrayId {
        static ID: CachedId = CachedId::new("shardloom.test.observed-native-probe");
        *ID
    }
    fn validate(
        &self,
        _: &Observation,
        dtype: &DType,
        len: usize,
        slots: &[Option<ArrayRef>],
    ) -> VortexResult<()> {
        let [Some(child)] = slots else {
            return Err(vortex_err!("observed native requires one child"));
        };
        if child.dtype() != dtype
            || child.len() != len
            || dtype.is_nullable()
            || !child.is_canonical()
        {
            return Err(vortex_err!(
                "observed native requires an aligned nonnullable canonical child"
            ));
        }
        Ok(())
    }
    fn nbuffers(_: ArrayView<'_, Self>) -> usize {
        0
    }
    fn buffer(_: ArrayView<'_, Self>, _: usize) -> BufferHandle {
        panic!("observed native has no buffers")
    }
    fn buffer_name(_: ArrayView<'_, Self>, _: usize) -> Option<String> {
        None
    }
    fn with_buffers(
        &self,
        array: ArrayView<'_, Self>,
        buffers: &[BufferHandle],
    ) -> VortexResult<ArrayParts<Self>> {
        with_empty_buffers(self, array, buffers)
    }
    fn serialize(_: ArrayView<'_, Self>, _: &VortexSession) -> VortexResult<Option<Vec<u8>>> {
        Ok(None)
    }
    fn deserialize(
        &self,
        _: &DType,
        _: usize,
        _: &[u8],
        _: &[BufferHandle],
        _: &dyn ArrayChildren,
        _: &VortexSession,
    ) -> VortexResult<ArrayParts<Self>> {
        Err(vortex_err!(
            "observed native is test-only and cannot be deserialized"
        ))
    }
    fn slot_name(_: ArrayView<'_, Self>, index: usize) -> String {
        assert_eq!(index, 0);
        "child".to_owned()
    }
    fn execute(array: Array<Self>, _: &mut ExecutionCtx) -> VortexResult<ExecutionResult> {
        array.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ExecutionResult::done(
            array.slots()[0].as_ref().expect("validated child").clone(),
        ))
    }
}

fn observed(child: ArrayRef, calls: &Arc<AtomicUsize>) -> ArrayRef {
    Array::try_from_parts(
        ArrayParts::new(
            ObservedNative,
            child.dtype().clone(),
            child.len(),
            Observation {
                calls: Arc::clone(calls),
            },
        )
        .with_slots(vec![Some(child)].into()),
    )
    .unwrap()
    .into_array()
}

fn assert_probe_miss(array: &ArrayRef, calls: &Arc<AtomicUsize>, ctx: &mut ExecutionCtx) {
    let columns = vec!["renamed_measure".to_owned()];
    let request = VortexSimpleAggregateRequest::new(vec![VortexSimpleAggregateMeasure::new(
        "sum",
        Some(ColumnRef::new(&columns[0]).unwrap()),
        "total".into(),
    )]);
    let mut states = SimpleAggregateStates::new(&request, &columns).unwrap();
    states.states[0].sum = 13.0;
    states.states[0].count = 2;
    let before = states.states[0].result_json().unwrap();
    let mut work = NativeNumericAccessorWork::default();
    assert!(!update(&mut states, array, &columns, None, &mut work, ctx).unwrap());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(states.states[0].result_json().unwrap(), before);
    assert_eq!(states.states[0].count, 2);
    assert_eq!(states.states[0].sum.to_bits(), 13_f64.to_bits());
    assert_eq!(work.encoded_reduction.calls, 0);
    assert_eq!(work.encoded_reduction.child_primitive_executions, 0);
    assert_eq!(work.encoded_reduction.structural_resolutions, 0);
}

#[test]
fn encoded_numeric_probe_unsupported_slice_never_executes_child_on_miss() {
    let session = VortexSession::default();
    let mut ctx = session.create_execution_ctx();
    let calls = Arc::new(AtomicUsize::new(0));
    let leaf = observed(
        PrimitiveArray::new(vec![7_i64, 11, 13, 17], Validity::NonNullable).into_array(),
        &calls,
    );
    let sliced = SliceArray::new(leaf, 1..3).into_array();
    assert_probe_miss(&sliced, &calls, &mut ctx);
    // Establish that this is a live decoder path, not an inert spy. The pinned
    // generic Slice provider executes the unsupported child during real decode.
    let decoded = sliced.execute::<PrimitiveArray>(&mut ctx).unwrap();
    assert_eq!(decoded.as_slice::<i64>(), &[11, 13]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn encoded_numeric_probe_unknown_struct_projection_never_executes_child_on_miss() {
    for with_selection in [false, true] {
        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();
        let calls = Arc::new(AtomicUsize::new(0));
        let canonical = StructArray::try_new(
            FieldNames::from(["unused", "renamed_measure"]),
            vec![
                PrimitiveArray::new(vec![99_u8; 3], Validity::NonNullable).into_array(),
                PrimitiveArray::new(vec![2_i64, 3, 5], Validity::NonNullable).into_array(),
            ],
            3,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let mut unknown = observed(canonical, &calls);
        if with_selection {
            let projection = select(["renamed_measure"], root())
                .bind(unknown.dtype())
                .unwrap();
            unknown = unknown.apply_bound(&projection).unwrap();
        }
        assert_probe_miss(&unknown, &calls, &mut ctx);
        let field = super::logical_field_from_native_array(&unknown, "renamed_measure").unwrap();
        let decoded = field.execute::<PrimitiveArray>(&mut ctx).unwrap();
        assert_eq!(decoded.as_slice::<i64>(), &[2, 3, 5]);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
