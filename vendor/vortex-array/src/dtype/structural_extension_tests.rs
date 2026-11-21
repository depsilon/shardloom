// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::fmt;
use std::hash::{Hash, Hasher};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use parking_lot::Mutex;
use vortex_error::VortexResult;
use vortex_session::registry::CachedId;

use crate::dtype::extension::{ExtDType, ExtId, ExtVTable};
use crate::dtype::{DType, Nullability, PType};
use crate::scalar::ScalarValue;

#[derive(Debug)]
struct Metadata {
    id: u8,
    cloned: bool,
    events: Arc<Mutex<Vec<String>>>,
}

impl Clone for Metadata {
    fn clone(&self) -> Self {
        self.events.lock().push(format!("clone{}", self.id));
        Self {
            id: self.id,
            cloned: true,
            events: Arc::clone(&self.events),
        }
    }
}

impl Drop for Metadata {
    fn drop(&mut self) {
        if self.cloned {
            self.events.lock().push(format!("drop{}", self.id));
        }
    }
}

impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.id.fmt(f)
    }
}

impl PartialEq for Metadata {
    fn eq(&self, other: &Self) -> bool {
        self.events.lock().push(format!("eq{}", self.id));
        self.id == other.id
    }
}
impl Eq for Metadata {}

impl Hash for Metadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.events.lock().push(format!("hash{}", self.id));
        self.id.hash(state);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
struct Probe;

impl ExtVTable for Probe {
    type Metadata = Metadata;
    type NativeValue<'a> = &'a ScalarValue;
    fn id(&self) -> ExtId {
        static ID: CachedId = CachedId::new("test.structural_probe");
        *ID
    }
    fn serialize_metadata(&self, _: &Metadata) -> VortexResult<Vec<u8>> {
        panic!("structural operation must not serialize extension metadata")
    }
    fn deserialize_metadata(&self, _: &[u8]) -> VortexResult<Metadata> {
        panic!("this test has no serialized metadata")
    }
    fn validate_dtype(dtype: &ExtDType<Self>) -> VortexResult<()> {
        dtype
            .metadata()
            .events
            .lock()
            .push(format!("validate{}", dtype.metadata().id));
        Ok(())
    }
    fn unpack_native<'a>(
        _: &'a ExtDType<Self>,
        storage: &'a ScalarValue,
    ) -> VortexResult<Self::NativeValue<'a>> {
        Ok(storage)
    }
}

fn probe(id: u8, storage: DType, events: &Arc<Mutex<Vec<String>>>) -> DType {
    DType::Extension(
        ExtDType::<Probe>::try_new(
            Metadata {
                id,
                cloned: false,
                events: Arc::clone(events),
            },
            storage,
        )
        .unwrap()
        .erased(),
    )
}

#[test]
fn structural_extension_metadata_dispatch_and_clone_order() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let make = || {
        probe(
            1,
            probe(
                2,
                DType::Primitive(PType::I64, Nullability::NonNullable),
                &events,
            ),
            &events,
        )
    };
    let a = make();
    let b = make();
    events.lock().clear();
    assert!(a == b);
    assert_eq!(*events.lock(), ["eq1", "eq2"]);
    events.lock().clear();
    super::hash(&a, false);
    assert_eq!(*events.lock(), ["hash1", "hash2"]);
    events.lock().clear();
    assert!(a.eq_ignore_nullability(&b));
    assert_eq!(*events.lock(), ["eq1", "eq2"]);
    events.lock().clear();
    super::hash(&a, true);
    assert_eq!(*events.lock(), ["hash1", "hash2"]);
    events.lock().clear();
    let converted = a.as_nullable();
    assert_eq!(*events.lock(), ["clone1", "clone2"]);
    assert!(converted.is_nullable());
    assert!(!a.is_nullable());
    assert!(converted.eq_ignore_nullability(&a));
    events.lock().clear();
    drop(converted);
    assert_eq!(*events.lock(), ["drop1", "drop2"]);
}

#[test]
fn structural_extension_conversion_preserves_identity_and_unwind_order() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let a = probe(
        1,
        probe(2, DType::Bool(Nullability::NonNullable), &events),
        &events,
    );
    let DType::Extension(extension) = &a else {
        unreachable!()
    };
    let typed = extension.clone().downcast::<Probe>();
    events.lock().clear();
    let same = extension
        .with_nullability(Nullability::NonNullable)
        .downcast::<Probe>();
    assert!(Arc::ptr_eq(&typed, &same));
    assert!(events.lock().is_empty());
    let new_typed = typed
        .with_nullability(Nullability::NonNullable)
        .downcast::<Probe>();
    assert!(!Arc::ptr_eq(&typed, &new_typed));
    assert_eq!(*events.lock(), ["clone1"]);
    let (DType::Extension(before), DType::Extension(after)) =
        (typed.storage_dtype(), new_typed.storage_dtype())
    else {
        unreachable!()
    };
    assert!(Arc::ptr_eq(
        &before.clone().downcast::<Probe>(),
        &after.clone().downcast::<Probe>()
    ));
    drop(new_typed);

    let null = probe(1, probe(2, DType::Null, &events), &events);
    events.lock().clear();
    assert!(catch_unwind(AssertUnwindSafe(|| null.as_nonnullable())).is_err());
    assert_eq!(*events.lock(), ["clone1", "clone2", "drop2", "drop1"]);
    assert!(null.is_nullable());
}

#[test]
fn structural_nullability_conversion_stops_at_nonextension_storage() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let nested = probe(2, DType::Bool(Nullability::NonNullable), &events);
    let a = probe(
        1,
        DType::List(Arc::new(nested), Nullability::NonNullable),
        &events,
    );
    events.lock().clear();
    let converted = a.as_nullable();
    assert_eq!(*events.lock(), ["clone1"]);
    let DType::Extension(extension) = &converted else {
        unreachable!()
    };
    let DType::List(child, nullability) = extension.storage_dtype() else {
        unreachable!()
    };
    assert_eq!(*nullability, Nullability::Nullable);
    assert!(!child.is_nullable());
}
