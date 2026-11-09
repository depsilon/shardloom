//! A literal's payload credit must survive independently of its bound plan.

use super::*;
use crate::resident_session::ResidentVortexSession;
use vortex::array::scalar::ScalarValue as NativeValue;

#[test]
fn native_flat_literal_payload_retains_credit_after_binding_owner_drops() {
    for source in [
        ScalarValue::Utf8("owned literal Ω".into()),
        ScalarValue::Binary(vec![0, 255, 1, 128]),
        ScalarValue::Utf8(String::new()),
        ScalarValue::Binary(Vec::new()),
    ] {
        let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let mut binder = Binder::new(&session).unwrap();
        let expression = binder.literal_expression(&source).unwrap();
        let Kind::Literal(scalar) = &expression.kind else {
            panic!("bound literal");
        };
        let bytes = match scalar.value().unwrap() {
            NativeValue::Utf8(value) => value.inner().clone(),
            NativeValue::Binary(value) => value.clone(),
            _ => panic!("flat payload"),
        };
        let expected = bytes.as_slice().to_vec();
        let clone = bytes.clone();
        drop((expression, binder));
        drop(session);
        assert!(
            memory.snapshot().reserved_bytes > 0,
            "literal payload lost credit: {source:?}"
        );
        assert_eq!(bytes.as_slice(), expected);
        drop(bytes);
        assert!(memory.snapshot().reserved_bytes > 0);
        assert_eq!(clone.as_slice(), expected);
        drop(clone);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
