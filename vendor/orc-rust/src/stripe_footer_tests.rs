// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::{
    column::Column,
    reader::memory::tests::{Counters, Memory},
};
use std::sync::Mutex;

fn fixture() -> (StripeFooter, StripeMetadata, RootDataType) {
    let schema = RootDataType::from_proto(&[
        proto::Type {
            kind: Some(proto::r#type::Kind::Struct as i32),
            subtypes: vec![1],
            field_names: vec!["n".into()],
            ..Default::default()
        },
        proto::Type {
            kind: Some(proto::r#type::Kind::Long as i32),
            ..Default::default()
        },
    ])
    .unwrap();
    let footer = StripeFooter {
        streams: vec![proto::Stream {
            column: Some(1),
            kind: Some(proto::stream::Kind::Data as i32),
            length: Some(4),
        }],
        columns: vec![proto::ColumnEncoding::default(); 2],
        writer_timezone: Some("UTC".into()),
        encryption: Vec::new(),
    };
    let info = StripeMetadata::try_from(&proto::StripeInformation {
        offset: Some(3),
        index_length: Some(0),
        data_length: Some(4),
        footer_length: Some(footer.encoded_len() as u64),
        number_of_rows: Some(4),
        ..Default::default()
    })
    .unwrap();
    (footer, info, schema)
}

fn memory(limit: usize) -> (Arc<dyn DecoderMemory>, Arc<Mutex<Counters>>) {
    let counters = Arc::new(Mutex::new(Counters::default()));
    (
        Arc::new(Memory {
            counters: Arc::clone(&counters),
            limit,
        }),
        counters,
    )
}

#[test]
fn parsed_footer_credit_follows_cloned_columns() {
    let (footer, info, schema) = fixture();
    let (memory, counters) = memory(1 << 20);
    let footer = read(
        Bytes::from(footer.encode_to_vec()),
        None,
        Some(&memory),
        &info,
        &schema,
    )
    .unwrap();
    let bytes = retained(&footer).unwrap();
    assert_eq!(counters.lock().unwrap().live, bytes);
    let column = Column::new_with_footer(
        "n".into(),
        schema.children()[0].data_type().clone(),
        Arc::clone(&footer),
    );
    let cloned = column.clone();
    drop(footer);
    drop(column);
    assert_eq!(counters.lock().unwrap().live, bytes);
    assert_eq!(cloned.column_id(), 1);
    drop(cloned);
    assert_eq!(counters.lock().unwrap().live, 0);
}

#[test]
fn footer_buffer_and_protobuf_admission_deny_before_allocation_and_refund() {
    let (footer, info, schema) = fixture();
    for limit in [1, footer.encoded_len()] {
        let (memory, counters) = memory(limit);
        let error = read(
            Bytes::from(footer.encode_to_vec()),
            None,
            Some(&memory),
            &info,
            &schema,
        )
        .unwrap_err();
        assert!(error.to_string().contains("test shared memory denied"));
        assert_eq!(counters.lock().unwrap().live, 0);
        assert!(counters.lock().unwrap().peak <= limit);
    }
}

#[test]
fn malformed_footer_extents_columns_timezone_and_encryption_fail_explicitly() {
    let (footer, info, schema) = fixture();
    let mut variants = Vec::new();
    let mut value = footer.clone();
    value.columns.clear();
    variants.push((value, "encodings"));
    let mut value = footer.clone();
    value.writer_timezone = Some("invalid-zone".into());
    variants.push((value, "timezone"));
    let mut value = footer.clone();
    value.streams[0].length = Some(5);
    variants.push((value, "extent"));
    let mut value = footer.clone();
    value.streams[0].length = None;
    variants.push((value, "length is missing"));
    let mut value = footer.clone();
    value.streams[0].kind = Some(999);
    variants.push((value, "stream kind"));
    let mut value = footer.clone();
    value.columns[0].kind = Some(999);
    variants.push((value, "column encoding"));
    let mut value = footer.clone();
    value.streams[0].column = Some(999);
    variants.push((value, "outside the file schema"));
    let mut value = footer.clone();
    value.streams.push(value.streams[0].clone());
    variants.push((value, "duplicate"));
    let mut value = footer.clone();
    value
        .encryption
        .push(proto::StripeEncryptionVariant::default());
    variants.push((value, "encrypted"));
    for (footer, reason) in variants {
        let (memory, counters) = memory(1 << 20);
        let error = read(
            Bytes::from(footer.encode_to_vec()),
            None,
            Some(&memory),
            &info,
            &schema,
        )
        .unwrap_err();
        assert!(error.to_string().contains(reason), "{error}");
        assert_eq!(counters.lock().unwrap().live, 0);
    }
}
