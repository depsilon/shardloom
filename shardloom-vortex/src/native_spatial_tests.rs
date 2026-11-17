use super::*;
use vortex::array::dtype::{Nullability, StructFields};

fn physical(kind: Kind) -> DType {
    let coordinate = DType::Struct(
        StructFields::new(
            ["x", "y"].into(),
            vec![DType::Primitive(PType::F64, Nullability::NonNullable); 2],
        ),
        Nullability::NonNullable,
    );
    match kind {
        Kind::Point => coordinate.as_nullable(),
        Kind::LineString => DType::List(coordinate.into(), Nullability::Nullable),
        Kind::Polygon => DType::List(
            DType::List(coordinate.into(), Nullability::NonNullable).into(),
            Nullability::Nullable,
        ),
    }
}

#[test]
fn native_spatial_metadata_roundtrip_canonical_crs_and_edge_identity() {
    for kind in Kind::ALL {
        for raw in [
            "{}",
            r#" {"crs":"EPSG:4326", "crs_type":"authority_code", "edges":"spherical"} "#,
            r#"{"crs":{"name":"未知 Ω","axis":[{"name":"x"},{"name":"y"}]},"crs_type":"projjson"}"#,
        ] {
            let parsed = Metadata::parse(kind, raw).unwrap();
            let declared = dtype(parsed.clone(), physical(kind)).unwrap();
            assert_eq!(metadata(&declared), Some(&parsed));
            assert_eq!(storage(&declared), Some(&physical(kind)));
            let bytes = match kind {
                Kind::Point => Point::default().serialize_metadata(&parsed),
                Kind::LineString => LineString::default().serialize_metadata(&parsed),
                Kind::Polygon => Polygon::default().serialize_metadata(&parsed),
            }
            .unwrap();
            let restored = match kind {
                Kind::Point => Point::default().deserialize_metadata(&bytes),
                Kind::LineString => LineString::default().deserialize_metadata(&bytes),
                Kind::Polygon => Polygon::default().deserialize_metadata(&bytes),
            }
            .unwrap();
            assert_eq!(parsed, restored);
        }
    }
    assert_eq!(
        Point::default()
            .deserialize_metadata(b" { } ")
            .unwrap()
            .as_str(),
        "{}"
    );
    assert!(Point::default().deserialize_metadata(&[0xff]).is_err());
    assert!(
        Point::default()
            .serialize_metadata(&Metadata::parse(Kind::Polygon, "{}").unwrap())
            .is_err()
    );
}

#[test]
fn native_spatial_metadata_refuses_ambiguous_or_unsupported_interpretation() {
    for raw in [
        "[]",
        "null",
        r#"{"crs":null}"#,
        r#"{"crs":""}"#,
        r#"{"crs_type":"projjson"}"#,
        r#"{"crs":"x","crs_type":"projjson"}"#,
        r#"{"crs":{},"crs_type":"srid"}"#,
        r#"{"crs":"x","crs_type":"unknown"}"#,
        r#"{"edges":"planar"}"#,
        r#"{"edges":null}"#,
        r#"{"version":1}"#,
        r#"{"crs":"x","crs":"y"}"#,
        r#"{"crs":{"name":"a","name":"b"}}"#,
    ] {
        assert!(Metadata::parse(Kind::Point, raw).is_err(), "{raw}");
    }
    assert!(Metadata::parse(Kind::Point, &" ".repeat(MAX_METADATA_BYTES + 1)).is_err());
    for wrong in [
        DType::Primitive(PType::F64, Nullability::Nullable),
        DType::FixedSizeList(
            DType::Primitive(PType::F64, Nullability::NonNullable).into(),
            2,
            Nullability::Nullable,
        ),
        DType::Struct(
            StructFields::new(
                ["x", "y", "z"].into(),
                vec![DType::Primitive(PType::F64, Nullability::NonNullable); 3],
            ),
            Nullability::Nullable,
        ),
    ] {
        assert!(dtype(Metadata::parse(Kind::Point, "{}").unwrap(), wrong).is_err());
    }
}

#[test]
fn native_spatial_value_domains_keep_empty_distinct_and_require_closed_rings() {
    assert!(values::point(f64::NAN, f64::NAN, true).unwrap());
    assert!(!values::point(-0.0, f64::MAX, true).unwrap());
    for pair in [
        (f64::NAN, 0.0),
        (0.0, f64::NAN),
        (f64::INFINITY, f64::INFINITY),
    ] {
        assert!(values::point(pair.0, pair.1, true).is_err());
    }
    assert!(values::point(f64::NAN, f64::NAN, false).is_err());
    values::sequence(0, false, |_| unreachable!()).unwrap();
    assert!(values::sequence(1, false, |_| Ok((0.0, 0.0))).is_err());
    assert!(values::sequence(0, true, |_| unreachable!()).is_err());
    let closed = [(0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (0.0, 0.0)];
    values::sequence(4, true, |i| Ok(closed[i])).unwrap();
    assert!(
        values::sequence(4, true, |i| Ok((f64::from(u32::try_from(i).unwrap()), 0.0))).is_err()
    );
}
