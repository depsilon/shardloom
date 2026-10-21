// SPDX-License-Identifier: Apache-2.0
//! Local, immutable authority. Request digests never introduce new grants.

use super::{arrow, *};
use serde::Deserialize;
use wire::Operations;

const FREEZE: &str = include_str!(
    "../../docs/benchmarks/evidence/native-exchange-control-2026-10-20/protocol-freeze.json"
);
const FREEZE_SHA256: &str = "bf9f6a0e5840e03bf4536d8816b6fe4138dfb9807439633135ec111c6e28e5dc";
pub(crate) const SUITE_QUERY: &str = "projection_suite";
pub(crate) const SQL_QUERY: &str = "sql_projection";

#[derive(Clone)]
pub(crate) struct Case {
    pub(crate) id: String,
    pub(crate) rows: u64,
    pub(crate) source_sha256: Bytes,
    pub(crate) dtype_sha256: Bytes,
    pub(crate) operation_sha256: Bytes,
    pub(crate) operations: Operations,
    pub(crate) native: NativeExpected,
    pub(crate) arrow: Vec<(Mode, arrow::Expected)>,
}

impl Case {
    pub(crate) fn operation(&self, query: &str) -> Result<Operations> {
        match query {
            SUITE_QUERY => Ok(self.operations.clone()),
            SQL_QUERY if self.id == "dictionary_large" => Ok(Operations {
                projections: vec![self.operations.projections[2].clone()],
                execution_order: vec![0],
            }),
            _ => Err(mismatch("operation is outside the frozen source grant")),
        }
    }

    pub(crate) fn binding(
        &self,
        principal: &str,
        epoch: &Bytes,
        attempt_id: Bytes,
        policy_version: u64,
        query: &str,
    ) -> Result<Binding> {
        let operation = self.operation(query)?;
        let binding = Binding {
            server_epoch: epoch.clone(),
            principal: principal.to_owned(),
            source_id: self.id.clone(),
            source_sha256: self.source_sha256.clone(),
            dtype_sha256: self.dtype_sha256.clone(),
            operation_sha256: hash(&operation.encode_to_vec()),
            attempt_id,
            partition_id: 0,
            first_row: 0,
            rows: self.rows,
            policy_version,
            query_id: query.to_owned(),
        };
        validate_binding(&binding, &binding)?;
        Ok(binding)
    }

    pub(crate) fn arrow_expected(&self, mode: Mode) -> Result<arrow::Expected> {
        self.arrow
            .iter()
            .find(|(candidate, _)| *candidate == mode)
            .map(|(_, expected)| expected.clone())
            .ok_or_else(|| mismatch("explicit Arrow control is absent from authority"))
    }
}

pub(crate) struct Authority {
    pub(crate) cases: Vec<Case>,
    pub(crate) sql: String,
}

impl Authority {
    pub(crate) fn load() -> Result<Self> {
        if hash(FREEZE.as_bytes()) != unhex(FREEZE_SHA256)? {
            return Err(mismatch(
                "compiled fixture freeze differs from reviewed identity",
            ));
        }
        let freeze: Frozen = serde_json::from_str(FREEZE)
            .map_err(|_| invalid("invalid compiled fixture authority"))?;
        let cases = freeze
            .cases
            .into_iter()
            .map(Case::try_from)
            .collect::<Result<Vec<_>>>()?;
        if cases.len() != 13 {
            return Err(invalid("compiled authority requires all thirteen cases"));
        }
        for (index, case) in cases.iter().enumerate() {
            if cases[..index].iter().any(|prior| prior.id == case.id) {
                return Err(invalid("duplicate compiled source identity"));
            }
        }
        Ok(Self {
            cases,
            sql: freeze.ordinary_sql_client.sql,
        })
    }

    pub(crate) fn case(&self, source_id: &str) -> Result<&Case> {
        self.cases
            .iter()
            .find(|case| case.id == source_id)
            .ok_or_else(|| mismatch("source is outside the frozen grant"))
    }
}

pub(crate) fn unhex(value: &str) -> Result<Bytes> {
    if !value.len().is_multiple_of(2) || value.len() > MAX_CONTROL * 2 {
        return Err(invalid("invalid authority hex length"));
    }
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let digit = |byte: u8| match byte {
                b'0'..=b'9' => Ok(byte - b'0'),
                b'a'..=b'f' => Ok(byte - b'a' + 10),
                _ => Err(invalid("invalid authority hex digit")),
            };
            Ok(digit(pair[0])? * 16 + digit(pair[1])?)
        })
        .collect::<Result<Vec<_>>>()
        .map(Bytes::from)
}

#[derive(Deserialize)]
struct Frozen {
    cases: Vec<FrozenCase>,
    ordinary_sql_client: FrozenSql,
}

#[derive(Deserialize)]
struct FrozenSql {
    sql: String,
}

#[derive(Deserialize)]
struct FrozenCase {
    id: String,
    rows: u64,
    source_sha256: String,
    dtype_sha256: String,
    operation_sha256: String,
    operations_protobuf_hex: String,
    ordinary_native_ipc: FrozenNative,
    arrow_flight: Vec<FrozenArrow>,
}

#[derive(Deserialize)]
struct FrozenNative {
    messages: [FrozenMessage; 2],
    ordered_payload_sha256: String,
}

#[derive(Deserialize)]
struct FrozenMessage {
    bytes: u64,
    sha256: String,
}

#[derive(Deserialize)]
struct FrozenArrow {
    codec: String,
    ordered_payload_sha256: String,
    frames: Vec<FrozenFrame>,
}

#[derive(Deserialize)]
struct FrozenFrame {
    header_bytes: usize,
    header_sha256: String,
    body_bytes: usize,
    body_sha256: String,
    declared_decoded_buffer_bytes: u64,
    nodes: usize,
    buffers: usize,
    header_kind: String,
}

impl TryFrom<FrozenCase> for Case {
    type Error = Error;

    fn try_from(value: FrozenCase) -> Result<Self> {
        let operations_bytes = unhex(&value.operations_protobuf_hex)?;
        let operation_sha256 = unhex(&value.operation_sha256)?;
        if hash(&operations_bytes) != operation_sha256 {
            return Err(mismatch("compiled operation digest differs"));
        }
        let operations: Operations = strict_decode(operations_bytes, MAX_CONTROL)?;
        if operations.projections.len() != 3 || operations.execution_order != [0, 1, 2, 1] {
            return Err(invalid(
                "compiled operations differ from frozen execution order",
            ));
        }
        let [dtype, array] = value.ordinary_native_ipc.messages;
        let case = Self {
            id: value.id,
            rows: value.rows,
            source_sha256: unhex(&value.source_sha256)?,
            dtype_sha256: unhex(&value.dtype_sha256)?,
            operation_sha256,
            operations,
            native: NativeExpected {
                messages: [
                    (dtype.bytes, unhex(&dtype.sha256)?),
                    (array.bytes, unhex(&array.sha256)?),
                ],
                ordered_sha256: unhex(&value.ordinary_native_ipc.ordered_payload_sha256)?,
            },
            arrow: value
                .arrow_flight
                .into_iter()
                .map(FrozenArrow::into_expected)
                .collect::<Result<_>>()?,
        };
        if case.arrow.len() != 3
            || case.arrow.iter().map(|(mode, _)| *mode).collect::<Vec<_>>()
                != [Mode::ArrowNone, Mode::ArrowLz4, Mode::ArrowZstd]
        {
            return Err(invalid("compiled authority requires each exact Arrow role"));
        }
        let binding = case.binding(
            "reader",
            &Bytes::from_static(&[1; 16]),
            Bytes::from_static(&[2; 16]),
            1,
            SUITE_QUERY,
        )?;
        NativeStream::new(binding.clone(), Mode::NativeFragments, case.native.clone())?;
        for (mode, expected) in &case.arrow {
            arrow::ArrowStream::new(binding.clone(), *mode, expected.clone())?;
        }
        Ok(case)
    }
}

impl FrozenArrow {
    fn into_expected(self) -> Result<(Mode, arrow::Expected)> {
        let mode = match self.codec.as_str() {
            "none" => Mode::ArrowNone,
            "lz4" => Mode::ArrowLz4,
            "zstd" => Mode::ArrowZstd,
            _ => return Err(invalid("unknown compiled Arrow codec")),
        };
        let frames = self
            .frames
            .into_iter()
            .map(|frame| {
                Ok(arrow::ExpectedFrame {
                    header_bytes: frame.header_bytes,
                    header_sha256: unhex(&frame.header_sha256)?,
                    body_bytes: frame.body_bytes,
                    body_sha256: unhex(&frame.body_sha256)?,
                    metadata: arrow::Metadata {
                        decoded_bytes: frame.declared_decoded_buffer_bytes,
                        nodes: frame.nodes,
                        buffers: frame.buffers,
                        kind: frame.header_kind,
                    },
                })
            })
            .collect::<Result<_>>()?;
        Ok((
            mode,
            arrow::Expected {
                frames,
                ordered_sha256: unhex(&self.ordered_payload_sha256)?,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_frozen_sources_and_controls_have_independent_complete_authority() {
        let authority = Authority::load().unwrap();
        assert_eq!(authority.cases.len(), 13);
        assert_eq!(
            authority.sql,
            "SELECT category AS last_value, event_key AS first_value FROM exchange_fixture"
        );
        for case in &authority.cases {
            let binding = case
                .binding(
                    "reader",
                    &Bytes::from_static(&[1; 16]),
                    Bytes::from_static(&[2; 16]),
                    1,
                    SUITE_QUERY,
                )
                .unwrap();
            assert_eq!(binding.operation_sha256, case.operation_sha256);
            for mode in [Mode::ArrowNone, Mode::ArrowLz4, Mode::ArrowZstd] {
                assert_eq!(
                    case.arrow_expected(mode).unwrap().frames.len(),
                    case.arrow
                        .iter()
                        .find(|(m, _)| *m == mode)
                        .unwrap()
                        .1
                        .frames
                        .len()
                );
            }
            assert!(case.arrow_expected(Mode::NativeFragments).is_err());
            assert!(case.operation("unknown_operation").is_err());
            assert_eq!(
                case.operation(SQL_QUERY).is_ok(),
                case.id == "dictionary_large"
            );
        }
        assert!(authority.case("../../data").is_err());
        let sql = authority
            .case("dictionary_large")
            .unwrap()
            .operation(SQL_QUERY)
            .unwrap();
        assert_eq!(sql.projections.len(), 1);
        assert_eq!(sql.projections[0].id, "last_first_renamed");
        assert_eq!(sql.execution_order, [0]);
        for bad in ["0", "ffGG", "00\0"] {
            assert!(unhex(bad).is_err());
        }
    }
}
