// SPDX-License-Identifier: Apache-2.0
//! Original Rust Prost declarations implementing the checked-in frozen
//! `native-exchange/exchange-v1.proto` schema.

#[derive(Clone, Copy, Debug, PartialEq, Eq, ::prost::Enumeration)]
#[repr(i32)]
pub(crate) enum Mode {
    Unspecified = 0,
    NativeFragments = 1,
    NativeAssembled = 2,
    ArrowNone = 3,
    ArrowLz4 = 4,
    ArrowZstd = 5,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct Binding {
    #[prost(bytes = "bytes", tag = "1")]
    pub(crate) server_epoch: bytes::Bytes,
    #[prost(string, tag = "2")]
    pub(crate) principal: String,
    #[prost(string, tag = "3")]
    pub(crate) source_id: String,
    #[prost(bytes = "bytes", tag = "4")]
    pub(crate) source_sha256: bytes::Bytes,
    #[prost(bytes = "bytes", tag = "5")]
    pub(crate) dtype_sha256: bytes::Bytes,
    #[prost(bytes = "bytes", tag = "6")]
    pub(crate) operation_sha256: bytes::Bytes,
    #[prost(bytes = "bytes", tag = "7")]
    pub(crate) attempt_id: bytes::Bytes,
    #[prost(uint64, tag = "8")]
    pub(crate) partition_id: u64,
    #[prost(uint64, tag = "9")]
    pub(crate) first_row: u64,
    #[prost(uint64, tag = "10")]
    pub(crate) rows: u64,
    #[prost(uint64, tag = "11")]
    pub(crate) policy_version: u64,
    #[prost(string, tag = "12")]
    pub(crate) query_id: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct Limits {
    #[prost(uint64, tag = "1")]
    pub(crate) native_memory_bytes: u64,
    #[prost(uint64, tag = "2")]
    pub(crate) queued_bytes: u64,
    #[prost(uint32, tag = "3")]
    pub(crate) queued_frames: u32,
    #[prost(uint32, tag = "4")]
    pub(crate) fragment_payload_bytes: u32,
    #[prost(uint32, tag = "5")]
    pub(crate) encoded_rpc_bytes: u32,
    #[prost(uint32, tag = "6")]
    pub(crate) control_bytes: u32,
    #[prost(uint64, tag = "7")]
    pub(crate) complete_payload_bytes: u64,
    #[prost(uint32, tag = "8")]
    pub(crate) frames: u32,
    #[prost(uint32, tag = "9")]
    pub(crate) fragment_segments: u32,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct Profile {
    #[prost(uint32, tag = "1")]
    pub(crate) extension_version: u32,
    #[prost(string, tag = "2")]
    pub(crate) vortex_version: String,
    #[prost(uint32, tag = "3")]
    pub(crate) vortex_message_version: u32,
    #[prost(string, repeated, tag = "4")]
    pub(crate) physical_registry: Vec<String>,
    #[prost(message, optional, tag = "5")]
    pub(crate) limits: Option<Limits>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct ProjectionField {
    #[prost(string, tag = "1")]
    pub(crate) source: String,
    #[prost(string, tag = "2")]
    pub(crate) alias: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct Projection {
    #[prost(string, tag = "1")]
    pub(crate) id: String,
    #[prost(message, repeated, tag = "2")]
    pub(crate) fields: Vec<ProjectionField>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct Operations {
    #[prost(message, repeated, tag = "1")]
    pub(crate) projections: Vec<Projection>,
    #[prost(uint32, repeated, tag = "2")]
    pub(crate) execution_order: Vec<u32>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct Open {
    #[prost(message, optional, tag = "1")]
    pub(crate) binding: Option<Binding>,
    #[prost(message, optional, tag = "2")]
    pub(crate) profile: Option<Profile>,
    #[prost(enumeration = "Mode", tag = "3")]
    pub(crate) mode: i32,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct Ack {
    #[prost(message, optional, tag = "1")]
    pub(crate) profile: Option<Profile>,
    #[prost(enumeration = "Mode", tag = "2")]
    pub(crate) mode: i32,
    #[prost(bytes = "bytes", tag = "3")]
    pub(crate) expected_payload_sha256: bytes::Bytes,
    #[prost(uint64, tag = "4")]
    pub(crate) expected_payload_bytes: u64,
    #[prost(uint32, tag = "5")]
    pub(crate) expected_messages: u32,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct NativeFragment {
    #[prost(uint32, tag = "1")]
    pub(crate) message_index: u32,
    #[prost(uint64, tag = "2")]
    pub(crate) message_offset: u64,
    #[prost(uint64, tag = "3")]
    pub(crate) message_bytes: u64,
    #[prost(bytes = "bytes", repeated, tag = "4")]
    pub(crate) segments: Vec<bytes::Bytes>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct Complete {
    #[prost(uint64, tag = "1")]
    pub(crate) frames: u64,
    #[prost(uint32, tag = "2")]
    pub(crate) messages: u32,
    #[prost(uint64, tag = "3")]
    pub(crate) rows: u64,
    #[prost(uint64, tag = "4")]
    pub(crate) payload_bytes: u64,
    #[prost(bytes = "bytes", tag = "5")]
    pub(crate) ordered_payload_sha256: bytes::Bytes,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct ArrowPosition {
    #[prost(message, optional, tag = "1")]
    pub(crate) ack: Option<Ack>,
    #[prost(message, optional, tag = "2")]
    pub(crate) complete: Option<Complete>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct Frame {
    #[prost(message, optional, tag = "1")]
    pub(crate) binding: Option<Binding>,
    #[prost(uint64, tag = "2")]
    pub(crate) sequence: u64,
    #[prost(oneof = "frame::Body", tags = "3, 4, 5, 6")]
    pub(crate) body: Option<frame::Body>,
}

pub(crate) mod frame {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub(crate) enum Body {
        #[prost(message, tag = "3")]
        Ack(super::Ack),
        #[prost(message, tag = "4")]
        Native(super::NativeFragment),
        #[prost(message, tag = "5")]
        Arrow(super::ArrowPosition),
        #[prost(message, tag = "6")]
        Complete(super::Complete),
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct Cancel {
    #[prost(message, optional, tag = "1")]
    pub(crate) binding: Option<Binding>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct ReplacePolicy {
    #[prost(bytes = "bytes", tag = "1")]
    pub(crate) server_epoch: bytes::Bytes,
    #[prost(uint64, tag = "2")]
    pub(crate) expected_version: u64,
    #[prost(uint64, tag = "3")]
    pub(crate) new_version: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct SqlTicket {
    #[prost(message, optional, tag = "1")]
    pub(crate) binding: Option<Binding>,
}
