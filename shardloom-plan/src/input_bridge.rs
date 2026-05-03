//! Bridge helpers for converting `shardloom_core` universal input contracts into plan requests.

use shardloom_core::{Result, UniversalInputSource};

use crate::ScanRequest;

/// Converts a universal input source into a `ScanRequest` when it has a resolvable dataset URI.
///
/// # Errors
/// Returns any validation error from dataset reference derivation.
pub fn input_source_to_scan_request(source: &UniversalInputSource) -> Result<Option<ScanRequest>> {
    shardloom_core::input_source_to_dataset_ref(source).map(|opt| opt.map(ScanRequest::new))
}

#[cfg(test)]
mod tests {
    use super::input_source_to_scan_request;
    use crate::ScanMode;
    use shardloom_core::{DatasetUri, InputSourceId, InputSourceKind, UniversalInputSource};

    #[test]
    fn uri_backed_source_converts_to_plan_only_scan_request() {
        let source = UniversalInputSource::from_dataset_uri(
            DatasetUri::new("file://tmp/a.vortex").expect("uri"),
        )
        .expect("source");
        let request = input_source_to_scan_request(&source)
            .expect("conversion")
            .expect("some");
        assert_eq!(request.mode, ScanMode::PlanOnly);
        assert!(!request.requires_execution());
    }

    #[test]
    fn source_without_uri_returns_none() {
        let source = UniversalInputSource::new(
            InputSourceId::new("inmem").expect("id"),
            InputSourceKind::InMemory,
        );
        assert!(input_source_to_scan_request(&source).expect("ok").is_none());
    }
}
