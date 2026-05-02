//! `Vortex`-native IO surface for `ShardLoom`.
//!
//! This crate is the dedicated integration point for treating `Vortex` as a
//! first-class native input and output target.

use shardloom_core::{Result, ShardLoomError};

/// Marker for a native `Vortex` dataset handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexDataset {
    /// Dataset location or identifier.
    pub uri: String,
}

/// Open a `Vortex` dataset handle.
///
/// # Errors
/// Returns an error when the provided URI is empty or only whitespace.
pub fn open_dataset(uri: impl Into<String>) -> Result<VortexDataset> {
    let uri = uri.into();
    if uri.trim().is_empty() {
        return Err(ShardLoomError::new("vortex dataset URI must not be empty"));
    }
    Ok(VortexDataset { uri })
}

#[cfg(test)]
mod tests {
    use super::open_dataset;

    #[test]
    fn opens_dataset() {
        let ds = open_dataset("vortex://example").expect("dataset should open");
        assert_eq!(ds.uri, "vortex://example");
    }
}
