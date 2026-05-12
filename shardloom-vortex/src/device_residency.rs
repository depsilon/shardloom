//! Device residency evidence.
//!
//! Current `ShardLoom` execution is CPU-default. This report makes future
//! CPU/GPU/device boundaries explicit without claiming CUDA, `GPUDirect`, cuDF, or
//! Arrow device execution.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeviceResidencyKind {
    Cpu,
    Cuda,
    FutureDevice,
}

impl DeviceResidencyKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cpu => "cpu",
            Self::Cuda => "cuda",
            Self::FutureDevice => "future_device",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeviceResidencyOutputBoundary {
    Host,
    Device,
    ArrowDevice,
    Cudf,
    VortexArtifact,
}

impl DeviceResidencyOutputBoundary {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Host => "host",
            Self::Device => "device",
            Self::ArrowDevice => "arrow_device",
            Self::Cudf => "cudf",
            Self::VortexArtifact => "vortex_artifact",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct DeviceResidencyReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub device_kind: DeviceResidencyKind,
    pub device_buffer_refs: Vec<String>,
    pub host_to_device_bytes: u64,
    pub device_to_host_bytes: u64,
    pub direct_storage_candidate: bool,
    pub gpu_memory_pool: Option<String>,
    pub kernel_registry: Option<String>,
    pub fused_expression_candidate: bool,
    pub output_boundary: DeviceResidencyOutputBoundary,
    pub cpu_execution_default: bool,
    pub gpu_runtime_claim_allowed: bool,
    pub host_materialization_reported: bool,
    pub external_engine_invoked: bool,
    pub fallback_attempted: bool,
}

impl DeviceResidencyReport {
    #[must_use]
    pub fn cpu_default() -> Self {
        Self {
            schema_version: "shardloom.device_residency_report.v1",
            report_id: "cg15.cg19.device_residency.cpu_default",
            device_kind: DeviceResidencyKind::Cpu,
            device_buffer_refs: Vec::new(),
            host_to_device_bytes: 0,
            device_to_host_bytes: 0,
            direct_storage_candidate: false,
            gpu_memory_pool: None,
            kernel_registry: None,
            fused_expression_candidate: false,
            output_boundary: DeviceResidencyOutputBoundary::Host,
            cpu_execution_default: true,
            gpu_runtime_claim_allowed: false,
            host_materialization_reported: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub fn cuda_future_posture() -> Self {
        Self {
            schema_version: "shardloom.device_residency_report.v1",
            report_id: "cg15.cg19.device_residency.cuda_future",
            device_kind: DeviceResidencyKind::Cuda,
            device_buffer_refs: Vec::new(),
            host_to_device_bytes: 0,
            device_to_host_bytes: 0,
            direct_storage_candidate: true,
            gpu_memory_pool: None,
            kernel_registry: None,
            fused_expression_candidate: true,
            output_boundary: DeviceResidencyOutputBoundary::Device,
            cpu_execution_default: false,
            gpu_runtime_claim_allowed: false,
            host_materialization_reported: false,
            external_engine_invoked: false,
            fallback_attempted: false,
        }
    }

    #[must_use]
    pub const fn claim_blocked(&self) -> bool {
        !self.gpu_runtime_claim_allowed
    }

    #[must_use]
    pub const fn fallback_free(&self) -> bool {
        !self.external_engine_invoked && !self.fallback_attempted
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "device residency\nschema_version: {}\nreport: {}\ndevice: {}\noutput: {}\ngpu runtime claim: blocked\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.device_kind.as_str(),
            self.output_boundary.as_str(),
        )
    }
}

#[must_use]
pub fn plan_device_residency_report() -> DeviceResidencyReport {
    DeviceResidencyReport::cpu_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_residency_defaults_to_cpu_without_gpu_claims() {
        let report = plan_device_residency_report();

        assert_eq!(report.device_kind, DeviceResidencyKind::Cpu);
        assert!(report.cpu_execution_default);
        assert_eq!(report.host_to_device_bytes, 0);
        assert_eq!(report.device_to_host_bytes, 0);
        assert!(!report.direct_storage_candidate);
        assert!(report.claim_blocked());
        assert!(report.fallback_free());
    }

    #[test]
    fn device_residency_can_describe_future_cuda_without_runtime_support() {
        let report = DeviceResidencyReport::cuda_future_posture();

        assert_eq!(report.device_kind, DeviceResidencyKind::Cuda);
        assert!(report.direct_storage_candidate);
        assert!(report.fused_expression_candidate);
        assert_eq!(
            report.output_boundary,
            DeviceResidencyOutputBoundary::Device
        );
        assert!(!report.gpu_runtime_claim_allowed);
        assert!(report.claim_blocked());
        assert!(report.fallback_free());
    }

    #[test]
    fn device_residency_text_keeps_fallback_disabled() {
        let report = plan_device_residency_report();

        assert!(
            report
                .to_human_text()
                .contains("gpu runtime claim: blocked")
        );
        assert!(
            report
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }
}
