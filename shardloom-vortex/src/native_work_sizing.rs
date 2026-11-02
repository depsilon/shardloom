//! Representation costs and applied evidence for the shared runtime sizer.
//! Payload reservation and validation remain with their existing native owners.

use crate::resident_session::NativeExecutionContext;
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::adaptive_work::{
    AdaptiveWorkReport, AdaptiveWorkSizer, WorkCost, WorkDenial, WorkLimits, WorkShape,
};
use vortex::array::dtype::DType;

#[derive(Clone, Copy)]
pub(crate) enum Owner {
    Scan,
    Replay,
    ColumnarDecode,
    Output,
    SortOutput,
    #[cfg(feature = "vortex-write")]
    Spill,
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io", unix))]
    CompatibilityWriter,
}

impl Owner {
    const fn index(self) -> usize {
        match self {
            Self::Scan => 0,
            Self::Replay => 1,
            Self::ColumnarDecode => 2,
            Self::Output => 3,
            Self::SortOutput => 6,
            #[cfg(feature = "vortex-write")]
            Self::Spill => 4,
            #[cfg(all(feature = "vortex-write", feature = "universal-format-io", unix))]
            Self::CompatibilityWriter => 5,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Scan => "native_scan",
            Self::Replay => "native_replay",
            Self::ColumnarDecode => "columnar_decode",
            Self::Output => "native_output",
            Self::SortOutput => "native_sort_output",
            #[cfg(feature = "vortex-write")]
            Self::Spill => "native_spill",
            #[cfg(all(feature = "vortex-write", feature = "universal-format-io", unix))]
            Self::CompatibilityWriter => "compatibility_writer",
        }
    }
}

#[derive(Default, Clone, Copy)]
pub(crate) struct Reports([AdaptiveWorkReport; 7]);

impl Reports {
    pub(crate) fn record(&mut self, owner: Owner, shape: WorkShape) {
        self.0[owner.index()].record(shape);
    }

    pub(crate) fn record_decline(&mut self, owner: Owner, denial: WorkDenial) {
        self.0[owner.index()].record_decline(denial);
    }

    pub(crate) fn append_certificate(&self, proof: &mut String) {
        use std::fmt::Write as _;
        for owner in [
            Owner::Scan,
            Owner::Replay,
            Owner::ColumnarDecode,
            Owner::Output,
            Owner::SortOutput,
            #[cfg(feature = "vortex-write")]
            Owner::Spill,
            #[cfg(all(feature = "vortex-write", feature = "universal-format-io", unix))]
            Owner::CompatibilityWriter,
        ] {
            let report = self.0[owner.index()];
            if report.completed_windows == 0 && report.declined_windows == 0 {
                continue;
            }
            let name = owner.name();
            write!(proof,
                " adaptive_{name}_completed_windows={} adaptive_{name}_headroom_limited_windows={} adaptive_{name}_shrunk_windows={} adaptive_{name}_grown_windows={} adaptive_{name}_minimum_rows={} adaptive_{name}_maximum_rows={} adaptive_{name}_minimum_headroom_bytes={} adaptive_{name}_ceiling_bytes={} adaptive_{name}_peak_estimated_bytes={} adaptive_{name}_cost_scope=additional_workspace_estimate_not_rss",
                report.completed_windows, report.headroom_limited_windows, report.shrunk_windows,
                report.grown_windows, report.minimum_units, report.maximum_units,
                report.minimum_headroom_bytes, report.ceiling_bytes, report.peak_estimated_bytes_per_lane,
            ).expect("String writes cannot fail");
            if report.declined_windows != 0 {
                write!(proof,
                    " adaptive_{name}_declined_windows={} adaptive_{name}_decline_reason=insufficient_headroom adaptive_{name}_maximum_declined_required_bytes={}",
                    report.declined_windows, report.maximum_declined_required_bytes,
                ).expect("String writes cannot fail");
            }
        }
    }
}

pub(crate) struct Rows {
    owner: Owner,
    sizing: AdaptiveWorkSizer,
    cost: WorkCost,
    maximum: usize,
    copies: u64,
}

pub(crate) struct SelectedRows {
    shape: WorkShape,
    pub(crate) rows: usize,
}

impl Rows {
    pub(crate) fn from_fields(
        owner: Owner,
        fields: &[(String, DType)],
        maximum: usize,
        copies: u64,
    ) -> Result<Self> {
        Ok(Self {
            owner,
            sizing: AdaptiveWorkSizer::default(),
            cost: fields_cost(fields, copies)?,
            maximum,
            copies,
        })
    }

    /// A schema-derived starting estimate. Variable values and provider decode
    /// workspace are not bounded by this estimate. Actual owners still reserve.
    pub(crate) fn new(owner: Owner, dtype: &DType, maximum: usize, copies: u64) -> Result<Self> {
        Self::with_cost(
            owner,
            crate::native_payload_schema::schema_bytes(dtype)?,
            row_bytes(dtype, 0)?,
            maximum,
            copies,
        )
    }

    fn with_cost(
        owner: Owner,
        metadata: u64,
        row_bytes: u64,
        maximum: usize,
        copies: u64,
    ) -> Result<Self> {
        Ok(Self {
            owner,
            sizing: AdaptiveWorkSizer::default(),
            cost: cost(metadata, row_bytes, copies)?,
            maximum,
            copies,
        })
    }

    pub(crate) fn select(
        &self,
        remaining: u64,
        context: &NativeExecutionContext<'_>,
    ) -> Result<SelectedRows> {
        context.check_cancelled()?;
        let shape = self.sizing.select(
            self.owner.name(),
            context.memory().snapshot(),
            self.cost,
            WorkLimits {
                min_units: 1,
                max_units: remaining.min(self.maximum as u64),
                max_in_flight: 1,
            },
        )?;
        Ok(SelectedRows {
            rows: usize::try_from(shape.units()).map_err(|_| overflow())?,
            shape,
        })
    }

    /// Only compact newly produced payload is a useful observation. A borrowed
    /// slice's retained backing size is not its per-row construction cost.
    #[allow(clippy::needless_pass_by_value)] // Consume the selection once; completion is not reusable.
    pub(crate) fn complete(
        &mut self,
        selected: SelectedRows,
        compact_bytes: Option<u64>,
        context: &NativeExecutionContext<'_>,
    ) {
        let observed = compact_bytes.map(|bytes| {
            bytes
                .div_ceil(selected.rows as u64)
                .saturating_mul(self.copies)
                .saturating_add(16)
        });
        context.record_adaptive_work(self.owner, selected.shape);
        self.sizing.complete(selected.shape, observed);
    }
}

/// Schema and physical starting costs without constructing a temporary dtype.
/// Owners add their own simultaneous scratch, framing and retained structures.
pub(crate) fn fields_cost(fields: &[(String, DType)], copies: u64) -> Result<WorkCost> {
    let metadata = fields.iter().try_fold(
        crate::native_payload_schema::names_bytes(fields.len(), |i| fields[i].0.as_str())?,
        |sum, (_, dtype)| {
            sum.checked_add(crate::native_payload_schema::metadata_bytes(dtype)?)
                .ok_or_else(overflow)
        },
    )?;
    let row_bytes = fields.iter().try_fold(0_u64, |sum, (_, dtype)| {
        sum.checked_add(row_bytes(dtype, 0)?).ok_or_else(overflow)
    })?;
    cost(metadata, row_bytes, copies)
}

fn cost(metadata: u64, row_bytes: u64, copies: u64) -> Result<WorkCost> {
    Ok(WorkCost {
        fixed_bytes: metadata.checked_mul(copies).ok_or_else(overflow)?,
        bytes_per_unit: row_bytes
            .checked_mul(copies)
            .and_then(|bytes| bytes.checked_add(16))
            .ok_or_else(overflow)?,
    })
}

/// Initial physical value/validity estimate. This never reads values or validates
/// domains; nested variable lengths are learned only from completed native work.
fn row_bytes(dtype: &DType, depth: usize) -> Result<u64> {
    if depth > 64 {
        return Err(overflow());
    }
    let bytes = match dtype {
        DType::Null | DType::Bool(_) => 1,
        DType::Primitive(ptype, _) => ptype.byte_width() as u64,
        DType::Decimal(_, _) => 16,
        DType::Utf8(_) | DType::Binary(_) => 24,
        DType::List(child, _) => 8_u64
            .checked_add(row_bytes(child, depth + 1)?)
            .ok_or_else(overflow)?,
        DType::FixedSizeList(child, count, _) => row_bytes(child, depth + 1)?
            .checked_mul(u64::from(*count))
            .ok_or_else(overflow)?,
        DType::Struct(fields, _) => fields.fields().try_fold(0_u64, |sum, field| {
            sum.checked_add(row_bytes(&field, depth + 1)?)
                .ok_or_else(overflow)
        })?,
        DType::Extension(extension) => return row_bytes(extension.storage_dtype(), depth + 1),
        // Other domains remain governed by the binder/provider. Sizing does not
        // add support or evaluate a lazy value in order to estimate its cost.
        DType::Map(..) | DType::Union(..) | DType::Variant(..) => 64,
    };
    bytes
        .checked_add(u64::from(dtype.is_nullable()))
        .ok_or_else(overflow)
}

fn overflow() -> ShardLoomError {
    ShardLoomError::InvalidOperation(
        "native adaptive work schema size overflow; no fallback execution was attempted".into(),
    )
}
