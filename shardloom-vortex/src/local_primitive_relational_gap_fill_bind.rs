//! Bind one shared temporal grid over the existing native ordering owner.

use super::{
    Binder, DType, Node, NodeKind, Nullability, Result, failed, validate_name, validate_width,
};
use crate::{
    local_primitives::native_relational_gap_fill as kernel,
    relational_query::{
        VortexRelationalGapFill, VortexRelationalNullOrder, VortexRelationalOrderKey,
    },
};
use shardloom_core::temporal::operations::{Kernel, Operation};

impl Binder<'_> {
    pub(super) fn gap_fill(
        &mut self,
        fill: &VortexRelationalGapFill,
        depth: usize,
    ) -> Result<Node> {
        validate_width(fill.partition_by.len().saturating_add(1))?;
        self.charge_fields(fill.partition_by.len() + 1)?;
        let input = self.bind(&fill.input, depth + 1)?;
        self.charge_fields(input.fields.len())?;
        let time = top_level(&input.fields, fill.time.as_str())?;
        let temporal =
            crate::native_temporal::temporal_dtype(&input.fields[time].1).ok_or_else(|| {
                failed("gap filling requires an explicitly typed temporal time column")
            })?;
        let operation = Operation::Bucket {
            step: fill.step,
            origin: fill.origin,
        };
        self.charge(operation.admission_bytes(&temporal, None)?)?;
        let grid = Kernel::bind(operation, temporal, None)?;
        let bound = |value: i64| -> Result<i128> {
            let index = grid.bucket_index(value)?;
            if grid.grid_point(index)? != value {
                return Err(failed(
                    "gap-fill bounds must be aligned grid points; bucket them explicitly",
                ));
            }
            Ok(index)
        };
        let start = fill.start.map(bound).transpose()?;
        let end = fill.end.map(bound).transpose()?;
        if start.zip(end).is_some_and(|(start, end)| start > end) {
            return Err(failed("gap-fill start must not follow its end"));
        }
        let mut partition = Vec::new();
        let mut keys = Vec::new();
        for column in &fill.partition_by {
            let index = top_level(&input.fields, column.as_str())?;
            if index == time || partition.contains(&index) {
                return Err(failed(
                    "gap-fill partition columns must be unique and distinct from time",
                ));
            }
            partition.push(index);
            keys.push(VortexRelationalOrderKey {
                column: column.clone(),
                descending: false,
                nulls: Some(VortexRelationalNullOrder::First),
            });
        }
        keys.push(VortexRelationalOrderKey {
            column: fill.time.clone(),
            descending: false,
            nulls: Some(VortexRelationalNullOrder::First),
        });
        let key_names = keys
            .iter()
            .map(|key| key.column.as_str().to_owned())
            .collect();
        let fields = input
            .fields
            .iter()
            .enumerate()
            .map(|(index, (name, dtype))| {
                (
                    name.clone(),
                    if index == time {
                        dtype.as_nonnullable()
                    } else if partition.contains(&index) {
                        dtype.clone()
                    } else {
                        dtype.as_nullable()
                    },
                )
            })
            .collect::<Vec<_>>();
        let dtype = DType::struct_(fields.clone(), Nullability::NonNullable);
        let input = Box::new(self.sort_input(&keys, input)?);
        Ok(Node {
            fields: fields.clone(),
            kind: NodeKind::GapFill {
                input,
                spec: Box::new(kernel::Spec {
                    fields,
                    dtype,
                    time,
                    partition,
                    key_names,
                    grid,
                    start,
                    end,
                }),
            },
        })
    }
}

fn top_level(fields: &[(String, DType)], name: &str) -> Result<usize> {
    validate_name(name)?;
    fields.iter().position(|(field, _)| field == name)
        .ok_or_else(|| failed("gap-fill time and partition keys require existing top-level columns; project nested keys first"))
}
