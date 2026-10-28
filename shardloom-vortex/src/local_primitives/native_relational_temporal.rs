//! Explicit temporal conversions bound once by the shared expression owner.

use super::{Cell, Result, Value, failed};
use shardloom_core::temporal::{
    TemporalDType, TemporalKind as Kind,
    civil::{self, NANOS_PER_DAY},
    rescale_ticks,
    text::TemporalTextCodec,
    timezone::BoundTimeZone,
};
use std::borrow::Cow;
use vortex::array::{
    dtype::DType,
    extension::datetime::{Date, Timestamp},
};

pub(in crate::local_primitives) struct Cast {
    source: Option<TemporalDType>,
    target: Option<TemporalDType>,
    source_zone: Option<BoundTimeZone>,
    source_text: Option<TemporalTextCodec>,
    target_text: Option<TemporalTextCodec>,
    binary: bool,
}

impl Cast {
    /// Metadata and immutable zone rules are admitted before their construction.
    pub(in crate::local_primitives) fn admission_bytes(
        source: &DType,
        target: &DType,
    ) -> Result<usize> {
        if !matches!(source, DType::Extension(_)) && !matches!(target, DType::Extension(_)) {
            return Ok(0);
        }
        let mut bytes = 4096_usize;
        let (source_zone, target_zone) = zone_names(source, target);
        for zone in [source_zone, target_zone].into_iter().flatten() {
            bytes = bytes
                .checked_add(BoundTimeZone::admission_bytes(zone)?)
                .ok_or_else(|| failed("temporal cast admission overflow"))?;
        }
        Ok(bytes)
    }

    pub(in crate::local_primitives) fn bind(
        source: &DType,
        target: &DType,
    ) -> Result<Option<Self>> {
        if !matches!(source, DType::Extension(_)) && !matches!(target, DType::Extension(_)) {
            return Ok(None);
        }
        let source_type = logical(source)?;
        let target_type = logical(target)?;
        let admitted = match (&source_type, &target_type) {
            (Some(source), Some(target)) => compatible(source.kind(), target.kind()),
            (None, Some(_)) => matches!(source, DType::Utf8(_) | DType::Null),
            (Some(_), None) => matches!(target, DType::Utf8(_) | DType::Binary(_)),
            (None, None) => false,
        };
        if !admitted {
            return Err(failed(
                "temporal cast requires compatible logical domains; assigning or removing a timezone requires its explicit operation",
            ));
        }
        let (source_zone, _) = zone_names(source, target);
        let source_text = if matches!(target, DType::Utf8(_) | DType::Binary(_)) {
            source_type
                .clone()
                .map(TemporalTextCodec::new)
                .transpose()?
        } else {
            None
        };
        let target_text = if matches!(source, DType::Utf8(_)) {
            target_type
                .clone()
                .map(TemporalTextCodec::new)
                .transpose()?
        } else {
            None
        };
        let source_zone = if source_text.is_some() {
            None
        } else {
            source_zone
        };
        Ok(Some(Self {
            source: source_type,
            target: target_type,
            source_zone: source_zone.map(BoundTimeZone::new).transpose()?,
            source_text,
            target_text,
            binary: matches!(target, DType::Binary(_)),
        }))
    }

    pub(in crate::local_primitives) fn formats_text(&self) -> bool {
        self.target.is_none()
    }

    /// Semantic failures alone may become NULL under `TRY_CAST`. The caller owns
    /// memory admission and cancellation outside this method.
    pub(in crate::local_primitives) fn evaluate(&self, value: &Cell) -> Result<Value<'static>> {
        if let Some(target) = &self.target {
            let value = if let Some(source) = &self.source {
                let value = ticks(value)?;
                source.validate_value(value)?;
                self.convert(value, source, target)?
            } else {
                let Cell::Utf8(bytes) = value else {
                    return Err(failed("temporal parsing requires UTF8 text"));
                };
                let text = std::str::from_utf8(bytes)
                    .map_err(|_| failed("temporal text is not valid UTF8"))?;
                self.target_text
                    .as_ref()
                    .ok_or_else(|| failed("temporal text conversion is not bound"))?
                    .parse(text)?
            };
            target.validate_value(value)?;
            return Ok(Value::Int(value));
        }
        let source = self
            .source
            .as_ref()
            .ok_or_else(|| failed("temporal source metadata absent"))?;
        let value = ticks(value)?;
        source.validate_value(value)?;
        let text = self
            .source_text
            .as_ref()
            .ok_or_else(|| failed("temporal text conversion is not bound"))?
            .format(value)?;
        if text.capacity() > 1024 {
            return Err(failed("temporal text exceeded its reserved capacity"));
        }
        Ok(if self.binary {
            Value::Binary(Cow::Owned(text.into_bytes()))
        } else {
            Value::Text(Cow::Owned(text))
        })
    }

    fn convert(&self, value: i64, source: &TemporalDType, target: &TemporalDType) -> Result<i64> {
        match (source.kind(), target.kind()) {
            (Kind::Time(from), Kind::Time(to))
            | (Kind::Duration(from), Kind::Duration(to))
            | (Kind::Timestamp { unit: from, .. }, Kind::Timestamp { unit: to, .. }) => {
                rescale_ticks(value, *from, *to)
            }
            (_, Kind::Date32 | Kind::Date64) => {
                let days = match source.kind() {
                    Kind::Date32 => i128::from(value),
                    Kind::Date64 => i128::from(value / 86_400_000),
                    Kind::Timestamp { unit, .. } => {
                        if let Some(zone) = &self.source_zone {
                            let date = zone.civil(value, *unit)?;
                            civil::days_from_civil(date.year, date.month, date.day)
                        } else {
                            (i128::from(value) * unit.nanoseconds()).div_euclid(NANOS_PER_DAY)
                        }
                    }
                    _ => return Err(failed("temporal domain cannot be cast to a date")),
                };
                date_ticks(days, target)
            }
            (
                Kind::Date32 | Kind::Date64,
                Kind::Timestamp {
                    unit,
                    timezone: None,
                },
            ) => {
                let days = if matches!(source.kind(), Kind::Date32) {
                    value
                } else {
                    value / 86_400_000
                };
                civil::nanos_to_ticks(i128::from(days) * NANOS_PER_DAY, *unit)
            }
            _ => Err(failed("temporal cast has no admitted conversion")),
        }
    }
}

fn logical(dtype: &DType) -> Result<Option<TemporalDType>> {
    if matches!(dtype, DType::Extension(_)) {
        crate::native_temporal::temporal_dtype(dtype)
            .map(Some)
            .ok_or_else(|| failed("cast extension metadata is not admitted"))
    } else {
        Ok(None)
    }
}

fn compatible(source: &Kind, target: &Kind) -> bool {
    match (source, target) {
        (
            Kind::Date32 | Kind::Date64,
            Kind::Date32 | Kind::Date64 | Kind::Timestamp { timezone: None, .. },
        )
        | (Kind::Timestamp { .. }, Kind::Date32 | Kind::Date64)
        | (Kind::Time(_), Kind::Time(_))
        | (Kind::Duration(_), Kind::Duration(_)) => true,
        (
            Kind::Timestamp { timezone: left, .. },
            Kind::Timestamp {
                timezone: right, ..
            },
        ) => left.is_some() == right.is_some(),
        _ => false,
    }
}

fn zone_names<'a>(source: &'a DType, target: &'a DType) -> (Option<&'a str>, Option<&'a str>) {
    let zone = |dtype: &'a DType| match dtype {
        DType::Extension(extension) => extension
            .metadata_opt::<Timestamp>()
            .and_then(|metadata| metadata.tz.as_deref()),
        _ => None,
    };
    let target_date =
        matches!(target, DType::Extension(extension) if extension.metadata_opt::<Date>().is_some());
    (
        if matches!(target, DType::Utf8(_) | DType::Binary(_)) || target_date {
            zone(source)
        } else {
            None
        },
        if matches!(source, DType::Utf8(_)) {
            zone(target)
        } else {
            None
        },
    )
}

fn ticks(value: &Cell) -> Result<i64> {
    match value {
        Cell::Date(value) => Ok(i64::from(*value)),
        Cell::Timestamp(value) | Cell::Temporal(value) => Ok(*value),
        _ => Err(failed("temporal cast source must retain its logical type")),
    }
}

fn date_ticks(days: i128, dtype: &TemporalDType) -> Result<i64> {
    let ticks = if matches!(dtype.kind(), Kind::Date32) {
        days
    } else {
        days * 86_400_000
    };
    let ticks =
        i64::try_from(ticks).map_err(|_| failed("date conversion exceeds its storage domain"))?;
    dtype.validate_value(ticks)?;
    Ok(ticks)
}

#[cfg(test)]
#[path = "native_relational_temporal_tests.rs"]
mod tests;
