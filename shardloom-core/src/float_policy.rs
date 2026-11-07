//! Immutable caller-selected floating semantics, independent of resource grants.

use crate::{Result, ShardLoomError};

/// Admission of nonfinite floating data for one native session.
/// NULL validity is independent of this policy. It does not relax finite control
/// parameters, exact-domain casts, or output-format representability checks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum FloatPolicy {
    /// Preserve existing observed-nonfinite refusals.
    #[default]
    Strict,
    /// Admit NaN and infinities, with NaNs equal/high and signed zeros equal.
    Extended,
}

impl FloatPolicy {
    /// Parse the stable, case-sensitive public policy name.
    /// # Errors
    /// Rejects unknown policies before input or native execution.
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "strict" => Ok(Self::Strict),
            "extended" => Ok(Self::Extended),
            _ => Err(ShardLoomError::InvalidOperation(
                "float_policy must be strict or extended; no fallback execution was attempted"
                    .to_owned(),
            )),
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::Extended => "extended",
        }
    }

    /// Whether a valid floating value may be observed by an admitted operator.
    #[must_use]
    pub fn admits(self, value: f64) -> bool {
        self == Self::Extended || value.is_finite()
    }
}

/// Canonical NaN for newly computed Float64 results, never for opaque carriage.
pub const CANONICAL_F64_NAN_BITS: u64 = 0x7ff8_0000_0000_0000;
/// Canonical NaN for newly computed Float32 results, never for opaque carriage.
pub const CANONICAL_F32_NAN_BITS: u32 = 0x7fc0_0000;

/// Widen raw IEEE binary16 bits without quieting a carried signaling NaN.
///
/// Finite values widen exactly. NaN fraction bits occupy the corresponding
/// high Float64 fraction positions, so exact narrowing recovers every bit.
/// The raw-bit boundary keeps the provider's half type out of the core graph.
#[must_use]
pub fn widen_f16_bits(bits: u16) -> f64 {
    let sign = u64::from(bits & 0x8000) << 48;
    let exponent = u64::from((bits >> 10) & 0x1f);
    let fraction = u64::from(bits & 0x03ff);
    let magnitude = match (exponent, fraction) {
        (0, 0) => 0,
        (0, fraction) => {
            // A binary16 subnormal is fraction * 2^-24. Normalize its leading
            // bit without performing floating arithmetic on the representation.
            let highest = fraction.ilog2();
            ((999 + u64::from(highest)) << 52) | ((fraction - (1_u64 << highest)) << (52 - highest))
        }
        (31, fraction) => 0x7ff0_0000_0000_0000 | (fraction << 42),
        (exponent, fraction) => ((exponent + 1008) << 52) | (fraction << 42),
    };
    f64::from_bits(sign | magnitude)
}

/// Recover exactly representable IEEE binary16 bits, including carried NaNs.
///
/// Rejects finite rounding, overflow and underflow, and NaN fraction bits that
/// do not fit binary16. This is representation conversion; callers separately
/// enforce the session's floating observation policy.
#[must_use]
pub fn narrow_f16_bits_exact(value: f64) -> Option<u16> {
    let bits = value.to_bits();
    let sign = (bits >> 48) & 0x8000;
    let exponent = (bits >> 52) & 0x7ff;
    let fraction = bits & 0x000f_ffff_ffff_ffff;
    let discarded_fraction = (1_u64 << 42) - 1;
    let narrowed = match exponent {
        0 if fraction == 0 => sign,
        999..=1008 => {
            let shift = 1051 - exponent;
            let significand = (1_u64 << 52) | fraction;
            if significand & ((1_u64 << shift) - 1) != 0 {
                return None;
            }
            sign | (significand >> shift)
        }
        1009..=1038 => {
            if fraction & discarded_fraction != 0 {
                return None;
            }
            sign | ((exponent - 1008) << 10) | (fraction >> 42)
        }
        2047 => {
            if fraction & discarded_fraction != 0 {
                return None;
            }
            sign | 0x7c00 | (fraction >> 42)
        }
        _ => return None,
    };
    u16::try_from(narrowed).ok()
}

/// Widen in a scalar register without quieting a carried signaling NaN.
/// Finite values widen exactly; NaN payload bits occupy the corresponding high
/// mantissa positions so an exact narrowing can recover the original F32 bits.
#[must_use]
pub fn widen_f32(value: f32) -> f64 {
    if value.is_nan() {
        let bits = value.to_bits();
        f64::from_bits(
            (u64::from(bits & 0x8000_0000) << 32)
                | 0x7ff0_0000_0000_0000
                | (u64::from(bits & 0x007f_ffff) << 29),
        )
    } else {
        f64::from(value)
    }
}

/// Recover an exactly representable F32 value, including carried NaN bits.
#[must_use]
#[allow(clippy::cast_possible_truncation)] // Exact roundtrip checked; NaN mantissa bounded explicitly.
pub fn narrow_f32_exact(value: f64) -> Option<f32> {
    if value.is_nan() {
        let bits = value.to_bits();
        if bits & ((1_u64 << 29) - 1) != 0 {
            return None;
        }
        Some(f32::from_bits(
            ((bits >> 32) as u32 & 0x8000_0000)
                | 0x7f80_0000
                | ((bits & 0x000f_ffff_ffff_ffff) >> 29) as u32,
        ))
    } else {
        let narrowed = value as f32;
        (f64::from(narrowed).to_bits() == value.to_bits()).then_some(narrowed)
    }
}

/// Normalize semantic floating identity without altering the carried value.
/// The caller must first admit the value through its session policy.
#[must_use]
pub fn float_key_bits(value: f64) -> u64 {
    if value.is_nan() {
        CANONICAL_F64_NAN_BITS
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    }
}

/// Fix an arithmetic-generated NaN payload without changing other result bits.
#[must_use]
pub fn canonicalize_f64_result(value: f64) -> f64 {
    if value.is_nan() {
        f64::from_bits(CANONICAL_F64_NAN_BITS)
    } else {
        value
    }
}

/// Fix an arithmetic-generated NaN payload without changing other result bits.
#[must_use]
pub fn canonicalize_f32_result(value: f32) -> f32 {
    if value.is_nan() {
        f32::from_bits(CANONICAL_F32_NAN_BITS)
    } else {
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_widening_preserves_every_binary16_representation() {
        for bits in u16::MIN..=u16::MAX {
            let value = widen_f16_bits(bits);
            assert_eq!(narrow_f16_bits_exact(value), Some(bits), "{bits:04x}");
            assert_eq!(value.is_sign_negative(), bits & 0x8000 != 0);
            assert_eq!(value.is_finite(), bits & 0x7c00 != 0x7c00);
            if bits & 0x7c00 == 0x7c00 {
                assert_eq!(
                    value.to_bits(),
                    (u64::from(bits & 0x8000) << 48)
                        | 0x7ff0_0000_0000_0000
                        | (u64::from(bits & 0x03ff) << 42)
                );
            }
        }
        for (bits, expected) in [
            (0x0000, 0.0_f64),
            (0x8000, -0.0),
            (0x0001, 1.0 / 16_777_216.0),
            (0x03ff, 1023.0 / 16_777_216.0),
            (0x0400, 1.0 / 16_384.0),
            (0x3c00, 1.0),
            (0xbc00, -1.0),
            (0x7bff, 65_504.0),
            (0xfbff, -65_504.0),
            (0x7c00, f64::INFINITY),
            (0xfc00, f64::NEG_INFINITY),
        ] {
            assert_eq!(widen_f16_bits(bits).to_bits(), expected.to_bits());
        }
    }

    #[test]
    fn binary16_narrowing_rejects_rounding_and_unrepresentable_regions() {
        for bits in u16::MIN..=u16::MAX {
            let value = widen_f16_bits(bits);
            if !value.is_finite() {
                continue;
            }
            let wide = value.to_bits();
            if wide.trailing_zeros() >= 63 {
                assert!(narrow_f16_bits_exact(f64::from_bits(wide | 1)).is_none());
            } else {
                for adjacent in [wide - 1, wide + 1] {
                    assert!(
                        narrow_f16_bits_exact(f64::from_bits(adjacent)).is_none(),
                        "adjacent to {bits:04x}: {adjacent:016x}"
                    );
                }
            }
        }
        for sign in [0, 1_u64 << 63] {
            for exponent in 0_u64..2047 {
                if (999..=1038).contains(&exponent) {
                    continue;
                }
                for fraction in [0, 1, 0x000f_ffff_ffff_ffff] {
                    if exponent == 0 && fraction == 0 {
                        continue;
                    }
                    let bits = sign | (exponent << 52) | fraction;
                    assert!(narrow_f16_bits_exact(f64::from_bits(bits)).is_none());
                }
            }
            for discarded in 0..42 {
                let bits = sign | CANONICAL_F64_NAN_BITS | (1_u64 << discarded);
                assert!(narrow_f16_bits_exact(f64::from_bits(bits)).is_none());
            }
        }
        for value in [0.1, 65_520.0, 1.0 / 33_554_432.0] {
            assert!(narrow_f16_bits_exact(value).is_none());
            assert!(narrow_f16_bits_exact(-value).is_none());
        }
    }

    #[test]
    fn register_widening_preserves_every_special_f32_payload_bit() {
        for sign in [0, 0x8000_0000] {
            for payload in [1, 2, 0x003f_ffff, 0x0040_0000, 0x0040_1234, 0x007f_ffff] {
                let bits = sign | 0x7f80_0000 | payload;
                assert_eq!(
                    narrow_f32_exact(widen_f32(f32::from_bits(bits)))
                        .unwrap()
                        .to_bits(),
                    bits
                );
            }
        }
        for bits in [
            0,
            0x8000_0000,
            1,
            0x8000_0001,
            0x7f7f_ffff,
            0xff7f_ffff,
            0x7f80_0000,
            0xff80_0000,
        ] {
            assert_eq!(
                narrow_f32_exact(widen_f32(f32::from_bits(bits)))
                    .unwrap()
                    .to_bits(),
                bits
            );
        }
        assert!(narrow_f32_exact(f64::from_bits(CANONICAL_F64_NAN_BITS | 1)).is_none());
        assert!(narrow_f32_exact(1.0 + f64::EPSILON).is_none());
        assert!(narrow_f32_exact(f64::MAX).is_none());
    }

    #[test]
    fn policies_and_key_identity_keep_null_and_representation_separate() {
        for name in ["strict", "extended"] {
            assert_eq!(FloatPolicy::parse(name).unwrap().as_str(), name);
        }
        for name in ["", "EXTENDED", "ieee", " extended"] {
            assert!(FloatPolicy::parse(name).is_err());
        }
        assert_eq!(FloatPolicy::default(), FloatPolicy::Strict);
        let bits = [
            0xfff8_0000_0000_0123,
            0x7ff0_0000_0000_0001,
            0x7ff8_1234_5678_9abc,
        ];
        for bits in bits {
            let value = f64::from_bits(bits);
            assert!(!FloatPolicy::Strict.admits(value));
            assert!(FloatPolicy::Extended.admits(value));
            assert_eq!(float_key_bits(value), CANONICAL_F64_NAN_BITS);
            assert_eq!(
                canonicalize_f64_result(value).to_bits(),
                CANONICAL_F64_NAN_BITS
            );
            assert_eq!(value.to_bits(), bits);
        }
        let ordered = [
            f64::NEG_INFINITY,
            -1.0,
            -0.0,
            0.0,
            1.0,
            f64::INFINITY,
            f64::NAN,
        ];
        for pair in ordered.windows(2) {
            let left = f64::from_bits(float_key_bits(pair[0]));
            let right = f64::from_bits(float_key_bits(pair[1]));
            assert!(!left.total_cmp(&right).is_gt());
        }
        for value in [
            f64::NEG_INFINITY,
            -f64::MAX,
            -0.0,
            0.0,
            f64::MIN_POSITIVE,
            f64::INFINITY,
        ] {
            assert_eq!(canonicalize_f64_result(value).to_bits(), value.to_bits());
            assert_eq!(FloatPolicy::Strict.admits(value), value.is_finite());
        }
        assert_eq!(
            canonicalize_f32_result(f32::from_bits(0xff80_0123)).to_bits(),
            CANONICAL_F32_NAN_BITS
        );
        assert_eq!(
            canonicalize_f32_result(-0.0).to_bits(),
            (-0.0_f32).to_bits()
        );
    }
}
