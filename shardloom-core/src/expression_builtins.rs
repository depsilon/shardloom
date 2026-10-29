//! Shared definitions of the admitted native scalar function family.
//! SQL intake, native type/null binding, kernel selection and discovery use
//! this table. It describes existing providers; it is not an evaluator.

use super::NullBehavior;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScalarTypeRule {
    NumericAbs,
    NumericRound,
    DecimalOutput,
    Utf8,
    Utf8Offsets,
    Utf8Length,
    Utf8Predicate,
    ByteLength,
    BinaryDecode,
    EpochMinute,
    EpochTruncate,
    DateField,
    TimestampField,
    DateOffset,
    TimestampOffset,
    DateDifference,
    TimestampDifference,
    Temporal,
    JsonText,
    TimestampParse,
    Coalesce,
    NullIf,
    Conditional,
}

impl ScalarTypeRule {
    #[must_use]
    pub const fn contract(self) -> &'static str {
        match self {
            Self::NumericAbs => {
                "numeric -> checked promoted numeric; decimal precision/scale retained"
            }
            Self::NumericRound => "numeric -> checked promoted numeric; decimal scale becomes zero",
            Self::DecimalOutput => {
                "decimal or integer operands and literal precision/scale/rounding -> Decimal128; one exact-ratio rounding step and checked final precision"
            }
            Self::Utf8 => "UTF8 arguments -> UTF8",
            Self::Utf8Offsets => "UTF8, integer offsets/lengths -> UTF8; Unicode code points",
            Self::Utf8Length => "UTF8 -> Int64 Unicode code-point count",
            Self::Utf8Predicate => "UTF8 arguments -> Boolean",
            Self::ByteLength => "UTF8 or binary -> Int64 byte count",
            Self::BinaryDecode => "UTF8 -> binary; malformed encoded input is an error",
            Self::EpochMinute => "integer epoch seconds or timestamp text -> integer minute",
            Self::EpochTruncate => {
                "integer epoch seconds or timestamp text -> minute-truncated value of that family"
            }
            Self::DateField | Self::DateDifference => {
                "Date32 (or explicitly parsed ISO text) -> Int64"
            }
            Self::TimestampField | Self::TimestampDifference => {
                "naive TimestampMicros (or explicitly parsed ISO text) -> Int64"
            }
            Self::DateOffset => "Date32 and integer days -> Date32 with checked overflow",
            Self::TimestampOffset => {
                "naive TimestampMicros and integer seconds -> TimestampMicros with checked overflow"
            }
            Self::Temporal => {
                "typed temporal operands and literal calendar policies -> exact metadata-preserving result; pinned timezone rules, checked precision and overflow"
            }
            Self::JsonText => {
                "UTF8 JSON and literal path -> nullable UTF8 JSON text, preserving string quotes and JSON null; missing path -> SQL null"
            }
            Self::TimestampParse => "UTF8 and literal format -> naive TimestampMicros",
            Self::Coalesce => "compatible selected operands -> shared native common type",
            Self::NullIf => "comparable selected operands -> first operand type",
            Self::Conditional => {
                "Boolean condition and compatible branches -> shared native common branch type"
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScalarNullRule {
    Propagate,
    AlwaysNullable,
    AllNullable,
    Branches,
}

impl ScalarNullRule {
    #[must_use]
    pub const fn contract(self) -> &'static str {
        match self {
            Self::Propagate => "null input produces null; nullable if any input is nullable",
            Self::AlwaysNullable => "may produce null even with non-null inputs",
            Self::AllNullable => "first non-null value; nullable only if all inputs are nullable",
            Self::Branches => {
                "TRUE selects first branch; FALSE/NULL selects second; branch nullability"
            }
        }
    }
    #[must_use]
    pub const fn behavior(self) -> NullBehavior {
        match self {
            Self::Propagate => NullBehavior::NullPropagating,
            _ => NullBehavior::NullAware,
        }
    }
}

#[derive(Debug)]
pub struct ScalarDefinition {
    pub name: &'static str,
    pub aliases: &'static [&'static str],
    pub min_args: usize,
    pub max_args: usize,
    pub type_rule: ScalarTypeRule,
    pub null_rule: ScalarNullRule,
    pub family: &'static str,
    pub reference_supported: bool,
    pub summary: &'static str,
}

impl ScalarDefinition {
    #[must_use]
    pub fn accepts_arity(&self, count: usize) -> bool {
        (self.min_args..=self.max_args).contains(&count)
    }
}

macro_rules! definitions {
    ($(($name:literal, [$($alias:literal),*], $min:literal, $max:literal, $types:ident, $nulls:ident, $family:literal, $reference:literal, $summary:literal)),* $(,)?) => {
        pub const SCALARS: &[ScalarDefinition] = &[$(ScalarDefinition {
            name: $name, aliases: &[$($alias),*], min_args: $min, max_args: $max,
            type_rule: ScalarTypeRule::$types, null_rule: ScalarNullRule::$nulls,
            family: $family, reference_supported: $reference, summary: $summary,
        }),*];
    };
}

definitions!(
    (
        "decimal_rescale",
        [],
        4,
        4,
        DecimalOutput,
        Propagate,
        "decimal_output",
        true,
        "Rescale an exact decimal or integer to an explicit decimal output domain."
    ),
    (
        "decimal_add",
        [],
        5,
        5,
        DecimalOutput,
        Propagate,
        "decimal_output",
        true,
        "Add exact decimal or integer operands and round once to the explicit output domain."
    ),
    (
        "decimal_subtract",
        [],
        5,
        5,
        DecimalOutput,
        Propagate,
        "decimal_output",
        true,
        "Subtract exact decimal or integer operands and round once to the explicit output domain."
    ),
    (
        "decimal_multiply",
        [],
        5,
        5,
        DecimalOutput,
        Propagate,
        "decimal_output",
        true,
        "Multiply exact decimal or integer operands and round once to the explicit output domain."
    ),
    (
        "decimal_divide",
        [],
        5,
        5,
        DecimalOutput,
        Propagate,
        "decimal_output",
        true,
        "Divide exact decimal or integer operands and round once to the explicit output domain."
    ),
    (
        "temporal_extract",
        [],
        2,
        2,
        Temporal,
        Propagate,
        "temporal_calendar",
        false,
        "Extract a literal calendar field from a typed date, time or timestamp."
    ),
    (
        "temporal_trunc",
        [],
        2,
        3,
        Temporal,
        Propagate,
        "temporal_calendar",
        false,
        "Truncate a typed date, time or timestamp to a literal calendar boundary."
    ),
    (
        "temporal_bucket",
        [],
        3,
        6,
        Temporal,
        Propagate,
        "temporal_calendar",
        false,
        "Label an anchored half-open temporal bucket with exact elapsed or explicit calendar spacing."
    ),
    (
        "calendar_add_days",
        [],
        2,
        3,
        Temporal,
        Propagate,
        "temporal_calendar",
        false,
        "Add local calendar days with an explicit optional gap/fold policy."
    ),
    (
        "calendar_add_months",
        [],
        2,
        4,
        Temporal,
        Propagate,
        "temporal_calendar",
        false,
        "Add calendar months with explicit optional month-end and gap/fold policies."
    ),
    (
        "temporal_add_duration",
        [],
        2,
        2,
        Temporal,
        Propagate,
        "temporal_arithmetic",
        false,
        "Add a typed fixed duration exactly, retaining the left operand's unit."
    ),
    (
        "temporal_sub_duration",
        [],
        2,
        2,
        Temporal,
        Propagate,
        "temporal_arithmetic",
        false,
        "Subtract a typed fixed duration exactly, retaining the left operand's unit."
    ),
    (
        "timestamp_difference",
        [],
        2,
        2,
        Temporal,
        Propagate,
        "temporal_arithmetic",
        false,
        "Subtract timestamps with identical metadata to produce a typed duration."
    ),
    (
        "timestamp_assign_timezone",
        [],
        2,
        3,
        Temporal,
        Propagate,
        "temporal_calendar",
        false,
        "Resolve naive civil ticks in a declared timezone, rejecting ambiguity by default."
    ),
    (
        "timestamp_convert_timezone",
        [],
        2,
        2,
        Temporal,
        Propagate,
        "temporal_calendar",
        false,
        "Change an instant's presentation timezone without changing its ticks."
    ),
    (
        "timestamp_remove_timezone",
        [],
        1,
        1,
        Temporal,
        Propagate,
        "temporal_calendar",
        false,
        "Remove a timezone while preserving the instant's local civil fields."
    ),
    (
        "abs",
        ["numeric_abs"],
        1,
        1,
        NumericAbs,
        Propagate,
        "numeric_abs",
        true,
        "Checked absolute value."
    ),
    (
        "floor",
        ["numeric_floor"],
        1,
        1,
        NumericRound,
        Propagate,
        "numeric_rounding",
        true,
        "Round toward negative infinity."
    ),
    (
        "ceil",
        ["ceiling", "numeric_ceil"],
        1,
        1,
        NumericRound,
        Propagate,
        "numeric_rounding",
        true,
        "Round toward positive infinity."
    ),
    (
        "round",
        ["numeric_round"],
        1,
        1,
        NumericRound,
        Propagate,
        "numeric_rounding",
        true,
        "Round to an integer; halfway values away from zero."
    ),
    (
        "lower",
        ["utf8_lower"],
        1,
        1,
        Utf8,
        Propagate,
        "string_transform",
        true,
        "Unicode lowercase."
    ),
    (
        "upper",
        ["utf8_upper"],
        1,
        1,
        Utf8,
        Propagate,
        "string_transform",
        true,
        "Unicode uppercase."
    ),
    (
        "trim",
        ["utf8_trim"],
        1,
        1,
        Utf8,
        Propagate,
        "string_transform",
        true,
        "Trim Unicode whitespace."
    ),
    (
        "length",
        ["utf8_length"],
        1,
        1,
        Utf8Length,
        Propagate,
        "string_length",
        true,
        "Count Unicode code points."
    ),
    (
        "starts_with",
        ["utf8_starts_with"],
        2,
        2,
        Utf8Predicate,
        Propagate,
        "string_predicate",
        true,
        "Exact UTF8 prefix comparison."
    ),
    (
        "ends_with",
        ["utf8_ends_with"],
        2,
        2,
        Utf8Predicate,
        Propagate,
        "string_predicate",
        true,
        "Exact UTF8 suffix comparison."
    ),
    (
        "contains",
        ["utf8_contains"],
        2,
        2,
        Utf8Predicate,
        Propagate,
        "string_predicate",
        true,
        "Exact UTF8 substring comparison."
    ),
    (
        "regex_match",
        ["utf8_regex_match", "rlike", "regexp", "regexp_like"],
        2,
        2,
        Utf8Predicate,
        Propagate,
        "string_predicate",
        true,
        "Match a bounded literal Rust-regex pattern."
    ),
    (
        "concat",
        ["utf8_concat"],
        2,
        128,
        Utf8,
        Propagate,
        "string_function",
        true,
        "Concatenate UTF8 values; null propagates."
    ),
    (
        "substr",
        ["substring", "utf8_substr", "utf8_substring"],
        3,
        3,
        Utf8Offsets,
        Propagate,
        "string_function",
        true,
        "Substring with native SQL start/length semantics."
    ),
    (
        "left",
        ["utf8_left"],
        2,
        2,
        Utf8Offsets,
        Propagate,
        "string_function",
        true,
        "Take a UTF8 prefix by code-point length."
    ),
    (
        "right",
        ["utf8_right"],
        2,
        2,
        Utf8Offsets,
        Propagate,
        "string_function",
        true,
        "Take a UTF8 suffix by code-point length."
    ),
    (
        "replace",
        ["utf8_replace"],
        3,
        3,
        Utf8,
        Propagate,
        "string_function",
        true,
        "Replace exact substrings."
    ),
    (
        "byte_length",
        ["binary_byte_length", "octet_length"],
        1,
        1,
        ByteLength,
        Propagate,
        "binary_byte_length",
        true,
        "Count UTF8/binary bytes."
    ),
    (
        "unhex",
        ["binary_unhex"],
        1,
        1,
        BinaryDecode,
        Propagate,
        "binary_function",
        true,
        "Decode hexadecimal text."
    ),
    (
        "from_base64",
        ["binary_from_base64"],
        1,
        1,
        BinaryDecode,
        Propagate,
        "binary_function",
        true,
        "Decode Base64 text."
    ),
    (
        "url_domain",
        [],
        1,
        1,
        Utf8,
        Propagate,
        "function",
        false,
        "Extract a URL domain using the admitted native contract."
    ),
    (
        "regex_domain",
        ["regex_replace_domain"],
        1,
        1,
        Utf8,
        Propagate,
        "function",
        false,
        "Apply the fixed HTTP(S) domain capture."
    ),
    (
        "regexp_replace",
        ["regex_replace"],
        3,
        3,
        Utf8,
        Propagate,
        "function",
        false,
        "Only the fixed HTTP(S) domain pattern and capture are admitted."
    ),
    (
        "extract_minute",
        ["minute"],
        1,
        1,
        EpochMinute,
        Propagate,
        "function",
        false,
        "Extract minute from epoch seconds or timestamp text."
    ),
    (
        "date_trunc_minute",
        ["truncate_minute"],
        1,
        1,
        EpochTruncate,
        Propagate,
        "function",
        false,
        "Truncate epoch seconds or timestamp text to a minute."
    ),
    (
        "date_year",
        ["year"],
        1,
        1,
        DateField,
        Propagate,
        "date_extract",
        true,
        "Extract Date32 calendar year."
    ),
    (
        "date_month",
        ["month"],
        1,
        1,
        DateField,
        Propagate,
        "date_extract",
        true,
        "Extract Date32 calendar month."
    ),
    (
        "date_day",
        ["day"],
        1,
        1,
        DateField,
        Propagate,
        "date_extract",
        true,
        "Extract Date32 day of month."
    ),
    (
        "timestamp_year",
        [],
        1,
        1,
        TimestampField,
        Propagate,
        "timestamp_extract",
        true,
        "Extract naive microsecond timestamp year."
    ),
    (
        "timestamp_month",
        [],
        1,
        1,
        TimestampField,
        Propagate,
        "timestamp_extract",
        true,
        "Extract naive microsecond timestamp month."
    ),
    (
        "timestamp_day",
        [],
        1,
        1,
        TimestampField,
        Propagate,
        "timestamp_extract",
        true,
        "Extract naive microsecond timestamp day."
    ),
    (
        "timestamp_hour",
        [],
        1,
        1,
        TimestampField,
        Propagate,
        "timestamp_extract",
        true,
        "Extract naive microsecond timestamp hour."
    ),
    (
        "timestamp_minute",
        [],
        1,
        1,
        TimestampField,
        Propagate,
        "timestamp_extract",
        true,
        "Extract naive microsecond timestamp minute."
    ),
    (
        "timestamp_second",
        [],
        1,
        1,
        TimestampField,
        Propagate,
        "timestamp_extract",
        true,
        "Extract naive microsecond timestamp second."
    ),
    (
        "date_add_days",
        [],
        2,
        2,
        DateOffset,
        Propagate,
        "date_arithmetic",
        true,
        "Add whole days with checked overflow."
    ),
    (
        "date_sub_days",
        [],
        2,
        2,
        DateOffset,
        Propagate,
        "date_arithmetic",
        true,
        "Subtract whole days with checked overflow."
    ),
    (
        "timestamp_add_seconds",
        [],
        2,
        2,
        TimestampOffset,
        Propagate,
        "timestamp_arithmetic",
        true,
        "Add seconds while preserving timestamp microseconds."
    ),
    (
        "timestamp_sub_seconds",
        [],
        2,
        2,
        TimestampOffset,
        Propagate,
        "timestamp_arithmetic",
        true,
        "Subtract seconds while preserving timestamp microseconds."
    ),
    (
        "date_diff_days",
        [],
        2,
        2,
        DateDifference,
        Propagate,
        "temporal_difference",
        true,
        "Subtract Date32 values in days."
    ),
    (
        "timestamp_diff_seconds",
        [],
        2,
        2,
        TimestampDifference,
        Propagate,
        "temporal_difference",
        true,
        "Subtract timestamps and truncate whole seconds toward zero."
    ),
    (
        "coalesce",
        [],
        1,
        128,
        Coalesce,
        AllNullable,
        "null_coalesce",
        true,
        "Select the first non-null compatible value."
    ),
    (
        "nullif",
        [],
        2,
        2,
        NullIf,
        AlwaysNullable,
        "nullif_projection",
        true,
        "Return null when comparable values are equal."
    ),
    (
        "case_when",
        [],
        3,
        3,
        Conditional,
        Branches,
        "conditional_projection",
        true,
        "Select a compatible branch with SQL three-valued condition semantics."
    ),
    (
        "json_extract",
        [],
        2,
        2,
        JsonText,
        AlwaysNullable,
        "function",
        false,
        "Extract an admitted JSON scalar path; absent paths yield SQL null."
    ),
    (
        "strptime",
        [],
        2,
        2,
        TimestampParse,
        Propagate,
        "function",
        false,
        "Parse an admitted literal timestamp format; malformed values fail."
    ),
    (
        "try_strptime",
        [],
        2,
        2,
        TimestampParse,
        AlwaysNullable,
        "function",
        false,
        "Parse an admitted literal timestamp format; malformed values yield null."
    ),
);

#[must_use]
pub fn scalar(name: &str) -> Option<&'static ScalarDefinition> {
    let name = name.trim();
    SCALARS.iter().find(|definition| {
        definition.name.eq_ignore_ascii_case(name)
            || definition
                .aliases
                .iter()
                .any(|alias| alias.eq_ignore_ascii_case(name))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn definitions_have_unique_names_bounded_arities_and_stable_aliases() {
        let mut names = std::collections::BTreeSet::new();
        for definition in SCALARS {
            assert!(
                definition.min_args > 0
                    && definition.min_args <= definition.max_args
                    && definition.max_args <= 128
            );
            for name in std::iter::once(&definition.name).chain(definition.aliases) {
                assert!(
                    names.insert(name.to_ascii_lowercase()),
                    "duplicate function alias {name}"
                );
                assert_eq!(
                    scalar(&name.to_ascii_uppercase()).unwrap().name,
                    definition.name
                );
            }
        }
        assert!(scalar("external_evaluator").is_none());
    }
}
