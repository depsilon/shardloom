//! Relational reducer semantics. Legacy single-primitive providers keep their
//! narrower admission set and map explicitly into this shared relation family.

use super::{SimpleAggregateFunction, native_relational_batch::failed};
use crate::query_primitive::VortexQuantileLevel;
use shardloom_core::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Function {
    Count,
    CountDistinct,
    Sum,
    Avg,
    SumDistinct,
    AvgDistinct,
    Min,
    Max,
    VarPop,
    VarSamp,
    StddevPop,
    StddevSamp,
    CovarPop,
    CovarSamp,
    Corr,
    Median,
    QuantileCont,
    QuantileDisc,
    First,
    Last,
    ArgMin,
    ArgMax,
    List,
    StringAgg,
}

impl From<SimpleAggregateFunction> for Function {
    fn from(value: SimpleAggregateFunction) -> Self {
        match value {
            SimpleAggregateFunction::Count => Self::Count,
            SimpleAggregateFunction::CountDistinct => Self::CountDistinct,
            SimpleAggregateFunction::Sum => Self::Sum,
            SimpleAggregateFunction::Avg => Self::Avg,
            SimpleAggregateFunction::Min => Self::Min,
            SimpleAggregateFunction::Max => Self::Max,
        }
    }
}

impl Function {
    pub(super) const fn is_legacy(self) -> bool {
        matches!(
            self,
            Self::Count | Self::CountDistinct | Self::Sum | Self::Avg | Self::Min | Self::Max
        )
    }

    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::CountDistinct => "count_distinct",
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::SumDistinct => "sum_distinct",
            Self::AvgDistinct => "avg_distinct",
            Self::Min => "min",
            Self::Max => "max",
            Self::VarPop => "var_pop",
            Self::VarSamp => "var_samp",
            Self::StddevPop => "stddev_pop",
            Self::StddevSamp => "stddev_samp",
            Self::CovarPop => "covar_pop",
            Self::CovarSamp => "covar_samp",
            Self::Corr => "corr",
            Self::Median => "median",
            Self::QuantileCont => "quantile_cont",
            Self::QuantileDisc => "quantile_disc",
            Self::First => "first",
            Self::Last => "last",
            Self::ArgMin => "arg_min",
            Self::ArgMax => "arg_max",
            Self::List => "list",
            Self::StringAgg => "string_agg",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "count" => Ok(Self::Count),
            "count_distinct" | "count-distinct" | "distinct_count" | "distinct-count" => {
                Ok(Self::CountDistinct)
            }
            "sum" => Ok(Self::Sum),
            "avg" | "mean" => Ok(Self::Avg),
            "sum_distinct" => Ok(Self::SumDistinct),
            "avg_distinct" | "mean_distinct" => Ok(Self::AvgDistinct),
            "min" => Ok(Self::Min),
            "max" => Ok(Self::Max),
            "var_pop" => Ok(Self::VarPop),
            "var_samp" | "variance" | "var" => Ok(Self::VarSamp),
            "stddev_pop" => Ok(Self::StddevPop),
            "stddev_samp" | "stddev" | "std" => Ok(Self::StddevSamp),
            "covar_pop" => Ok(Self::CovarPop),
            "covar_samp" => Ok(Self::CovarSamp),
            "corr" => Ok(Self::Corr),
            "median" => Ok(Self::Median),
            "quantile_cont" => Ok(Self::QuantileCont),
            "quantile_disc" => Ok(Self::QuantileDisc),
            "first" => Ok(Self::First),
            "last" => Ok(Self::Last),
            "arg_min" => Ok(Self::ArgMin),
            "arg_max" => Ok(Self::ArgMax),
            "list" | "array_agg" => Ok(Self::List),
            "string_agg" => Ok(Self::StringAgg),
            other => Err(failed(&format!(
                "native aggregate function '{other}' is not admitted"
            ))),
        }
    }

    pub(super) const fn is_moment(self) -> bool {
        matches!(
            self,
            Self::VarPop
                | Self::VarSamp
                | Self::StddevPop
                | Self::StddevSamp
                | Self::CovarPop
                | Self::CovarSamp
                | Self::Corr
        )
    }

    pub(super) const fn is_quantile(self) -> bool {
        matches!(self, Self::Median | Self::QuantileCont | Self::QuantileDisc)
    }

    pub(super) const fn is_selector(self) -> bool {
        matches!(self, Self::First | Self::Last | Self::ArgMin | Self::ArgMax)
    }

    pub(super) const fn is_collection(self) -> bool {
        matches!(self, Self::List | Self::StringAgg)
    }

    pub(super) const fn is_ordered_value(self) -> bool {
        self.is_selector() || self.is_collection()
    }

    pub(super) fn validate_separator(self, separator: Option<&str>) -> Result<()> {
        if (self == Self::StringAgg) != separator.is_some() {
            return Err(failed(
                "string_agg requires a literal separator; other reducers do not accept one",
            ));
        }
        Ok(())
    }

    pub(super) const fn requires_secondary(self) -> bool {
        self.is_pair() || matches!(self, Self::ArgMin | Self::ArgMax)
    }

    pub(super) fn quantile_level(
        self,
        requested: Option<VortexQuantileLevel>,
    ) -> Result<Option<VortexQuantileLevel>> {
        match (self, requested) {
            (Self::Median, None) => VortexQuantileLevel::new(0.5).map(Some),
            (Self::QuantileCont | Self::QuantileDisc, Some(level)) => Ok(Some(level)),
            (Self::QuantileCont | Self::QuantileDisc, None) => Err(failed(
                "quantile_cont and quantile_disc require a scalar quantile level",
            )),
            (_, Some(_)) => Err(failed(
                "only quantile_cont and quantile_disc accept a quantile level",
            )),
            (_, None) => Ok(None),
        }
    }

    pub(super) const fn is_distinct(self) -> bool {
        matches!(
            self,
            Self::CountDistinct | Self::SumDistinct | Self::AvgDistinct
        )
    }

    pub(super) const fn is_average(self) -> bool {
        matches!(self, Self::Avg | Self::AvgDistinct)
    }

    pub(super) const fn is_pair(self) -> bool {
        matches!(self, Self::CovarPop | Self::CovarSamp | Self::Corr)
    }

    pub(super) const fn sample(self) -> bool {
        matches!(self, Self::VarSamp | Self::StddevSamp | Self::CovarSamp)
    }

    pub(super) const fn standard_deviation(self) -> bool {
        matches!(self, Self::StddevPop | Self::StddevSamp)
    }
}
