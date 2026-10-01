use std::{collections::HashMap, sync::Arc};

use arrow_schema::DataType;
use datafusion_common::{Result, internal_err, plan_err, utils::expr::COUNT_STAR_EXPANSION};
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Expr, Signature, Volatility,
    function::AccumulatorArgs,
};
use datafusion_functions_aggregate::{
    count::count_udaf,
    min_max::{max_udaf, min_udaf},
    sum::sum_udaf,
};
use dogpaddle_operation::operation::transform::AggregateCall;

pub(crate) fn planning_builtins() -> HashMap<&'static str, Arc<AggregateUDF>> {
    HashMap::from([
        ("count", count_udaf()),
        ("sum", sum_udaf()),
        ("avg", avg_udaf()),
        ("min", min_udaf()),
        ("max", max_udaf()),
    ])
}

pub(crate) fn lower(name: &str, arguments: Vec<Expr>) -> Option<AggregateCall> {
    let [argument] = arguments.try_into().ok()?;
    Some(match name {
        "count" if matches!(&argument, Expr::Literal(value, _) if value == &COUNT_STAR_EXPANSION) => {
            AggregateCall::CountAll
        }
        "count" => AggregateCall::Count(argument),
        "sum" => AggregateCall::Sum(argument),
        "avg" => AggregateCall::Avg(argument),
        "min" => AggregateCall::Min(argument),
        "max" => AggregateCall::Max(argument),
        _ => return None,
    })
}

fn avg_udaf() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(SqlAvg::new()))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SqlAvg {
    signature: Signature,
}

impl SqlAvg {
    fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Int64, DataType::UInt64],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for SqlAvg {
    fn name(&self) -> &'static str {
        "avg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, argument_types: &[DataType]) -> Result<DataType> {
        if matches!(argument_types, [DataType::Int64 | DataType::UInt64]) {
            Ok(DataType::Float64)
        } else {
            plan_err!("avg accepts one Int64 or UInt64 argument")
        }
    }

    fn accumulator(&self, _arguments: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        internal_err!("dogpaddle-sql does not build DataFusion physical aggregate plans")
    }
}
