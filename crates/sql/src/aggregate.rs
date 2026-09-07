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

struct Builtin {
    name: &'static str,
    udf: fn() -> Arc<AggregateUDF>,
    lower: fn(Vec<Expr>) -> Option<AggregateCall>,
}

const BUILTINS: &[Builtin] = &[
    Builtin {
        name: "count",
        udf: count_udaf,
        lower: lower_count,
    },
    Builtin {
        name: "sum",
        udf: sum_udaf,
        lower: lower_sum,
    },
    Builtin {
        name: "avg",
        udf: avg_udaf,
        lower: lower_avg,
    },
    Builtin {
        name: "min",
        udf: min_udaf,
        lower: lower_min,
    },
    Builtin {
        name: "max",
        udf: max_udaf,
        lower: lower_max,
    },
];

pub(crate) fn planning_builtins() -> HashMap<&'static str, Arc<AggregateUDF>> {
    BUILTINS
        .iter()
        .map(|builtin| (builtin.name, (builtin.udf)()))
        .collect()
}

pub(crate) fn lower(name: &str, arguments: Vec<Expr>) -> Option<AggregateCall> {
    let builtin = BUILTINS.iter().find(|builtin| builtin.name == name)?;
    (builtin.lower)(arguments)
}

fn lower_count(arguments: Vec<Expr>) -> Option<AggregateCall> {
    let [argument] = arguments.try_into().ok()?;
    Some(
        if matches!(&argument, Expr::Literal(value, _) if value == &COUNT_STAR_EXPANSION) {
            AggregateCall::count_all()
        } else {
            AggregateCall::count(argument)
        },
    )
}

fn lower_sum(arguments: Vec<Expr>) -> Option<AggregateCall> {
    lower_unary(arguments, AggregateCall::sum)
}

fn lower_avg(arguments: Vec<Expr>) -> Option<AggregateCall> {
    lower_unary(arguments, AggregateCall::avg)
}

fn lower_min(arguments: Vec<Expr>) -> Option<AggregateCall> {
    lower_unary(arguments, AggregateCall::min)
}

fn lower_max(arguments: Vec<Expr>) -> Option<AggregateCall> {
    lower_unary(arguments, AggregateCall::max)
}

fn lower_unary(
    arguments: Vec<Expr>,
    constructor: fn(Expr) -> AggregateCall,
) -> Option<AggregateCall> {
    let [argument] = arguments.try_into().ok()?;
    Some(constructor(argument))
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
