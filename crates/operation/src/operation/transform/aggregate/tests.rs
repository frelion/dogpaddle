//! Private coverage of the sufficient-statistics layout.

use super::{AggregateCall, AggregateDefinition, functions::StatisticKind, runtime::BoundCall};
use crate::col;
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

#[test]
fn repeated_calls_share_one_argument_statistic_and_extrema_layout() {
    let definition = AggregateDefinition::try_new(
        [("group", col("group"))],
        [
            ("rows", AggregateCall::CountAll),
            ("count", AggregateCall::Count(col("value"))),
            ("sum", AggregateCall::Sum(col("value"))),
            ("avg", AggregateCall::Avg(col("value"))),
            ("count_again", AggregateCall::Count(col("value"))),
            ("min", AggregateCall::Min(col("value"))),
            ("max", AggregateCall::Max(col("value"))),
            ("min_again", AggregateCall::Min(col("value"))),
        ],
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("group", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let mut fields = Vec::new();
    let bound = definition.bind_calls(&schema, &mut fields).unwrap();
    assert_eq!(bound.arguments.len(), 1);
    assert_eq!(bound.statistics.len(), 1);
    assert_eq!(bound.statistics[0].kind, StatisticKind::Signed);
    assert!(bound.statistics[0].count_output);
    assert!(bound.statistics[0].sum_output);
    assert_eq!(bound.layouts.len(), 1);
    assert_eq!(bound.slots.len(), 2);
    assert!(matches!(bound.calls[0], BoundCall::RowsCount));
    assert!(matches!(bound.calls[4], BoundCall::Count { statistic: 0 }));
    assert!(matches!(bound.calls[7], BoundCall::Extrema { slot: 0 }));
}
