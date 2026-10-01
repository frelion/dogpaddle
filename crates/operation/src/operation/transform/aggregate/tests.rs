//! Private coverage of the sufficient-statistics layout.

use super::{AggregateCall, AggregateDefinition, functions::StatisticKind};
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
    let bound = definition.compile_layout(&schema).unwrap();
    assert_eq!(bound.arguments.len(), 1);
    assert_eq!(bound.statistic_count, 1);
    let statistic = bound.arguments[0].statistic.as_ref().unwrap();
    assert_eq!(statistic.index, 0);
    assert_eq!(statistic.kind, StatisticKind::Signed);
    assert!(statistic.count_output);
    assert!(statistic.sum_output);
    assert_eq!(bound.layout_count, 1);
    assert_eq!(bound.extrema_count, 2);
    let extrema = bound.arguments[0].extrema.as_ref().unwrap();
    assert_eq!(extrema.partition, 0);
    assert_eq!(extrema.min_slot, Some(0));
    assert_eq!(extrema.max_slot, Some(1));
    assert!(matches!(bound.calls[0], AggregateCall::CountAll));
    assert!(matches!(bound.calls[4], AggregateCall::Count(0)));
    assert!(matches!(bound.calls[7], AggregateCall::Min(0)));
}

#[test]
fn addresses_follow_each_roles_first_use_after_min_first() {
    let definition = AggregateDefinition::try_new(
        [("group", col("group"))],
        [
            ("min_a", AggregateCall::Min(col("a"))),
            ("sum_b", AggregateCall::Sum(col("b"))),
            ("count_a", AggregateCall::Count(col("a"))),
            ("max_c", AggregateCall::Max(col("c"))),
            ("min_b", AggregateCall::Min(col("b"))),
            ("sum_a", AggregateCall::Sum(col("a"))),
            ("min_c", AggregateCall::Min(col("c"))),
            ("max_a", AggregateCall::Max(col("a"))),
        ],
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("group", DataType::Int64, false),
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::UInt64, true),
        Field::new("c", DataType::Utf8, true),
    ]));
    let bound = definition.compile_layout(&schema).unwrap();
    assert_eq!(bound.arguments.len(), 3);
    assert_eq!(
        (
            bound.statistic_count,
            bound.layout_count,
            bound.extrema_count
        ),
        (2, 3, 5)
    );
    for (index, partition, min_slot, max_slot) in [
        (0, 0, Some(0), Some(4)),
        (1, 2, Some(2), None),
        (2, 1, Some(3), Some(1)),
    ] {
        let extrema = bound.arguments[index].extrema.as_ref().unwrap();
        assert_eq!(
            (extrema.partition, extrema.min_slot, extrema.max_slot),
            (partition, min_slot, max_slot)
        );
    }
    let a = bound.arguments[0].statistic.as_ref().unwrap();
    let b = bound.arguments[1].statistic.as_ref().unwrap();
    assert_eq!((a.index, a.kind), (1, StatisticKind::Signed));
    assert_eq!((b.index, b.kind), (0, StatisticKind::Unsigned));
    assert!(a.count_output && a.sum_output && b.sum_output);
    assert!(!b.count_output);
    assert!(bound.arguments[2].statistic.is_none());
}

#[test]
fn statistical_roles_upgrade_count_and_never_downgrade_numeric_state() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("group", DataType::Int64, false),
        Field::new("value", DataType::UInt64, true),
    ]));
    let count = AggregateCall::Count(col("value"));
    let sum = AggregateCall::Sum(col("value"));
    let avg = AggregateCall::Avg(col("value"));
    for calls in [
        [count.clone(), sum.clone(), avg.clone()],
        [avg.clone(), count.clone(), sum.clone()],
        [sum, avg, count],
    ] {
        let definition = AggregateDefinition::try_new(
            [("group", col("group"))],
            ["first", "second", "third"].into_iter().zip(calls),
        )
        .unwrap();
        let bound = definition.compile_layout(&schema).unwrap();
        assert_eq!((bound.arguments.len(), bound.statistic_count), (1, 1));
        assert_eq!((bound.layout_count, bound.extrema_count), (0, 0));
        let statistic = bound.arguments[0].statistic.as_ref().unwrap();
        assert_eq!(
            (statistic.index, statistic.kind),
            (0, StatisticKind::Unsigned)
        );
        assert!(statistic.count_output && statistic.sum_output);
    }
}
