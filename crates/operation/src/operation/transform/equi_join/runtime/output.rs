//! Output rows and presence corrections for the current transactional page.

use std::num::NonZeroU64;

use super::{
    ArrowOutput, EquiJoinError, EquiJoinKind, EquiJoinOperation, KeyTransition, MatchTransition,
    OperationError, PreparedRow, ResidualPage, RowEffect, StepBudget, canonical_error,
};

impl EquiJoinOperation {
    pub(super) fn append_output_page(
        &self,
        port: usize,
        input: &PreparedRow,
        effect: RowEffect,
        matches: &[(Vec<u8>, NonZeroU64)],
        output: &mut ArrowOutput,
        budget: &mut StepBudget,
    ) -> Result<(), EquiJoinError> {
        if self.kind.left_only() {
            return self.append_existence_output(port, input, effect, matches, output, budget);
        }
        if matches.is_empty() && !effect.matched && self.kind.preserves(port) {
            self.append_padded(port, &input.row, input.difference, output, budget)?;
        }
        if matches.is_empty() {
            return Ok(());
        }
        let changes = matches
            .iter()
            .flat_map(|matched| {
                let opposite = matched.0.as_slice();
                let weight = i128::from(matched.1.get());
                let pair = if port == 0 {
                    [Some(input.row.as_slice()), Some(opposite)]
                } else {
                    [Some(opposite), Some(input.row.as_slice())]
                };
                let padding = if port == 0 {
                    [None, Some(opposite)]
                } else {
                    [Some(opposite), None]
                };
                // A match and its NULL-row correction share one cursor position
                // and transaction, including when both have identical values.
                [
                    (self.kind.preserves(1 - port)
                        && matches!(effect.transition, KeyTransition::First))
                    .then_some((padding, -weight)),
                    Some((pair, i128::from(input.difference) * weight)),
                    (self.kind.preserves(1 - port)
                        && matches!(effect.transition, KeyTransition::Last))
                    .then_some((padding, weight)),
                ]
            })
            .flatten()
            .map(|(fragments, difference)| {
                output_difference(difference)
                    .map(|difference| (fragments, difference))
                    .map_err(OperationError::from)
            });
        output
            .extend(&self.input_schemas, changes, budget)
            .map_err(canonical_error)
    }

    fn append_existence_output(
        &self,
        port: usize,
        input: &PreparedRow,
        effect: RowEffect,
        matches: &[(Vec<u8>, NonZeroU64)],
        output: &mut ArrowOutput,
        budget: &mut StepBudget,
    ) -> Result<(), EquiJoinError> {
        let semi = self.kind == EquiJoinKind::LeftSemi;
        if port == 0 {
            if effect.matched == semi {
                self.append(Some(&input.row), None, input.difference, output, budget)?;
            }
            return Ok(());
        }
        let sign = match effect.transition {
            KeyTransition::First => 1_i128,
            KeyTransition::Last => -1,
            KeyTransition::None => return Ok(()),
        } * if semi { 1 } else { -1 };
        let changes = matches.iter().map(|matched| {
            output_difference(sign * i128::from(matched.1.get()))
                .map(|difference| ([Some(matched.0.as_slice()), None], difference))
                .map_err(OperationError::from)
        });
        output
            .extend(&self.input_schemas[..1], changes, budget)
            .map_err(canonical_error)
    }

    fn append(
        &self,
        left: Option<&[u8]>,
        right: Option<&[u8]>,
        difference: i64,
        output: &mut ArrowOutput,
        budget: &mut StepBudget,
    ) -> Result<(), EquiJoinError> {
        let ports = if self.kind.left_only() { 1 } else { 2 };
        output
            .push(
                &self.input_schemas[..ports],
                [left, right],
                difference,
                budget,
            )
            .map_err(canonical_error)
    }

    fn append_padded(
        &self,
        port: usize,
        row: &[u8],
        difference: i64,
        output: &mut ArrowOutput,
        budget: &mut StepBudget,
    ) -> Result<(), EquiJoinError> {
        if port == 0 {
            self.append(Some(row), None, difference, output, budget)
        } else {
            self.append(None, Some(row), difference, output, budget)
        }
    }

    pub(super) fn append_residual_page(
        &self,
        port: usize,
        input: &PreparedRow,
        page: &ResidualPage,
        found_match: bool,
        output: &mut ArrowOutput,
        budget: &mut StepBudget,
    ) -> Result<(), EquiJoinError> {
        let current = if page.continuation.is_some() {
            None
        } else if self.kind.left_only() && port == 0 {
            (found_match == (self.kind == EquiJoinKind::LeftSemi)).then_some((
                [Some(input.row.as_slice()), None],
                i128::from(input.difference),
            ))
        } else {
            (self.kind.preserves(port) && !found_match).then_some((
                if port == 0 {
                    [Some(input.row.as_slice()), None]
                } else {
                    [None, Some(input.row.as_slice())]
                },
                i128::from(input.difference),
            ))
        };
        let changes = page
            .matches
            .iter()
            .flat_map(|matched| {
                let opposite = matched.row.as_slice();
                let weight = i128::from(matched.multiplicity);
                let pair = if port == 0 {
                    [Some(input.row.as_slice()), Some(opposite)]
                } else {
                    [Some(opposite), Some(input.row.as_slice())]
                };
                let correction = if self.kind.preserves(1 - port) {
                    (
                        if port == 0 {
                            [None, Some(opposite)]
                        } else {
                            [Some(opposite), None]
                        },
                        -weight,
                    )
                } else {
                    (
                        [Some(opposite), None],
                        if self.kind == EquiJoinKind::LeftSemi {
                            weight
                        } else {
                            -weight
                        },
                    )
                };
                [
                    (matched.transition == MatchTransition::BecameMatched).then_some(correction),
                    (!self.kind.left_only())
                        .then_some((pair, i128::from(input.difference) * weight)),
                    (matched.transition == MatchTransition::BecameUnmatched)
                        .then_some((correction.0, -correction.1)),
                ]
            })
            .flatten()
            .chain(current)
            .map(|(fragments, difference)| {
                output_difference(difference)
                    .map(|difference| (fragments, difference))
                    .map_err(OperationError::from)
            });
        let ports = if self.kind.left_only() { 1 } else { 2 };
        output
            .extend(&self.input_schemas[..ports], changes, budget)
            .map_err(canonical_error)
    }
}

fn output_difference(difference: i128) -> Result<i64, EquiJoinError> {
    i64::try_from(difference).map_err(|_| EquiJoinError::OutputDifferenceOverflow)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use dogpaddle_store::StoreSetup;

    use super::*;
    use crate::operation::relation::canonical_row_bounded;
    use crate::operation::transform::equi_join::runtime::PreparedMatch;

    #[test]
    fn a_refused_wide_residual_page_appends_no_output_prefix() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            false,
        )]));
        let source = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![
                String::new(),
                "a".repeat(128 * 1024),
                "b".repeat(128 * 1024),
            ]))],
        )
        .unwrap();
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("left", DataType::Utf8, true),
            Field::new("right", DataType::Utf8, true),
        ]));
        let mut setup = StoreSetup::new();
        let mut scope = setup.data_scope();
        let operation = EquiJoinOperation {
            kind: EquiJoinKind::FullOuter,
            input_schemas: [Arc::clone(&schema), schema],
            candidate_schema: Arc::clone(&output_schema),
            output_schema: Arc::clone(&output_schema),
            keys: Box::new([]),
            residual: None,
            left_rows: scope.data("left").unwrap(),
            right_rows: scope.data("right").unwrap(),
            match_counts: None,
        };
        let input = PreparedRow {
            row: canonical_row_bounded(&source, 0, usize::MAX).unwrap(),
            key: Vec::new(),
            matchable: true,
            difference: 1,
        };
        let page = ResidualPage {
            matches: (1..3)
                .map(|row| PreparedMatch {
                    row: canonical_row_bounded(&source, row, usize::MAX).unwrap(),
                    multiplicity: 1,
                    transition: MatchTransition::BecameMatched,
                })
                .collect(),
            qualifying: 2,
            continuation: None,
            items: 2,
        };
        let mut refused = ArrowOutput::default();
        let error = operation
            .append_residual_page(
                0,
                &input,
                &page,
                true,
                &mut refused,
                &mut StepBudget::new(2, 384 * 1024),
            )
            .unwrap_err();
        assert!(matches!(error, EquiJoinError::Budget(_)));
        assert!(refused.finish(&output_schema).unwrap().is_none());
        let mut complete = ArrowOutput::default();
        operation
            .append_residual_page(
                0,
                &input,
                &page,
                true,
                &mut complete,
                &mut StepBudget::new(2, 1024 * 1024),
            )
            .unwrap();
        let output = complete.finish(&output_schema).unwrap().unwrap();
        assert_eq!(output.diffs().values().as_ref(), &[-1, 1, -1, 1]);
        let left = output.records().column(0);
        assert!(left.is_null(0) && left.is_valid(1) && left.is_null(2) && left.is_valid(3));
        let right = output
            .records()
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(right.value(0), right.value(1));
        assert_eq!(right.value(2), right.value(3));
        assert!(right.value(0).starts_with('a') && right.value(2).starts_with('b'));
    }
}
