//! Output rows and corrections, shared by preflight validation and emission.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchOptions};
use arrow_schema::SchemaRef;
use datafusion_common::ScalarValue;
use dogpaddle_change::Change;
use dogpaddle_store::MultisetEntry;

use crate::operation::relation::decode_canonical_row;

use super::{
    ActiveRow, EquiJoinError, EquiJoinKind, EquiJoinOperation, KeyTransition, MatchTransition,
    PreparedMatch, RowEffect,
};

pub(super) struct OutputRows {
    columns: Vec<Vec<ScalarValue>>,
    differences: Vec<i64>,
}

impl EquiJoinOperation {
    pub(super) fn validate_output_page(
        &self,
        port: usize,
        input: &ActiveRow<'_>,
        effect: RowEffect,
        matches: &[MultisetEntry<Vec<u8>>],
    ) -> Result<(), EquiJoinError> {
        let mut output = OutputRows::new(self.output_schema.fields().len());
        self.append_output_page(port, input, effect, matches, &mut output)?;
        output.finish(&self.output_schema).map(|_| ())
    }

    pub(super) fn append_output_page(
        &self,
        port: usize,
        input: &ActiveRow<'_>,
        effect: RowEffect,
        matches: &[MultisetEntry<Vec<u8>>],
        output: &mut OutputRows,
    ) -> Result<(), EquiJoinError> {
        if self.kind.left_only() {
            return self.append_existence_output(port, input, effect, matches, output);
        }
        if matches.is_empty() && !effect.matched && self.kind.preserves(port) {
            self.append_padded(port, input.values()?, input.difference, output);
        }
        if matches.is_empty() {
            return Ok(());
        }
        let input_values = input.values()?;
        let opposite_schema = &self.input_schemas[1 - port];
        for matched in matches {
            let opposite =
                decode_canonical_row(opposite_schema, &matched.key).map_err(|source| {
                    EquiJoinError::CanonicalRow {
                        source: Box::new(source),
                    }
                })?;
            let difference =
                output_difference(i128::from(input.difference) * i128::from(matched.multiplicity))?;
            // A match and its NULL-row correction share one cursor position
            // and transaction, including when both have identical values.
            if self.kind.preserves(1 - port) && matches!(effect.transition, KeyTransition::First) {
                self.append_padded(
                    1 - port,
                    &opposite,
                    output_difference(-i128::from(matched.multiplicity))?,
                    output,
                );
            }
            if port == 0 {
                output.push(input_values, &opposite, difference);
            } else {
                output.push(&opposite, input_values, difference);
            }
            if self.kind.preserves(1 - port) && matches!(effect.transition, KeyTransition::Last) {
                self.append_padded(
                    1 - port,
                    &opposite,
                    output_difference(i128::from(matched.multiplicity))?,
                    output,
                );
            }
        }
        Ok(())
    }

    fn append_existence_output(
        &self,
        port: usize,
        input: &ActiveRow<'_>,
        effect: RowEffect,
        matches: &[MultisetEntry<Vec<u8>>],
        output: &mut OutputRows,
    ) -> Result<(), EquiJoinError> {
        let semi = self.kind == EquiJoinKind::LeftSemi;
        if port == 0 {
            if effect.matched == semi {
                output.push(input.values()?, &[], input.difference);
            }
            return Ok(());
        }
        let sign = match effect.transition {
            KeyTransition::First => 1_i128,
            KeyTransition::Last => -1,
            KeyTransition::None => return Ok(()),
        } * if semi { 1 } else { -1 };
        for matched in matches {
            let left =
                decode_canonical_row(&self.input_schemas[0], &matched.key).map_err(|source| {
                    EquiJoinError::CanonicalRow {
                        source: Box::new(source),
                    }
                })?;
            output.push(
                &left,
                &[],
                output_difference(sign * i128::from(matched.multiplicity))?,
            );
        }
        Ok(())
    }

    fn append_padded(
        &self,
        port: usize,
        values: &[ScalarValue],
        difference: i64,
        output: &mut OutputRows,
    ) {
        if port == 0 {
            output.push(values, &self.nulls[1], difference);
        } else {
            output.push(&self.nulls[0], values, difference);
        }
    }

    pub(super) fn append_residual_match_output(
        &self,
        port: usize,
        input: &ActiveRow<'_>,
        effect: RowEffect,
        matched: &PreparedMatch,
        transition: MatchTransition,
        output: &mut OutputRows,
    ) -> Result<(), EquiJoinError> {
        if transition == MatchTransition::BecameMatched {
            self.append_match_correction(1 - port, matched, true, output)?;
        }
        if !self.kind.left_only() {
            let difference =
                output_difference(i128::from(input.difference) * i128::from(matched.multiplicity))?;
            let input_values = input.values()?;
            if port == 0 {
                output.push(input_values, &matched.values, difference);
            } else {
                output.push(&matched.values, input_values, difference);
            }
        }
        if transition == MatchTransition::BecameUnmatched {
            self.append_match_correction(1 - port, matched, false, output)?;
        }
        debug_assert!(
            transition == MatchTransition::None
                || !matches!(effect.transition, KeyTransition::None)
        );
        Ok(())
    }

    fn append_match_correction(
        &self,
        port: usize,
        matched: &PreparedMatch,
        now_matched: bool,
        output: &mut OutputRows,
    ) -> Result<(), EquiJoinError> {
        let magnitude = i128::from(matched.multiplicity);
        if self.kind.preserves(port) {
            let difference = output_difference(if now_matched { -magnitude } else { magnitude })?;
            self.append_padded(port, &matched.values, difference, output);
        } else if self.kind.left_only() && port == 0 {
            let semi = self.kind == EquiJoinKind::LeftSemi;
            let positive = now_matched == semi;
            let difference = output_difference(if positive { magnitude } else { -magnitude })?;
            output.push(&matched.values, &[], difference);
        } else {
            return Err(EquiJoinError::InvalidMatchCount(
                "match transition targeted an untracked side",
            ));
        }
        Ok(())
    }

    pub(super) fn validate_residual_current(
        &self,
        port: usize,
        input: &ActiveRow<'_>,
        found_match: bool,
    ) -> Result<(), EquiJoinError> {
        let mut output = OutputRows::new(self.output_schema.fields().len());
        self.append_residual_current(port, input, found_match, &mut output)?;
        output.finish(&self.output_schema).map(|_| ())
    }

    pub(super) fn append_residual_current(
        &self,
        port: usize,
        input: &ActiveRow<'_>,
        found_match: bool,
        output: &mut OutputRows,
    ) -> Result<(), EquiJoinError> {
        if self.kind.left_only() && port == 0 {
            let semi = self.kind == EquiJoinKind::LeftSemi;
            if found_match == semi {
                output.push(input.values()?, &[], input.difference);
            }
        } else if self.kind.preserves(port) && !found_match {
            self.append_padded(port, input.values()?, input.difference, output);
        }
        Ok(())
    }
}

fn output_difference(difference: i128) -> Result<i64, EquiJoinError> {
    i64::try_from(difference).map_err(|_| EquiJoinError::OutputDifferenceOverflow)
}

impl OutputRows {
    pub(super) fn new(column_count: usize) -> Self {
        Self {
            columns: (0..column_count).map(|_| Vec::new()).collect(),
            differences: Vec::new(),
        }
    }

    fn push(&mut self, left: &[ScalarValue], right: &[ScalarValue], difference: i64) {
        debug_assert_eq!(self.columns.len(), left.len() + right.len());
        for (column, value) in self.columns.iter_mut().zip(left.iter().chain(right)) {
            column.push(value.clone());
        }
        self.differences.push(difference);
    }

    pub(super) fn finish(self, schema: &SchemaRef) -> Result<Option<Change>, EquiJoinError> {
        if self.differences.is_empty() {
            return Ok(None);
        }
        let row_count = self.differences.len();
        let columns = self
            .columns
            .into_iter()
            .map(ScalarValue::iter_to_array)
            .collect::<Result<Vec<ArrayRef>, _>>()?;
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        let records = RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options)?;
        Ok(Some(Change::try_new(
            records,
            Int64Array::from(self.differences),
        )?))
    }
}
