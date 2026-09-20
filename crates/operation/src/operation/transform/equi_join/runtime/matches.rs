//! Bounded candidate scans and residual evaluation; no durable writes.

use std::{mem::size_of, sync::Arc};

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, RecordBatchOptions};
use datafusion_common::ScalarValue;
use dogpaddle_store::{
    MultisetEntry, MultisetPage, ScanDirection, ScanLimit, StoreError, TransactionAccess,
};

use crate::operation::relation::decode_canonical_row;

use super::{
    ActiveRow, EquiJoinError, EquiJoinOperation, KeyTransition, PreparedMatch, PreparedRow,
    ResidualPage, RowEffect, TurnBudget,
};

const RESIDUAL_BATCH_ITEMS: usize = 256;
const RESIDUAL_BATCH_BYTES: usize = 1024 * 1024;
const RESIDUAL_BATCH_SCALAR_VALUES: usize = 16 * 1024;

fn residual_batch_item_limit(candidate_fields: usize) -> usize {
    (RESIDUAL_BATCH_SCALAR_VALUES / candidate_fields.max(1)).clamp(1, RESIDUAL_BATCH_ITEMS)
}

impl EquiJoinOperation {
    pub(super) fn scan_matches(
        &self,
        port: usize,
        row: &PreparedRow,
        effect: RowEffect,
        resume_after: Option<&Vec<u8>>,
        budget: &TurnBudget,
        access: TransactionAccess<'_>,
    ) -> Result<Option<MultisetPage<Vec<u8>>>, EquiJoinError> {
        if !effect.matched
            || (self.kind.left_only()
                && (port == 0 || matches!(effect.transition, KeyTransition::None)))
        {
            return Ok(Some(MultisetPage {
                entries: Vec::new(),
                continuation: None,
            }));
        }
        let mut opposite = self.rows(1 - port).access(access)?;
        let partition = opposite.partition(&row.key)?;
        let expanded =
            self.kind.preserves(1 - port) && !matches!(effect.transition, KeyTransition::None);
        let repeats_input = !(self.kind.left_only() && port == 1);
        let max_items = budget.max_scan_items(row, expanded, repeats_input);
        let max_bytes = budget.scan_bytes();
        let limit =
            ScanLimit::new(max_items, max_bytes).expect("positive Join page limits are valid");
        match partition.scan(ScanDirection::Ascending, resume_after, limit) {
            Ok(page) => Ok(Some(page)),
            Err(StoreError::ItemTooLarge { .. }) if !budget.is_empty() => Ok(None),
            Err(StoreError::ItemTooLarge { size, .. }) => {
                let limit = ScanLimit::new(1, size.max(1))
                    .expect("one item and a positive observed byte size are valid");
                Ok(Some(partition.scan(
                    ScanDirection::Ascending,
                    resume_after,
                    limit,
                )?))
            }
            Err(source) => Err(source.into()),
        }
    }

    pub(super) fn scan_residual_matches(
        &self,
        port: usize,
        row: &ActiveRow<'_>,
        effect: RowEffect,
        resume_after: Option<&Vec<u8>>,
        budget: &TurnBudget,
        access: TransactionAccess<'_>,
    ) -> Result<Option<ResidualPage>, EquiJoinError> {
        if !row.matchable
            || (self.kind.left_only() && matches!(effect.transition, KeyTransition::None))
        {
            return Ok(Some(ResidualPage {
                matches: Vec::new(),
                qualifying: 0,
                continuation: None,
                work: TurnBudget::work(row, &[], true),
            }));
        }
        let mut opposite = self.rows(1 - port).access(access)?;
        let partition = opposite.partition(&row.key)?;
        let expanded = !self.kind.left_only()
            && self.tracks_match_count(1 - port)
            && !matches!(effect.transition, KeyTransition::None);
        let max_items = budget
            .max_scan_items(row, expanded, true)
            .min(residual_batch_item_limit(
                self.candidate_schema.fields().len(),
            ));
        let max_bytes = budget.scan_bytes().min(RESIDUAL_BATCH_BYTES);
        let limit = ScanLimit::new(max_items, max_bytes)
            .expect("positive residual Join page limits are valid");
        let page = match partition.scan(ScanDirection::Ascending, resume_after, limit) {
            Ok(page) => page,
            Err(StoreError::ItemTooLarge { .. }) if !budget.is_empty() => return Ok(None),
            Err(StoreError::ItemTooLarge { size, .. }) => {
                let limit = ScanLimit::new(1, size.max(1))
                    .expect("one item and a positive observed byte size are valid");
                partition.scan(ScanDirection::Ascending, resume_after, limit)?
            }
            Err(source) => return Err(source.into()),
        };
        let work = self.residual_work(port, row, effect, &page.entries);
        if !budget.can_accept(work) {
            return Ok(None);
        }
        let (matches, qualifying) = self.evaluate_residual_matches(port, row, page.entries)?;
        Ok(Some(ResidualPage {
            matches,
            qualifying,
            continuation: page.continuation,
            work,
        }))
    }

    fn evaluate_residual_matches(
        &self,
        port: usize,
        input: &ActiveRow<'_>,
        entries: Vec<MultisetEntry<Vec<u8>>>,
    ) -> Result<(Vec<PreparedMatch>, usize), EquiJoinError> {
        if entries.is_empty() {
            return Ok((Vec::new(), 0));
        }
        let opposite_schema = &self.input_schemas[1 - port];
        let left_fields = self.input_schemas[0].fields().len();
        let input_values = input.values()?;
        let mut columns = (0..self.candidate_schema.fields().len())
            .map(|_| Vec::with_capacity(entries.len()))
            .collect::<Vec<Vec<ScalarValue>>>();
        let mut candidates = Vec::with_capacity(entries.len());
        for entry in entries {
            let opposite = decode_canonical_row(opposite_schema, &entry.key).map_err(|source| {
                EquiJoinError::CanonicalRow {
                    source: Box::new(source),
                }
            })?;
            if port == 0 {
                for (column, value) in columns[..left_fields].iter_mut().zip(input_values) {
                    column.push(value.clone());
                }
                for (column, value) in columns[left_fields..].iter_mut().zip(opposite) {
                    column.push(value);
                }
            } else {
                for (column, value) in columns[..left_fields].iter_mut().zip(opposite) {
                    column.push(value);
                }
                for (column, value) in columns[left_fields..].iter_mut().zip(input_values) {
                    column.push(value.clone());
                }
            }
            candidates.push((entry.key, entry.multiplicity));
        }
        let arrays = columns
            .into_iter()
            .map(ScalarValue::iter_to_array)
            .collect::<Result<Vec<ArrayRef>, _>>()?;
        let options = RecordBatchOptions::new().with_row_count(Some(candidates.len()));
        let records = RecordBatch::try_new_with_options(
            Arc::clone(&self.candidate_schema),
            arrays,
            &options,
        )?;
        let predicate = self
            .residual
            .as_ref()
            .expect("the residual path has a bound residual")
            .evaluate(&records)
            .map_err(|source| EquiJoinError::ResidualExpression { source })?;
        let predicate = predicate
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or(EquiJoinError::ResidualArray)?;
        if predicate.len() != candidates.len() {
            return Err(EquiJoinError::ResidualArray);
        }
        let opposite_columns = if port == 0 {
            left_fields..records.num_columns()
        } else {
            0..left_fields
        };
        let mut matches = Vec::new();
        let mut qualifying = 0_usize;
        for (index, (row, multiplicity)) in candidates.into_iter().enumerate() {
            if predicate.is_valid(index) && predicate.value(index) {
                qualifying = qualifying
                    .checked_add(1)
                    .ok_or(EquiJoinError::MatchCountOverflow)?;
                if self.kind.left_only() && port == 0 {
                    continue;
                }
                let values = opposite_columns
                    .clone()
                    .map(|column| {
                        ScalarValue::try_from_array(records.column(column).as_ref(), index)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                matches.push(PreparedMatch {
                    row,
                    values,
                    multiplicity,
                });
            }
        }
        Ok((matches, qualifying))
    }

    fn residual_work(
        &self,
        port: usize,
        row: &PreparedRow,
        effect: RowEffect,
        candidates: &[MultisetEntry<Vec<u8>>],
    ) -> (usize, usize) {
        // Every raw candidate is materialized to evaluate the predicate, even
        // when it is filtered out and produces no relational output.
        let (mut items, mut bytes) = TurnBudget::work(row, candidates, true);
        let scalar_slots_per_candidate = self
            .candidate_schema
            .fields()
            .len()
            .saturating_add(self.input_schemas[1 - port].fields().len())
            .saturating_add(self.output_schema.fields().len().saturating_mul(2));
        bytes = bytes.saturating_add(
            candidates
                .len()
                .saturating_mul(scalar_slots_per_candidate)
                .saturating_mul(size_of::<ScalarValue>()),
        );
        if !self.kind.left_only()
            && self.tracks_match_count(1 - port)
            && !matches!(effect.transition, KeyTransition::None)
        {
            items = items.saturating_add(candidates.len());
            for candidate in candidates {
                bytes = bytes.saturating_add(candidate.key.len()).saturating_add(
                    self.nulls[port]
                        .len()
                        .saturating_mul(size_of::<ScalarValue>()),
                );
            }
        }
        (items, bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::{RESIDUAL_BATCH_ITEMS, RESIDUAL_BATCH_SCALAR_VALUES, residual_batch_item_limit};

    #[test]
    fn residual_batch_limit_accounts_for_candidate_schema_width() {
        assert_eq!(residual_batch_item_limit(0), RESIDUAL_BATCH_ITEMS);
        assert_eq!(residual_batch_item_limit(1), RESIDUAL_BATCH_ITEMS);
        assert_eq!(
            residual_batch_item_limit(RESIDUAL_BATCH_SCALAR_VALUES / RESIDUAL_BATCH_ITEMS),
            RESIDUAL_BATCH_ITEMS
        );
        assert!(
            residual_batch_item_limit(RESIDUAL_BATCH_SCALAR_VALUES / RESIDUAL_BATCH_ITEMS + 1)
                < RESIDUAL_BATCH_ITEMS
        );
        assert_eq!(residual_batch_item_limit(RESIDUAL_BATCH_SCALAR_VALUES), 1);
        assert_eq!(residual_batch_item_limit(usize::MAX), 1);
    }
}
