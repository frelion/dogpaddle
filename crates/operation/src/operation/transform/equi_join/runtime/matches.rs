//! Bounded candidate scans and residual evaluation; no durable writes.

use std::{mem::size_of, num::NonZeroU64};

use arrow_array::{Array, BooleanArray};
use dogpaddle_store::{OrderedMapPage, ScanDirection, ScanLimit, StoreError, TransactionAccess};

use super::{
    ArrowOutput, EquiJoinError, EquiJoinKind, EquiJoinOperation, KeyTransition, MatchTransition,
    PreparedMatch, PreparedRow, ResidualPage, RowEffect, StepBudget, canonical_error,
};

const RESIDUAL_BATCH_ITEMS: usize = 256;
const RESIDUAL_BATCH_BYTES: usize = 1024 * 1024;
const RESIDUAL_BATCH_FIELDS: usize = 16 * 1024;

fn residual_batch_item_limit(candidate_fields: usize) -> usize {
    (RESIDUAL_BATCH_FIELDS / candidate_fields.max(1)).clamp(1, RESIDUAL_BATCH_ITEMS)
}

impl EquiJoinOperation {
    pub(super) fn scan_matches(
        &self,
        port: usize,
        row: &PreparedRow,
        effect: &mut RowEffect,
        resume_after: Option<&Vec<u8>>,
        budget: &mut StepBudget,
        access: TransactionAccess<'_>,
    ) -> Result<Option<OrderedMapPage<Vec<u8>, NonZeroU64>>, EquiJoinError> {
        if !row.matchable
            || (self.kind.left_only()
                && port == 1
                && matches!(effect.transition, KeyTransition::None))
        {
            return Ok(Some(OrderedMapPage {
                entries: Vec::new(),
                continuation: None,
            }));
        }
        let mut opposite = self.rows(1 - port).access(access)?;
        let partition = opposite.partition(&row.key)?;
        if self.kind != EquiJoinKind::Inner {
            // A continuation may be past the last row even when the partition
            // is nonempty; presence is independent of the returned page.
            effect.matched = !partition.is_empty()?;
        }
        if !effect.matched || (self.kind.left_only() && port == 0) {
            return Ok(Some(OrderedMapPage {
                entries: Vec::new(),
                continuation: None,
            }));
        }
        let repeats_input = !(self.kind.left_only() && port == 1);
        let max_items = budget.max_scan_items(row, repeats_input);
        let max_bytes = budget.scan_bytes();
        let limit =
            ScanLimit::new(max_items, max_bytes).expect("positive Join page limits are valid");
        match partition.scan(ScanDirection::Ascending, resume_after, limit) {
            Ok(page) => {
                budget.charge(StepBudget::scanned_bytes(row, &page.entries))?;
                Ok(Some(page))
            }
            Err(StoreError::ItemTooLarge { .. }) => {
                Err(EquiJoinError::Budget(crate::operation::BudgetExceeded))
            }
            Err(source) => Err(source.into()),
        }
    }

    pub(super) fn scan_residual_matches(
        &self,
        port: usize,
        row: &PreparedRow,
        effect: RowEffect,
        resume_after: Option<&Vec<u8>>,
        budget: &mut StepBudget,
        access: TransactionAccess<'_>,
    ) -> Result<Option<ResidualPage>, EquiJoinError> {
        if !row.matchable
            || (self.kind.left_only() && matches!(effect.transition, KeyTransition::None))
        {
            return Ok(Some(ResidualPage {
                matches: Vec::new(),
                qualifying: 0,
                continuation: None,
                items: 1,
            }));
        }
        let mut opposite = self.rows(1 - port).access(access)?;
        let partition = opposite.partition(&row.key)?;
        let max_items = budget
            .max_scan_items(row, true)
            .min(residual_batch_item_limit(
                self.candidate_schema.fields().len(),
            ));
        let max_bytes = budget.scan_bytes().min(RESIDUAL_BATCH_BYTES);
        let limit = ScanLimit::new(max_items, max_bytes)
            .expect("positive residual Join page limits are valid");
        let page = match partition.scan(ScanDirection::Ascending, resume_after, limit) {
            Ok(page) => page,
            Err(StoreError::ItemTooLarge { .. }) => {
                return Err(EquiJoinError::Budget(crate::operation::BudgetExceeded));
            }
            Err(source) => return Err(source.into()),
        };
        budget.charge(StepBudget::scanned_bytes(row, &page.entries))?;
        let work = self.residual_work(port, effect, &page.entries);
        if !budget.can_accept(work) {
            return Ok(None);
        }
        budget.charge(work.1)?;
        let (matches, qualifying) =
            self.evaluate_residual_matches(port, row, page.entries, budget)?;
        Ok(Some(ResidualPage {
            matches,
            qualifying,
            continuation: page.continuation,
            items: work.0,
        }))
    }

    fn evaluate_residual_matches(
        &self,
        port: usize,
        input: &PreparedRow,
        entries: Vec<(Vec<u8>, NonZeroU64)>,
        budget: &mut StepBudget,
    ) -> Result<(Vec<PreparedMatch>, usize), EquiJoinError> {
        if entries.is_empty() {
            return Ok((Vec::new(), 0));
        }
        let mut candidates = ArrowOutput::default();
        let rows = entries.iter().map(|entry| {
            let fragments = if port == 0 {
                [Some(input.row.as_slice()), Some(entry.0.as_slice())]
            } else {
                [Some(entry.0.as_slice()), Some(input.row.as_slice())]
            };
            Ok((fragments, 1))
        });
        candidates
            .extend(&self.input_schemas, rows, budget)
            .map_err(canonical_error)?;
        let records = candidates
            .records(&self.candidate_schema)
            .map_err(canonical_error)?;
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
        if predicate.len() != entries.len() {
            return Err(EquiJoinError::ResidualArray);
        }
        let mut matches = Vec::new();
        let mut qualifying = 0_usize;
        for (index, (row, multiplicity)) in entries.into_iter().enumerate() {
            if predicate.is_valid(index) && predicate.value(index) {
                qualifying = qualifying
                    .checked_add(1)
                    .ok_or(EquiJoinError::MatchCountOverflow)?;
                if self.kind.left_only() && port == 0 {
                    continue;
                }
                matches.push(PreparedMatch {
                    row,
                    multiplicity: multiplicity.get(),
                    transition: MatchTransition::None,
                });
            }
        }
        Ok((matches, qualifying))
    }

    fn residual_work(
        &self,
        port: usize,
        effect: RowEffect,
        candidates: &[(Vec<u8>, NonZeroU64)],
    ) -> (usize, usize) {
        // Every raw candidate is materialized to evaluate the predicate, even
        // when it is filtered out and produces no relational output.
        let items = candidates.len().max(1);
        let mut bytes = 0_usize;
        // Only retained match slots and support reads/writes belong here.
        // Candidate Arrow and every output row are admitted at construction.
        bytes = bytes.saturating_add(candidates.len().saturating_mul(size_of::<PreparedMatch>()));
        if self.tracks_match_count(1 - port) && !matches!(effect.transition, KeyTransition::None) {
            for candidate in candidates {
                bytes = bytes.saturating_add(candidate.0.len().saturating_add(9).saturating_mul(3));
            }
        }
        (items, bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::{RESIDUAL_BATCH_FIELDS, RESIDUAL_BATCH_ITEMS, residual_batch_item_limit};

    #[test]
    fn residual_batch_limit_accounts_for_candidate_schema_width() {
        assert_eq!(residual_batch_item_limit(0), RESIDUAL_BATCH_ITEMS);
        assert_eq!(residual_batch_item_limit(1), RESIDUAL_BATCH_ITEMS);
        assert_eq!(
            residual_batch_item_limit(RESIDUAL_BATCH_FIELDS / RESIDUAL_BATCH_ITEMS),
            RESIDUAL_BATCH_ITEMS
        );
        assert!(
            residual_batch_item_limit(RESIDUAL_BATCH_FIELDS / RESIDUAL_BATCH_ITEMS + 1)
                < RESIDUAL_BATCH_ITEMS
        );
        assert_eq!(residual_batch_item_limit(RESIDUAL_BATCH_FIELDS), 1);
        assert_eq!(residual_batch_item_limit(usize::MAX), 1);
    }
}
