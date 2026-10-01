use std::collections::{HashMap, HashSet, VecDeque};

use dogpaddle_change::Change;

use super::{
    Batch, Delete, Insert, Lookup, MAX_MUTATIONS_PER_BATCH, Matches, canonical_row_bounded,
    canonical_row_size_bounded, invalid, validate_technical_id,
};
use crate::operation::{OperationError, sink::buffered::DeliveryBatch};

struct RowPlan {
    row_index: usize,
    take: usize,
    through: u64,
    ids: VecDeque<u64>,
}

pub(super) const MAX_CANONICAL_BATCH_BYTES: usize = 8 * 1024 * 1024;

pub(crate) fn plan(
    input: &DeliveryBatch,
    from: u64,
    tail: u64,
    original_head: (u64, &Change),
    lookup: impl FnOnce(&[Lookup]) -> Result<Vec<Matches>, OperationError>,
) -> Result<Batch, OperationError> {
    let events = validate_input(input)?;
    let first = input.first_event_offset();
    let end = input.end_event_offset()?;
    if from < first || from > tail || end > tail || original_head.0 == 0 || original_head.0 > first
    {
        return Err(invalid("invalid delivery prefix bounds"));
    }
    let remaining = usize::try_from(end.saturating_sub(from)).expect("bounded events fit usize");
    let change = input.change();
    if change.diffs().values().iter().all(|diff| *diff > 0) {
        check_canonical_budget(change)?;
        return Ok(derive_inserts(input, from, remaining));
    }

    let (batch, mut births) = {
        let mut groups = Vec::<RowPlan>::new();
        let mut keys = HashMap::<Vec<u8>, usize>::new();
        let mut row_groups = Vec::with_capacity(change.num_rows());
        let mut event_offset = first;
        for (row_index, canonical) in bounded_canonical_rows(change)?.into_iter().enumerate() {
            let group = *keys.entry(canonical).or_insert_with(|| {
                let index = groups.len();
                groups.push(RowPlan {
                    row_index,
                    take: 0,
                    through: 0,
                    ids: VecDeque::new(),
                });
                index
            });
            let difference = change.diffs().value(row_index);
            let row_end = event_offset + difference.unsigned_abs();
            if difference < 0 {
                groups[group].take +=
                    usize::try_from(row_end.saturating_sub(from.max(event_offset)))
                        .expect("bounded events fit usize");
            }
            event_offset = row_end;
            row_groups.push(group);
        }
        lookup_groups(&mut groups, tail, lookup)?;
        let negatives = groups.iter().map(|group| group.take).sum::<usize>();
        let mut batch = Batch {
            inserts: Vec::with_capacity(remaining - negatives),
            deletes: Vec::with_capacity(negatives),
        };
        let mut births = Vec::new();
        let mut event_offset = first;
        for (row_index, group) in row_groups.into_iter().enumerate() {
            let row = &mut groups[group];
            let difference = change.diffs().value(row_index);
            let runtime_row = u64::try_from(row_index).expect("an addressable row index fits u64");
            for _ in 0..difference.unsigned_abs() {
                if event_offset >= from && event_offset > row.through {
                    if difference > 0 {
                        row.ids.push_back(event_offset);
                        batch.inserts.push(Insert {
                            row_index: runtime_row,
                            technical_id: event_offset,
                        });
                    } else {
                        let id = row.ids.pop_front().ok_or_else(|| {
                            invalid("target returned too few IDs for the remaining row prefix")
                        })?;
                        if id >= event_offset {
                            return Err(invalid("deletion cannot consume a later event"));
                        }
                        if id >= original_head.0 && id <= row.through {
                            births.push((id, row_index));
                        }
                        batch.deletes.push(Delete {
                            row_index: runtime_row,
                            technical_id: id,
                            event_offset,
                        });
                    }
                }
                event_offset += 1;
            }
        }
        (batch, births)
    };
    births.sort_unstable_by_key(|(id, _)| *id);
    let split = births.partition_point(|(id, _)| *id < first);
    validate_retained_births(original_head.0, original_head.1, &births[..split], change)?;
    // Keep the original Delivery: known births may lie in a skipped F/p prefix.
    validate_retained_births(first, change, &births[split..], change)?;
    debug_assert!(batch.inserts.len() + batch.deletes.len() <= events);
    Ok(batch)
}

fn derive_inserts(input: &DeliveryBatch, from: u64, events: usize) -> Batch {
    let mut inserts = Vec::with_capacity(events);
    let mut event_offset = input.first_event_offset();
    for row in 0..input.change().num_rows() {
        for _ in 0..input.change().diffs().value(row).unsigned_abs() {
            if event_offset >= from {
                inserts.push(Insert {
                    row_index: u64::try_from(row).expect("an addressable row index fits u64"),
                    technical_id: event_offset,
                });
            }
            event_offset += 1;
        }
    }
    Batch {
        inserts,
        deletes: Vec::new(),
    }
}

fn lookup_groups(
    groups: &mut [RowPlan],
    tail: u64,
    lookup: impl FnOnce(&[Lookup]) -> Result<Vec<Matches>, OperationError>,
) -> Result<(), OperationError> {
    let requests = groups
        .iter()
        .filter(|row| row.take != 0)
        .map(|row| Lookup {
            row_index: row.row_index,
            take: row.take,
        })
        .collect::<Vec<_>>();
    if requests.is_empty() {
        return Ok(());
    }
    let matches = lookup(&requests)?;
    if matches.len() != requests.len() {
        return Err(invalid(
            "target returned a different number of lookup results",
        ));
    }
    let mut unique = HashSet::new();
    for ((row, request), found) in groups
        .iter_mut()
        .filter(|row| row.take != 0)
        .zip(&requests)
        .zip(matches)
    {
        validate_matches(request, &found, tail)?;
        if found.ids.iter().any(|id| !unique.insert(*id)) {
            return Err(invalid("target returned an ID for multiple logical rows"));
        }
        row.through = found.through;
        row.ids = found.ids.into();
    }
    Ok(())
}

pub(super) fn validate_matches(
    request: &Lookup,
    found: &Matches,
    tail: u64,
) -> Result<(), OperationError> {
    if found.through >= tail
        || found.ids.len() > request.take
        || found.ids.iter().any(|id| *id == 0 || *id > found.through)
        || found.ids.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(invalid(
            "target returned invalid matching row progress or IDs",
        ));
    }
    Ok(())
}

// Merge at most 1024 sorted IDs into one scan of retained diff intervals. Only
// hit birth rows have their canonical size checked. Arrow compares the complete
// values through borrowed slices, including float bits and nested null semantics,
// without allocating another canonical payload. Compare each row pair once.
fn validate_retained_births(
    start: u64,
    head: &Change,
    prior: &[(u64, usize)],
    current: &Change,
) -> Result<(), OperationError> {
    let mut row = 0;
    let mut end = start;
    let mut checked = HashSet::new();
    let mut compared = HashSet::new();
    for &(id, current_row) in prior {
        while end <= id {
            if row >= head.num_rows() {
                return Err(invalid("matching ID is outside the retained head entry"));
            }
            end = end
                .checked_add(head.diffs().value(row).unsigned_abs())
                .ok_or_else(|| invalid("retained head event range exceeds u64"))?;
            row += 1;
        }
        let source = row - 1;
        if head.diffs().value(source) <= 0 {
            return Err(invalid("matching ID was born in a retained negative event"));
        }
        if !compared.insert((source, current_row)) {
            continue;
        }
        if checked.insert(source) {
            canonical_row_size_bounded(head.records(), source, MAX_CANONICAL_BATCH_BYTES)?;
        }
        if head.records().slice(source, 1) != current.records().slice(current_row, 1) {
            return Err(invalid(
                "matching ID belongs to a different retained birth row",
            ));
        }
    }
    Ok(())
}

fn validate_input(input: &DeliveryBatch) -> Result<usize, OperationError> {
    validate_technical_id(input.first_event_offset())?;
    let events = input
        .change()
        .diffs()
        .values()
        .iter()
        .try_fold(0_u64, |total, diff| total.checked_add(diff.unsigned_abs()))
        .ok_or_else(|| invalid("relation batch event count exceeds u64"))?;
    if events == 0
        || events > u64::try_from(MAX_MUTATIONS_PER_BATCH).expect("the batch limit fits u64")
    {
        return Err(invalid("relation batch exceeds its mutation limit"));
    }
    input
        .first_event_offset()
        .checked_add(events)
        .ok_or_else(|| invalid("relation batch event range exceeds u64"))?;
    Ok(usize::try_from(events).expect("bounded events fit usize"))
}

fn check_canonical_budget(input: &Change) -> Result<(), OperationError> {
    let mut remaining = MAX_CANONICAL_BATCH_BYTES;
    for row in 0..input.num_rows() {
        remaining -= canonical_row_size_bounded(input.records(), row, remaining)?;
    }
    Ok(())
}

fn bounded_canonical_rows(input: &Change) -> Result<Vec<Vec<u8>>, OperationError> {
    let mut remaining = MAX_CANONICAL_BATCH_BYTES;
    let mut rows = Vec::with_capacity(input.num_rows());
    for row in 0..input.num_rows() {
        let encoded = canonical_row_bounded(input.records(), row, remaining)?;
        remaining -= encoded.len();
        rows.push(encoded);
    }
    Ok(rows)
}
