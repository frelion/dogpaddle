use std::collections::{HashMap, HashSet, VecDeque};

use dogpaddle_change::Change;

use super::{
    Batch, Delete, Insert, Lookup, MAX_MUTATIONS_PER_BATCH, Matches, RelationTarget,
    canonical_row_bounded, canonical_row_size_bounded, invalid, validate_technical_id,
};
use crate::operation::{OperationError, sink::buffered::DeliveryBatch};

struct RowPlan {
    row_index: usize,
    net: i128,
    needed: u64,
    take: usize,
    ids: VecDeque<u64>,
}

pub(super) const MAX_CANONICAL_BATCH_BYTES: usize = 8 * 1024 * 1024;

pub(crate) fn prepare(
    target: &mut impl RelationTarget,
    input: &DeliveryBatch,
    original_head: (u64, &Change),
) -> Result<Batch, OperationError> {
    let events = validate_input(input)?;
    let change = input.change();
    if change.diffs().values().iter().all(|diff| *diff > 0) {
        check_canonical_budget(change)?;
        return Ok(derive_inserts(input, events));
    }

    let (batch, mut prior) = {
        let mut groups = Vec::<RowPlan>::new();
        let mut keys = HashMap::<Vec<u8>, usize>::new();
        let mut row_groups = Vec::with_capacity(change.num_rows());
        for (row_index, canonical) in bounded_canonical_rows(change)?.into_iter().enumerate() {
            let group = *keys.entry(canonical).or_insert_with(|| {
                let index = groups.len();
                groups.push(RowPlan {
                    row_index,
                    net: 0,
                    needed: 0,
                    take: 0,
                    ids: VecDeque::new(),
                });
                index
            });
            let row = &mut groups[group];
            let difference = change.diffs().value(row_index);
            let take = difference.unsigned_abs();
            if difference > 0 {
                row.net += i128::from(take);
            } else {
                let needed = u64::try_from((i128::from(take) - row.net).max(0))
                    .map_err(|_| invalid("retraction prefix overflows u64"))?;
                row.needed = row.needed.max(needed);
                row.take += usize::try_from(take).expect("the bounded batch fits usize");
                row.net -= i128::from(take);
            }
            row_groups.push(group);
        }
        lookup(target, change, input.first_event_offset(), &mut groups)?;
        let negatives = groups.iter().map(|group| group.take).sum::<usize>();
        let mut batch = Batch {
            inserts: Vec::with_capacity(events - negatives),
            deletes: Vec::with_capacity(negatives),
        };
        let mut prior = Vec::new();
        let mut event_offset = input.first_event_offset();
        // Each queue belongs to one exact row. Guarded old IDs and fresh event
        // offsets enter once and are removed once; persisted IDs need recovery checks.
        for (row_index, group) in row_groups.into_iter().enumerate() {
            let ids = &mut groups[group].ids;
            let difference = change.diffs().value(row_index);
            let runtime_row = u64::try_from(row_index).expect("an addressable row index fits u64");
            for _ in 0..difference.unsigned_abs() {
                if difference > 0 {
                    ids.push_back(event_offset);
                    batch.inserts.push(Insert {
                        row_index: runtime_row,
                        technical_id: event_offset,
                    });
                } else {
                    let id = ids.pop_front().ok_or_else(|| {
                        invalid("target returned too few IDs for an admitted row")
                    })?;
                    if id >= original_head.0 && id < input.first_event_offset() {
                        prior.push((id, row_index));
                    }
                    batch.deletes.push(Delete {
                        row_index: runtime_row,
                        technical_id: id,
                    });
                }
                event_offset += 1;
            }
        }
        (batch, prior)
    };
    validate_retained_births(original_head.0, original_head.1, &mut prior, change)?;
    Ok(batch)
}

fn derive_inserts(input: &DeliveryBatch, events: usize) -> Batch {
    let mut inserts = Vec::with_capacity(events);
    let mut event_offset = input.first_event_offset();
    for row in 0..input.change().num_rows() {
        for _ in 0..input.change().diffs().value(row).unsigned_abs() {
            inserts.push(Insert {
                row_index: u64::try_from(row).expect("an addressable row index fits u64"),
                technical_id: event_offset,
            });
            event_offset += 1;
        }
    }
    Batch {
        inserts,
        deletes: Vec::new(),
    }
}

fn lookup(
    target: &mut impl RelationTarget,
    input: &Change,
    first_event_offset: u64,
    groups: &mut [RowPlan],
) -> Result<(), OperationError> {
    let requests = groups
        .iter()
        .filter(|row| row.take != 0)
        .map(|row| Lookup {
            row_index: row.row_index,
            take: row.take,
        })
        .collect::<Vec<_>>();
    let matches = target.lookup(input, &requests)?;
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
        validate_matches(request, &found, first_event_offset)?;
        if found.ids.iter().any(|id| !unique.insert(*id)) {
            return Err(invalid("target returned an ID for multiple logical rows"));
        }
        if u64::try_from(found.ids.len()).expect("bounded IDs fit u64") < row.needed {
            return Err(invalid(format!(
                "row {} needs {} existing instances, but only {} exist",
                row.row_index,
                row.needed,
                found.ids.len()
            )));
        }
        row.ids = found.ids.into();
    }
    Ok(())
}

pub(super) fn validate_matches(
    request: &Lookup,
    found: &Matches,
    first_event_offset: u64,
) -> Result<(), OperationError> {
    if found.ids.len() > request.take
        || found
            .ids
            .iter()
            .any(|id| *id == 0 || *id >= first_event_offset)
        || found.ids.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(invalid("target returned invalid matching row IDs"));
    }
    Ok(())
}

/// Reconstructs runtime mutations without target lookup or persisted row indexes.
pub(crate) fn recover(
    input: &DeliveryBatch,
    negative_ids: &[u64],
    original_head: (u64, &Change),
) -> Result<Batch, OperationError> {
    let events = validate_input(input)?;
    let change = input.change();
    let negatives = change
        .diffs()
        .values()
        .iter()
        .filter(|diff| **diff < 0)
        .map(|diff| usize::try_from(diff.unsigned_abs()).expect("bounded events fit usize"))
        .sum::<usize>();
    if negative_ids.len() != negatives {
        return Err(invalid("prepared negative IDs do not cover the input"));
    }
    check_canonical_budget(change)?;
    if negatives == 0 {
        return Ok(derive_inserts(input, events));
    }
    let mut unique = HashSet::with_capacity(negatives);
    let mut batch = Batch {
        inserts: Vec::with_capacity(events - negatives),
        deletes: Vec::with_capacity(negatives),
    };
    let mut births = HashMap::<u64, usize>::new();
    let mut comparisons = HashSet::<(usize, usize)>::new();
    let mut prior = Vec::new();
    let mut event_offset = input.first_event_offset();
    let mut deletions = negative_ids.iter();
    for row in 0..change.num_rows() {
        let diff = change.diffs().value(row);
        for _ in 0..diff.unsigned_abs() {
            let row_index = u64::try_from(row).expect("an addressable row index fits u64");
            if diff > 0 {
                births.insert(event_offset, row);
                batch.inserts.push(Insert {
                    row_index,
                    technical_id: event_offset,
                });
            } else {
                let id = *deletions.next().expect("the negative count matches");
                validate_technical_id(id)?;
                if id >= event_offset || !unique.insert(id) {
                    return Err(invalid("invalid prepared negative technical ID"));
                }
                if id >= input.first_event_offset() {
                    let source = births.get(&id).ok_or_else(|| {
                        invalid("deletion cannot consume a negative or later event")
                    })?;
                    comparisons.insert((*source, row));
                } else if id >= original_head.0 {
                    prior.push((id, row));
                }
                batch.deletes.push(Delete {
                    row_index,
                    technical_id: id,
                });
            }
            event_offset += 1;
        }
    }
    for (source, row) in comparisons {
        if change.records().slice(source, 1) != change.records().slice(row, 1) {
            return Err(invalid(
                "deletion consumes an insert belonging to a different row",
            ));
        }
    }
    validate_retained_births(original_head.0, original_head.1, &mut prior, change)?;
    Ok(batch)
}

// Merge at most 1024 sorted IDs into one scan of retained diff intervals. Only
// hit birth rows have their canonical size checked. Arrow compares the complete
// values through borrowed slices, including float bits and nested null semantics,
// without allocating another canonical payload. Compare each row pair once.
fn validate_retained_births(
    start: u64,
    head: &Change,
    prior: &mut [(u64, usize)],
    current: &Change,
) -> Result<(), OperationError> {
    prior.sort_unstable_by_key(|(id, _)| *id);
    let mut row = 0;
    let mut end = start;
    let mut checked = HashSet::new();
    let mut compared = HashSet::new();
    for &(id, current_row) in prior.iter() {
        while end <= id {
            if row >= head.num_rows() {
                return Err(invalid("prepared ID is outside the retained head entry"));
            }
            end = end
                .checked_add(head.diffs().value(row).unsigned_abs())
                .ok_or_else(|| invalid("retained head event range exceeds u64"))?;
            row += 1;
        }
        let source = row - 1;
        if head.diffs().value(source) <= 0 {
            return Err(invalid("prepared ID was born in a retained negative event"));
        }
        if !compared.insert((source, current_row)) {
            continue;
        }
        if checked.insert(source) {
            canonical_row_size_bounded(head.records(), source, MAX_CANONICAL_BATCH_BYTES)?;
        }
        if head.records().slice(source, 1) != current.records().slice(current_row, 1) {
            return Err(invalid(
                "prepared ID belongs to a different retained birth row",
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
