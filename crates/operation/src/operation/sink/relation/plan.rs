use std::collections::{HashMap, HashSet, VecDeque};

use dogpaddle_change::Change;

use super::{
    Batch, Delete, EXHAUSTED_ID, Insert, Lookup, MAX_MUTATIONS_PER_BATCH, MAX_TECHNICAL_ID,
    Matches, RelationTarget, canonical_row_bounded, invalid, validate_next_id,
};
use crate::operation::{OperationError, sink::buffered::DeliveryBatch};

struct RowPlan {
    row_index: usize,
    net: i128,
    needed: u64,
    take: usize,
    ids: VecDeque<u64>,
}

struct Span {
    row_index: usize,
    group: usize,
    insert: bool,
    take: u64,
}

pub(super) const MAX_CANONICAL_BATCH_BYTES: usize = 8 * 1024 * 1024;

pub(super) fn prepare(
    target: &mut impl RelationTarget,
    input: &DeliveryBatch,
    next_id: u64,
) -> Result<(u64, Batch), OperationError> {
    validate_next_id(next_id)?;
    let change = input.change();
    let events = change
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

    let mut groups = Vec::<RowPlan>::new();
    let mut keys = HashMap::<Vec<u8>, usize>::new();
    let mut spans = Vec::with_capacity(change.num_rows());
    let mut next_id_after = next_id;
    let canonical_rows = bounded_canonical_rows(change)?;

    for (row_index, canonical) in canonical_rows.into_iter().enumerate() {
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
        let take = change.diffs().value(row_index).unsigned_abs();
        let admission = input.admission(row_index);
        let insert = change.diffs().value(row_index) > 0;
        if insert {
            // The first slice reserves the whole remaining multiplicity before
            // any visible target mutation. Later slices were admitted already.
            if admission > EXHAUSTED_ID - next_id_after {
                return Err(invalid(format!(
                    "technical ID range from {next_id_after} cannot reserve {admission} inserts"
                )));
            }
            next_id_after += take;
            row.net += i128::from(take);
        } else {
            // The first visible slice similarly proves the complete negative
            // event is admissible before any of its deletions are delivered.
            let needed = u64::try_from((i128::from(admission) - row.net).max(0))
                .map_err(|_| invalid("retraction count overflows u64"))?;
            if needed > MAX_TECHNICAL_ID {
                return Err(invalid("retraction exceeds the maximum target row count"));
            }
            row.needed = row.needed.max(needed);
            row.take = row
                .take
                .checked_add(usize::try_from(take).expect("the bounded batch fits usize"))
                .ok_or_else(|| invalid("relation lookup count exceeds usize"))?;
            row.net -= i128::from(take);
        }
        spans.push(Span {
            row_index,
            group,
            insert,
            take,
        });
    }

    lookup(target, change, next_id, &mut groups)?;

    let mut batch = Batch {
        inserts: Vec::new(),
        deletes: Vec::new(),
    };
    let mut allocated = next_id;
    for span in spans {
        let ids = &mut groups[span.group].ids;
        for _ in 0..span.take {
            if span.insert {
                batch.inserts.push(Insert {
                    row_index: u64::try_from(span.row_index)
                        .expect("an addressable row index fits u64"),
                    technical_id: allocated,
                });
                ids.push_back(allocated);
                allocated += 1;
            } else {
                batch.deletes.push(Delete {
                    row_index: u64::try_from(span.row_index)
                        .expect("an addressable row index fits u64"),
                    technical_id: ids.pop_front().ok_or_else(|| {
                        invalid("target returned too few IDs for an admitted row")
                    })?,
                });
            }
        }
    }
    debug_assert_eq!(allocated, next_id_after);
    Ok((next_id_after, batch))
}

fn lookup(
    target: &mut impl RelationTarget,
    input: &Change,
    next_id: u64,
    groups: &mut [RowPlan],
) -> Result<(), OperationError> {
    let requests = groups
        .iter()
        .filter(|row| row.take != 0)
        .map(|row| Lookup {
            row_index: row.row_index,
            needed: row
                .needed
                .max(u64::try_from(row.take).expect("the batch limit fits u64")),
            take: row.take,
        })
        .collect::<Vec<_>>();
    if requests.is_empty() {
        return Ok(());
    }

    let matches = target.lookup(input, &requests)?;
    if matches.len() != requests.len() {
        return Err(invalid(
            "target returned a different number of lookup results",
        ));
    }
    for ((row, request), found) in groups
        .iter_mut()
        .filter(|row| row.take != 0)
        .zip(&requests)
        .zip(matches)
    {
        validate_matches(request, &found, next_id)?;
        if found.count < row.needed {
            return Err(invalid(format!(
                "row {} needs {} existing instances, but only {} exist",
                row.row_index, row.needed, found.count
            )));
        }
        row.ids = found.ids.into();
    }
    Ok(())
}

fn validate_matches(request: &Lookup, found: &Matches, next_id: u64) -> Result<(), OperationError> {
    let selected = usize::try_from(
        found
            .count
            .min(u64::try_from(request.take).expect("the batch limit fits u64")),
    )
    .expect("the batch limit fits usize");
    if found.count > request.needed
        || found.ids.len() != selected
        || found.ids.iter().any(|id| *id == 0 || *id >= next_id)
        || found.ids.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(invalid("target returned invalid matching row IDs or count"));
    }
    Ok(())
}

/// Validates a recovered immutable plan against its exact reconstructed batch.
pub(super) fn validate(batch: &Batch, next_id: u64, input: &Change) -> Result<(), OperationError> {
    validate_next_id(next_id)?;
    let count = batch
        .inserts
        .len()
        .checked_add(batch.deletes.len())
        .ok_or_else(|| invalid("relation-plan mutation count exceeds usize"))?;
    let input_count = input
        .diffs()
        .values()
        .iter()
        .try_fold(0_u64, |total, diff| total.checked_add(diff.unsigned_abs()))
        .ok_or_else(|| invalid("relation batch event count exceeds u64"))?;
    if count == 0
        || count > MAX_MUTATIONS_PER_BATCH
        || u64::try_from(count).expect("the bounded count fits u64") != input_count
    {
        return Err(invalid("prepared relation plan does not cover its input"));
    }

    let first_id = next_id
        .checked_sub(u64::try_from(batch.inserts.len()).expect("the bounded count fits u64"))
        .filter(|id| *id > 0)
        .ok_or_else(|| invalid("invalid insert ID frontier"))?;
    if batch.inserts.iter().enumerate().any(|(index, insert)| {
        insert.technical_id != first_id + u64::try_from(index).expect("the bounded index fits u64")
    }) || batch
        .deletes
        .iter()
        .any(|delete| delete.technical_id == 0 || delete.technical_id >= next_id)
        || batch
            .deletes
            .iter()
            .map(|delete| delete.technical_id)
            .collect::<HashSet<_>>()
            .len()
            != batch.deletes.len()
    {
        return Err(invalid("invalid prepared technical IDs"));
    }

    let canonical_rows = bounded_canonical_rows(input)?;
    let mut inserts = batch.inserts.iter().rev();
    let mut deletions = batch.deletes.iter().rev();
    for row in (0..input.num_rows()).rev() {
        for _ in 0..input.diffs().value(row).unsigned_abs() {
            if input.diffs().value(row) > 0 {
                if inserts.next().map(|insert| insert.row_index)
                    != Some(u64::try_from(row).expect("an addressable row index fits u64"))
                {
                    return Err(invalid("prepared inserts do not match the input"));
                }
            } else {
                let delete = deletions
                    .next()
                    .ok_or_else(|| invalid("missing prepared deletion"))?;
                if delete.row_index
                    != u64::try_from(row).expect("an addressable row index fits u64")
                {
                    return Err(invalid("prepared deletions do not match the input"));
                }
                let id = delete.technical_id;
                if id >= first_id {
                    let insert = &batch.inserts
                        [usize::try_from(id - first_id).expect("the inserted ID is bounded")];
                    let source = usize::try_from(insert.row_index)
                        .map_err(|_| invalid("invalid insert row"))?;
                    if source >= row {
                        return Err(invalid(
                            "deletion cannot consume a later or different insert",
                        ));
                    }
                    if canonical_rows[source] != canonical_rows[row] {
                        return Err(invalid(
                            "deletion cannot consume a later or different insert",
                        ));
                    }
                }
            }
        }
    }
    if inserts.next().is_some() || deletions.next().is_some() {
        return Err(invalid("prepared mutations do not match the input"));
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
