use std::collections::{HashMap, VecDeque};

use dogpaddle_change::Change;

use super::{
    Batch, Continuation, EXHAUSTED_ID, Insert, Lookup, MAX_MUTATIONS_PER_BATCH, MAX_TECHNICAL_ID,
    Matches, Position, RelationTarget, advance_position, canonical_row, invalid, position_index,
    validate_next_id,
};
use crate::operation::OperationError;

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

pub(super) fn prepare(
    target: &mut impl RelationTarget,
    input: &Change,
    next_id: u64,
    start: Position,
) -> Result<(u64, Batch), OperationError> {
    validate_next_id(next_id)?;
    let mut groups = Vec::<RowPlan>::new();
    let mut keys = HashMap::<Vec<u8>, usize>::new();
    let mut spans = Vec::new();
    let mut position = start;
    let mut capacity = MAX_MUTATIONS_PER_BATCH;
    let mut next_id_after = next_id;

    let continuation = loop {
        let row_index = position_index(input, position)?;
        let group = *keys
            .entry(canonical_row(input.records(), row_index)?)
            .or_insert_with(|| {
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
        let take = position
            .remaining
            .min(u64::try_from(capacity).expect("the batch limit fits u64"));
        let insert = input.diffs().value(row_index) > 0;
        if insert {
            if position.remaining > EXHAUSTED_ID - next_id_after {
                return Err(invalid(format!(
                    "technical ID range from {next_id_after} cannot reserve {} inserts",
                    position.remaining
                )));
            }
            next_id_after += take;
            row.net += i128::from(take);
        } else {
            // Admit a whole negative event before its first visible part. A
            // within-row continuation already proves that admission succeeded.
            let required = if position.remaining == input.diffs().value(row_index).unsigned_abs() {
                position.remaining
            } else {
                take
            };
            let needed = u64::try_from((i128::from(required) - row.net).max(0))
                .map_err(|_| invalid("retraction count overflows u64"))?;
            if needed > MAX_TECHNICAL_ID {
                return Err(invalid("retraction exceeds the maximum target row count"));
            }
            row.needed = row.needed.max(needed);
            row.take += usize::try_from(take).expect("the batch limit fits usize");
            row.net -= i128::from(take);
        }
        spans.push(Span {
            row_index,
            group,
            insert,
            take,
        });
        capacity -= usize::try_from(take).expect("the batch limit fits usize");
        match advance_position(input, position, take) {
            Continuation::Done => break Continuation::Done,
            next @ Continuation::Position(_) if capacity == 0 => break next,
            Continuation::Position(next) => position = next,
        }
    };

    lookup(target, input, next_id, &mut groups)?;

    let mut batch = Batch {
        inserts: Vec::new(),
        deletes: Vec::new(),
        continuation,
    };
    let mut allocated = next_id;
    for span in spans {
        let ids = &mut groups[span.group].ids;
        for _ in 0..span.take {
            if span.insert {
                batch.inserts.push(Insert {
                    row_index: u64::try_from(span.row_index).expect("an addressable row fits u64"),
                    technical_id: allocated,
                });
                ids.push_back(allocated);
                allocated += 1;
            } else {
                batch.deletes.push(
                    ids.pop_front().ok_or_else(|| {
                        invalid("target returned too few IDs for an admitted row")
                    })?,
                );
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
    if !requests.is_empty() {
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
