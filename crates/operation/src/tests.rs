use crate::operation::{scan, sink, transform};

#[test]
fn builtin_definition_tags_are_unique_and_stable() {
    let mut tags = [
        scan::mysql_cdc::TAG,
        scan::postgres_cdc::TAG,
        scan::sequence::TAG,
        transform::aggregate::TAG,
        transform::asof_join::TAG,
        transform::distinct::TAG,
        transform::running_event_count::TAG,
        transform::filter::TAG,
        transform::equi_join::TAG,
        transform::select::TAG,
        transform::union_all::TAG,
        transform::schema_align::TAG,
        sink::clickhouse::TAG,
        sink::discard::TAG,
        sink::doris::TAG,
        sink::postgres::TAG,
        sink::sqlite::TAG,
    ];
    tags.sort_unstable();
    assert_eq!(
        tags.to_vec(),
        (1..=19)
            .filter(|tag| ![4, 6].contains(tag))
            .collect::<Vec<_>>()
    );
}
