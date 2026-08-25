use std::collections::HashSet;

use crate::{
    OperationDefinition,
    codec::DECODERS,
    operation::{source::SequenceSourceDefinition, transform::CountDefinition},
};

#[test]
fn decoder_tags_and_data_names_are_unique() {
    let mut tags = HashSet::new();
    for (tag, _) in DECODERS {
        assert!(tags.insert(*tag), "duplicate operation decoder tag {tag}");
    }

    for definition in [
        &SequenceSourceDefinition::new(0) as &dyn OperationDefinition,
        &CountDefinition::new(),
    ] {
        assert!(tags.contains(&definition.persistence_tag()));
        let mut names = HashSet::new();
        for name in definition.data_names() {
            assert!(!name.is_empty());
            assert!(!name.as_bytes().contains(&0));
            assert!(
                names.insert(*name),
                "duplicate operation data name {name:?}"
            );
        }
    }
}
