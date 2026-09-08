use super::row_hash;

#[test]
fn relation_row_hash_has_stable_v1_literal() {
    assert_eq!(
        row_hash(b"abc"),
        [
            4, 218, 114, 182, 185, 175, 202, 56, 247, 249, 33, 13, 62, 154, 119, 167,
        ]
    );
}
