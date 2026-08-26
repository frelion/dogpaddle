use std::{collections::HashMap, sync::Arc};

use arrow_array::{Array, StringArray};
use arrow_schema::{DataType, Schema};
use dogpaddle_change::{
    ChangeProjection, CodecError, ProjectionError, decode_change_projected, encode_change,
};

use super::support::{assert_change_eq, representative_change};

#[test]
fn projection_only_deletes_top_level_fields_and_shares_arrow_buffers() {
    let change = representative_change();
    let projection = ChangeProjection::try_new(change.schema(), [0, 2, 4, 5, 6]).unwrap();
    let projected = change.try_project(&projection).unwrap();

    assert_eq!(
        projected
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["id", "label", "items", "object", "nothing"]
    );
    assert_eq!(projected.schema(), projection.output_schema());
    assert_eq!(projected.schema().metadata(), change.schema().metadata());
    let projected_schema = projected.schema();
    assert_eq!(
        projected_schema.field(1).metadata().get("semantic"),
        Some(&"label".to_owned())
    );
    let object = projected_schema.field(3);
    let arrow_schema::DataType::Struct(children) = object.data_type() else {
        panic!("object projection did not retain its complete Struct subtree");
    };
    assert_eq!(children.len(), 2);

    for (source, target) in [(0, 0), (2, 1), (4, 2), (5, 3), (6, 4)] {
        assert!(Arc::ptr_eq(
            change.records().column(source),
            projected.records().column(target)
        ));
    }
    assert_eq!(
        change.diffs().values().as_ptr(),
        projected.diffs().values().as_ptr()
    );
}

#[test]
fn empty_and_identity_projections_preserve_rows_diffs_and_schema_identity() {
    let change = representative_change();
    let empty = ChangeProjection::try_new(change.schema(), []).unwrap();
    let projected = change.try_project(&empty).unwrap();
    assert_eq!(projected.records().num_columns(), 0);
    assert_eq!(projected.num_rows(), change.num_rows());
    assert_eq!(projected.schema().metadata(), change.schema().metadata());
    assert_eq!(projected.diffs(), change.diffs());
    assert_eq!(
        projected.diffs().values().as_ptr(),
        change.diffs().values().as_ptr()
    );

    let field_count = change.schema().fields().len();
    let identity = ChangeProjection::try_new(change.schema(), 0..field_count).unwrap();
    assert_eq!(identity.output_schema(), change.schema());
    let projected = change.try_project(&identity).unwrap();
    assert_change_eq(&projected, &change);
    for index in 0..field_count {
        assert!(Arc::ptr_eq(
            change.records().column(index),
            projected.records().column(index)
        ));
    }
}

#[test]
fn projected_change_is_owned_and_can_outlive_its_source() {
    let projected = {
        let change = representative_change();
        let projection = ChangeProjection::try_new(change.schema(), [2]).unwrap();
        change.try_project(&projection).unwrap()
    };

    let labels = projected
        .records()
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(labels.value(0), "add");
    assert!(labels.is_null(1));
    assert_eq!(labels.value(2), "next");
    assert_eq!(projected.diffs().values(), &[1, -1, 2]);
}

#[test]
fn projection_rejects_reordering_duplicates_bounds_and_exact_schema_drift() {
    let change = representative_change();
    let schema = change.schema();
    let encoded = encode_change(&change).unwrap();
    assert!(matches!(
        ChangeProjection::try_new(Arc::clone(&schema), [2, 0]),
        Err(ProjectionError::FieldsNotStrictlyIncreasing {
            previous: 2,
            current: 0
        })
    ));
    assert!(matches!(
        ChangeProjection::try_new(Arc::clone(&schema), [1, 1]),
        Err(ProjectionError::FieldsNotStrictlyIncreasing {
            previous: 1,
            current: 1
        })
    ));
    assert!(matches!(
        ChangeProjection::try_new(Arc::clone(&schema), [schema.fields().len()]),
        Err(ProjectionError::FieldOutOfBounds { .. })
    ));

    let mut metadata = schema.metadata().clone();
    metadata.insert("drift".to_owned(), "true".to_owned());
    let drifted = Arc::new(Schema::new_with_metadata(schema.fields().clone(), metadata));
    let drifted_projection = ChangeProjection::try_new(drifted, [0]).unwrap();
    assert!(matches!(
        change.try_project(&drifted_projection),
        Err(ProjectionError::SchemaMismatch)
    ));
    assert!(matches!(
        decode_change_projected(&encoded, &drifted_projection),
        Err(CodecError::Projection(ProjectionError::SchemaMismatch))
    ));

    for container in ["items", "object"] {
        let fields = schema
            .fields()
            .iter()
            .map(|field| {
                if field.name() != container {
                    return Arc::clone(field);
                }
                let data_type = match field.data_type() {
                    DataType::List(child) => {
                        DataType::List(Arc::new(child.as_ref().clone().with_metadata(
                            HashMap::from([("drift".to_owned(), "list-child".to_owned())]),
                        )))
                    }
                    DataType::Struct(children) => {
                        let mut children = children.iter().cloned().collect::<Vec<_>>();
                        children[0] =
                            Arc::new(children[0].as_ref().clone().with_metadata(HashMap::from([
                                ("drift".to_owned(), "struct-child".to_owned()),
                            ])));
                        DataType::Struct(children.into())
                    }
                    data_type => panic!("{container} unexpectedly has type {data_type}"),
                };
                Arc::new(field.as_ref().clone().with_data_type(data_type))
            })
            .collect::<Vec<_>>();
        let drifted = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        let drifted_projection = ChangeProjection::try_new(drifted, [0]).unwrap();
        assert!(matches!(
            change.try_project(&drifted_projection),
            Err(ProjectionError::SchemaMismatch)
        ));
        assert!(matches!(
            decode_change_projected(&encoded, &drifted_projection),
            Err(CodecError::Projection(ProjectionError::SchemaMismatch))
        ));
    }
}
