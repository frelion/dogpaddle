use dogpaddle_operation::{
    OperationDefinition, OperationKind, OperationSetupError, RuntimeResource, decode_definition,
    encode_definition,
    operation::{
        Operation,
        scan::{
            PostgresCdcScanConfig, PostgresCdcScanDefinition, PostgresCdcScanOptions,
            PostgresCdcScanSpec, PostgresColumn, PostgresType,
        },
    },
};
use dogpaddle_store::{Cell, Queue, Store, StoreSetup};
use std::{
    num::{NonZeroU32, NonZeroU64},
    time::Duration,
};

use super::support::construct_checked_with_resource;

fn construct_checked(
    definition: &(impl Clone + Into<OperationDefinition>),
    inputs: &[arrow_schema::SchemaRef],
) -> Result<Option<arrow_schema::SchemaRef>, dogpaddle_operation::OperationBindError> {
    construct_checked_with_resource(definition, inputs, &RuntimeResource::new(config()))
}

fn definition() -> PostgresCdcScanDefinition {
    PostgresCdcScanDefinition::try_new(
        PostgresCdcScanSpec {
            engine_name: "orders".into(),
            database: "shop".into(),
            schema: "public".into(),
            table: "orders".into(),
            slot: "orders_slot".into(),
            publication: "orders_pub".into(),
            system_identifier: "123".into(),
            database_oid: 42,
            table_oid: 43,
            columns: vec![PostgresColumn::new("id", PostgresType::Int64, false)],
        },
        NonZeroU64::new(1_048_576).unwrap(),
    )
    .unwrap()
}

fn config() -> PostgresCdcScanConfig {
    PostgresCdcScanConfig::new_unencrypted(
        "/nonexistent/dogpaddle-runtime",
        "127.0.0.1",
        1,
        "shop",
        "cdc",
        "do-not-persist-this-password",
    )
    .unwrap()
}

fn literal_definition_bytes() -> Vec<u8> {
    let mut expected = b"dogpaddle.operation\0\0\x01".to_vec();
    expected.extend_from_slice(br#"{"postgres_cdc_scan":{"spec":{"engine_name":"orders","database":"shop","schema":"public","table":"orders","slot":"orders_slot","publication":"orders_pub","system_identifier":"123","database_oid":42,"table_oid":43,"columns":[{"name":"id","data_type":"int64","nullable":false}]},"output_projection":[0],"bootstrap_spool_bytes":1048576}}"#);
    expected
}

#[test]
fn postgres_cdc_definition_has_a_canonical_non_secret_variant_and_exact_schema() {
    let definition = definition();
    assert_eq!(
        OperationDefinition::from(definition.clone()).kind(),
        OperationKind::Scan
    );
    let bytes = encode_definition(&definition.clone().into());
    let expected = literal_definition_bytes();
    assert_eq!(bytes, expected);
    let decoded = decode_definition(&bytes).unwrap();
    assert_eq!(decoded.kind(), OperationKind::Scan);
    assert_eq!(encode_definition(&decoded), bytes);
    let binding = construct_checked(&decoded, &[]).unwrap();
    let output = binding.as_ref().unwrap();
    assert_eq!(output.fields().len(), 1);
    assert_eq!(output.field(0).name(), "id");
    assert_eq!(output.field(0).data_type(), &arrow_schema::DataType::Int64);
    assert!(!output.field(0).is_nullable());
    assert!(!String::from_utf8(bytes).unwrap().contains("password"));
    let mut trailing = expected.clone();
    trailing.push(b' ');
    assert!(decode_definition(&trailing).is_err());
    for length in 0..expected.len() {
        assert!(decode_definition(&expected[..length]).is_err());
    }
}

#[test]
fn postgres_cdc_uses_one_input_queue() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state");
    let definition = definition();
    let mut setup = StoreSetup::new();
    let (operation, _) = OperationDefinition::from(definition.clone())
        .construct(
            &[],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::new(config()),
        )
        .unwrap()
        .into_parts();
    let transactions = setup.commit(&path, |_| Ok(())).unwrap();
    drop((operation, transactions));
    let store = Store::open(path).unwrap();
    store
        .open_data::<Queue<Vec<u8>>>("operation/postgres_cdc_scan.input")
        .unwrap();
    for suffix in ["bootstrap_spool", "published"] {
        let name = format!("operation/postgres_cdc_scan.{suffix}");
        assert!(matches!(
            store.open_data::<Queue<Vec<u8>>>(&name),
            Err(dogpaddle_store::StoreError::DataNotFound(missing)) if missing == name
        ));
    }
}

#[test]
fn postgres_cdc_materialization_requires_one_exact_runtime_resource() {
    let definition = definition();
    assert!(matches!(
        OperationDefinition::from(definition.clone()).validate_resource(&RuntimeResource::none()),
        Err(OperationSetupError::MissingRuntimeResource)
    ));
    assert!(matches!(
        OperationDefinition::from(definition.clone())
            .validate_resource(&RuntimeResource::new(42_u64)),
        Err(OperationSetupError::WrongRuntimeResource)
    ));
    assert!(
        OperationDefinition::from(definition.clone())
            .validate_resource(&RuntimeResource::new(config()))
            .is_ok()
    );
}

#[test]
fn postgres_cdc_restore_is_read_only_and_does_not_start_external_resources() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state");
    let mut setup = StoreSetup::new();
    let definition: OperationDefinition = definition().into();
    let (operation, _) = definition
        .construct(
            &[],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::new(config()),
        )
        .unwrap()
        .into_parts();
    let transactions = setup.commit(&path, |_| Ok(())).unwrap();
    drop((operation, transactions));
    for _ in 0..2 {
        let store = Store::open(&path).unwrap();
        let phase = store
            .open_data::<Cell<u32>>("operation/postgres_cdc_scan.phase")
            .unwrap();
        let (operation, _) = definition
            .construct(
                &[],
                &mut store.data_scope().scoped("operation"),
                RuntimeResource::new(config()),
            )
            .unwrap()
            .into_parts();
        let Operation::Source(mut source) = operation else {
            panic!("expected source");
        };
        let snapshot = store.read_transaction();
        source.restore(snapshot.access()).unwrap();
        assert_eq!(phase.read(snapshot.access()).unwrap().get().unwrap(), None);
        drop(source.poll().unwrap()); // Concrete BeginCapture performs no I/O or writes.
    }
}
#[test]
fn postgres_cdc_corrupt_sealed_checkpoint_is_rejected_without_rewriting_state() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state");
    let mut setup = StoreSetup::new();
    let definition: OperationDefinition = definition().into();
    let (operation, _) = definition
        .construct(
            &[],
            &mut setup.data_scope().scoped("operation"),
            RuntimeResource::new(config()),
        )
        .unwrap()
        .into_parts();
    let transactions = setup.commit(&path, |_| Ok(())).unwrap();
    drop((operation, transactions));
    let store = Store::open(&path).unwrap();
    let phase = store
        .open_data::<Cell<u32>>("operation/postgres_cdc_scan.phase")
        .unwrap();
    let checkpoint_handle = store
        .open_data::<Cell<Vec<u8>>>("operation/postgres_cdc_scan.checkpoint")
        .unwrap();
    let (mut writes, reads) = store.into_transactions().split();
    let txn = writes.begin();
    phase.access(txn.access()).unwrap().set(&2).unwrap();
    checkpoint_handle
        .access(txn.access())
        .unwrap()
        .set(&vec![0])
        .unwrap();
    txn.commit().unwrap();
    drop((writes, reads));
    let store = Store::open(&path).unwrap();
    let (operation, _) = definition
        .construct(
            &[],
            &mut store.data_scope().scoped("operation"),
            RuntimeResource::new(config()),
        )
        .unwrap()
        .into_parts();
    let Operation::Source(mut source) = operation else {
        panic!("expected source");
    };
    let snapshot = store.read_transaction();
    assert!(source.restore(snapshot.access()).is_err());
    let checkpoint_handle = store
        .open_data::<Cell<Vec<u8>>>("operation/postgres_cdc_scan.checkpoint")
        .unwrap();
    assert_eq!(
        checkpoint_handle
            .read(snapshot.access())
            .unwrap()
            .get()
            .unwrap(),
        Some(vec![0])
    );
}
#[test]
fn postgres_cdc_schema_rejects_unsupported_precision_and_invalid_columns() {
    let column = |data_type| PostgresColumn::new("id", data_type, false);
    let mut no_columns = definition().spec().clone();
    no_columns.columns.clear();
    assert!(
        PostgresCdcScanDefinition::try_new(no_columns, NonZeroU64::new(1_048_576).unwrap())
            .is_err()
    );
    for columns in [
        vec![PostgresColumn::new("", PostgresType::Int64, false)],
        vec![PostgresColumn::new(
            "$dogpaddle.value",
            PostgresType::Int64,
            false,
        )],
        vec![column(PostgresType::Int64), column(PostgresType::Text)],
        vec![column(PostgresType::Numeric {
            precision: 0,
            scale: 0,
        })],
        vec![column(PostgresType::Numeric {
            precision: 39,
            scale: 0,
        })],
        vec![column(PostgresType::Numeric {
            precision: 2,
            scale: 3,
        })],
        vec![column(PostgresType::Numeric {
            precision: 2,
            scale: -1,
        })],
    ] {
        let mut spec = definition().spec().clone();
        spec.columns = columns;
        let definition =
            PostgresCdcScanDefinition::try_new(spec, NonZeroU64::new(1_048_576).unwrap()).unwrap();
        assert!(
            OperationDefinition::from(definition.clone())
                .output_schema(&[])
                .is_err()
        );
    }
}

#[test]
fn postgres_cdc_projection_is_ordered_and_can_preserve_rows_without_columns() {
    let mut spec = definition().spec().clone();
    spec.columns
        .push(PostgresColumn::new("payload", PostgresType::Text, true));
    let capacity = NonZeroU64::new(1_048_576).unwrap();

    let projected =
        PostgresCdcScanDefinition::try_new_projected(spec.clone(), vec![1], capacity).unwrap();
    assert_eq!(projected.output_projection(), &[1]);
    let output = OperationDefinition::from(projected.clone())
        .output_schema(&[])
        .unwrap()
        .unwrap();
    assert_eq!(output.fields().len(), 1);
    assert_eq!(output.field(0).name(), "payload");
    assert!(output.field(0).is_nullable());

    let empty =
        PostgresCdcScanDefinition::try_new_projected(spec.clone(), vec![], capacity).unwrap();
    assert!(
        OperationDefinition::from(empty.clone())
            .output_schema(&[])
            .unwrap()
            .unwrap()
            .fields()
            .is_empty()
    );

    for invalid in [vec![0, 0], vec![1, 0], vec![2]] {
        assert!(
            PostgresCdcScanDefinition::try_new_projected(spec.clone(), invalid, capacity).is_err()
        );
    }
}

#[test]
fn postgres_cdc_runtime_config_is_secret_safe_and_requires_explicit_unencrypted_setup() {
    let options = PostgresCdcScanOptions::new()
        .retry_limit(3)
        .unwrap()
        .heartbeat_interval(Duration::from_secs(2))
        .unwrap();
    let debug = format!("{:?}", config().options(options));
    assert!(debug.contains("[redacted]"));
    assert!(debug.contains("PostgresCdcScanOptions"));
    assert!(debug.contains("retry_limit: 3"));
    assert!(!debug.contains("do-not-persist-this-password"));
    assert!(
        PostgresCdcScanConfig::new_unencrypted("relative", "host", 5432, "db", "user", "password")
            .is_err()
    );
    assert!(
        PostgresCdcScanConfig::new_unencrypted("/bundle", "host", 0, "db", "user", "password")
            .is_err()
    );
}

#[test]
fn postgres_cdc_runtime_options_validate_java_bounds_before_external_io() {
    let defaults = PostgresCdcScanOptions::new();
    assert_eq!(defaults, PostgresCdcScanOptions::default());

    let too_large = Duration::from_millis(u64::try_from(i32::MAX).unwrap() + 1);
    for invalid in [Duration::ZERO, Duration::from_nanos(1), too_large] {
        assert!(defaults.connect_timeout(invalid).is_err());
        assert!(defaults.query_timeout(invalid).is_err());
        assert!(defaults.heartbeat_interval(invalid).is_err());
        assert!(defaults.retry_max_delay(invalid).is_err());
    }

    assert!(
        defaults
            .retry_max_delay(Duration::from_millis(300))
            .is_err()
    );
    assert!(defaults.retry_max_delay(Duration::from_millis(301)).is_ok());
    assert!(
        defaults
            .query_timeout(Duration::from_millis(2_147_483_001))
            .is_err()
    );

    let maximum = u32::try_from(i32::MAX).unwrap();
    assert!(defaults.retry_limit(maximum).is_ok());
    assert!(defaults.retry_limit(maximum + 1).is_err());
    assert!(
        defaults
            .snapshot_fetch_size(NonZeroU32::new(maximum).unwrap())
            .is_ok()
    );
    assert!(
        defaults
            .snapshot_fetch_size(NonZeroU32::new(maximum + 1).unwrap())
            .is_err()
    );
}

#[test]
fn raw_plan_business_validation_precedes_store_handle_access() {
    let mut payload = serde_json::to_value(definition()).unwrap();
    payload["output_projection"] = serde_json::json!([1, 0]);
    let plan: OperationDefinition =
        serde_json::from_value(serde_json::json!({"postgres_cdc_scan": payload})).unwrap();
    crate::support::assert_rejected_plan_before_data(&plan, &[], RuntimeResource::new(config()));
}
