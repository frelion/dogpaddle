//! Capture into a Source-owned FIFO, then consume its front with downstream state.
use dogpaddle_change::SchemaBoundChangeCodec;
use dogpaddle_operation::{
    OperationDefinition, RuntimeResource,
    operation::{Operation, OperationError, scan::SequenceScanDefinition},
};
use dogpaddle_store::{Cell, StoreSetup};
fn main() -> Result<(), OperationError> {
    let root = tempfile::tempdir()?;
    let mut setup = StoreSetup::new();
    let received = setup.create_data::<Cell<u64>>("received")?;
    let (operation, schema) = OperationDefinition::from(SequenceScanDefinition::new(10))
        .construct(
            &[],
            &mut setup.data_scope().scoped("source"),
            RuntimeResource::none(),
        )?
        .into_parts();
    let codec = SchemaBoundChangeCodec::try_new(schema.ok_or("missing source schema")?)?;
    let (mut writes, reads) = setup.commit(root.path().join("state"), |_| Ok(()))?.split();
    let Operation::Source(mut source) = operation else {
        return Err("expected source".into());
    };
    source.restore(reads.begin().access())?;
    for _ in 0..3 {
        let mut delivery = source.poll()?.ok_or("expected generated delivery")?;
        {
            let txn = writes.begin();
            assert!(source.record(txn.access(), &mut delivery)?);
            txn.commit()?;
        }
        source.ack(delivery)?;
        let change = codec.decode_owned(
            source
                .published(reads.begin().access())?
                .ok_or("missing published Change")?,
        )?;
        let txn = writes.begin();
        source.consume_published(txn.access())?;
        received
            .access(txn.access())?
            .set(&u64::try_from(change.num_rows())?)?;
        txn.commit()?;
        println!("activated captured Change: {:?}", change.records());
    }
    Ok(())
}
