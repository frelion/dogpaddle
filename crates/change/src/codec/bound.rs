use std::{
    io::Write,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

use arrow_buffer::Buffer as ArrowBuffer;
use arrow_ipc::writer::{
    DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteContext, IpcWriteOptions,
    write_message,
};
use arrow_schema::{Schema, SchemaRef};

use super::{CodecError, batch, ensure_little_endian_target, size, stream};
use crate::{Change, validate_schema};

const MAGIC: &[u8; 8] = b"DPCHB001";
const FINGERPRINT_BYTES: usize = 32;
const PREFIX_BYTES: usize = MAGIC.len() + FINGERPRINT_BYTES;
const CANONICAL_EOS: &[u8; 8] = &[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0];

/// Encodes persistent Changes whose exact logical Schema is known by the owner.
///
/// A bound entry stores a fixed format marker, a BLAKE3 fingerprint of the
/// canonical physical Arrow Schema, one uncompressed Arrow IPC `RecordBatch`,
/// and the canonical end-of-stream marker. The full Schema is held once by the
/// owning resource instead of being repeated in every entry. The fingerprint
/// prevents an entry from being decoded through a codec bound to another
/// Schema, including Schemas whose columns happen to have the same physical
/// buffer layout.
///
/// Construction validates the complete logical Schema once. Every encode still
/// requires exact Schema equality, including field names, order, nullability,
/// nested fields, and Schema and field metadata.
#[derive(Clone, Debug)]
pub struct SchemaBoundChangeCodec {
    logical_schema: SchemaRef,
    physical_schema: SchemaRef,
    schema_fingerprint: [u8; FINGERPRINT_BYTES],
}

impl SchemaBoundChangeCodec {
    /// Creates a codec bound to one exact logical Schema.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] when the Schema is outside the supported Change
    /// v1 subset, the current target is not little-endian, or Arrow cannot
    /// produce the canonical Schema identity used by the persistent format.
    pub fn try_new(logical_schema: SchemaRef) -> Result<Self, CodecError> {
        ensure_little_endian_target()?;
        validate_schema(logical_schema.as_ref())?;
        let physical_schema = stream::physical_schema(logical_schema.as_ref());
        let schema_fingerprint = schema_fingerprint(physical_schema.as_ref())?;
        Ok(Self {
            logical_schema,
            physical_schema,
            schema_fingerprint,
        })
    }

    /// Returns the exact logical Schema bound to this codec.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.logical_schema)
    }

    /// Encodes one Change without repeating its Arrow Schema.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::SchemaMismatch`] when `change` does not have the
    /// exact bound Schema, or another [`CodecError`] if Arrow cannot encode the
    /// physical batch.
    pub fn encode(&self, change: &Change) -> Result<Vec<u8>, CodecError> {
        self.require_schema(change.records().schema_ref())?;
        let options = stream::write_options()?;
        let encoded = encode_batch(change, Arc::clone(&self.physical_schema), &options)?;
        let mut output = Vec::with_capacity(
            PREFIX_BYTES + encoded.ipc_message.len() + encoded.arrow_data.len() + 16,
        );
        output.extend_from_slice(MAGIC);
        output.extend_from_slice(&self.schema_fingerprint);
        write_message(&mut output, encoded, &options)?;
        output.extend_from_slice(CANONICAL_EOS);
        Ok(output)
    }

    /// Encodes one bound Change while limiting its body and complete entry.
    ///
    /// The logical Arrow slices are measured before the IPC body is built.
    /// The output writer also refuses to grow beyond `max_bytes`.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::EncodedSizeLimitExceeded`] when the uncompressed
    /// body or complete entry exceeds `max_bytes`,
    /// [`CodecError::SchemaMismatch`] for a different logical Schema, or
    /// another [`CodecError`] if Arrow cannot encode the batch.
    pub fn encode_bounded(&self, change: &Change, max_bytes: usize) -> Result<Vec<u8>, CodecError> {
        self.require_schema(change.records().schema_ref())?;
        let body_bytes = size::body_len_bounded(change, max_bytes)?;
        let mut output = stream::BoundedWriter::new(max_bytes);
        if self.write_prefix(&mut output).is_err() {
            return Err(CodecError::size_limit(max_bytes));
        }
        if body_bytes > output.remaining() {
            return Err(CodecError::size_limit(max_bytes));
        }
        let options = stream::write_options()?;
        let encoded = encode_batch(change, Arc::clone(&self.physical_schema), &options)?;
        if let Err(error) = write_message(&mut output, encoded, &options) {
            return if output.limit_hit() {
                Err(CodecError::size_limit(max_bytes))
            } else {
                Err(error.into())
            };
        }
        if output.write_all(CANONICAL_EOS).is_err() {
            return Err(CodecError::size_limit(max_bytes));
        }
        Ok(output.into_inner())
    }

    /// Decodes one borrowed bound entry with complete batch and value checks.
    ///
    /// The returned Change owns its Arrow buffers and does not borrow `encoded`.
    /// Use [`Self::decode_owned`] to let suitably aligned Arrow buffers share
    /// the supplied allocation.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::SchemaMismatch`] when the entry fingerprint does
    /// not match this codec, or another [`CodecError`] for malformed framing,
    /// batch metadata, Arrow values, or differences.
    pub fn decode(&self, encoded: &[u8]) -> Result<Change, CodecError> {
        ensure_little_endian_target()?;
        catch_unwind(AssertUnwindSafe(|| {
            let parsed = self.parse(encoded)?;
            batch::decode(&parsed, None)
        }))
        .map_err(|_| CodecError::invalid("Arrow IPC decoding panicked"))?
    }

    /// Decodes one owned bound entry and shares aligned Arrow body buffers.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::decode`].
    pub fn decode_owned(&self, encoded: Vec<u8>) -> Result<Change, CodecError> {
        ensure_little_endian_target()?;
        catch_unwind(AssertUnwindSafe(|| {
            let encoded = ArrowBuffer::from(encoded);
            let parsed = self.parse(encoded.as_slice())?;
            batch::decode_owned(&encoded, &parsed)
        }))
        .map_err(|_| CodecError::invalid("Arrow IPC decoding panicked"))?
    }

    fn parse<'encoded>(
        &self,
        encoded: &'encoded [u8],
    ) -> Result<stream::ParsedChange<'encoded>, CodecError> {
        let prefix = encoded
            .get(..PREFIX_BYTES)
            .ok_or_else(|| CodecError::invalid("schema-bound Change prefix is truncated"))?;
        if &prefix[..MAGIC.len()] != MAGIC {
            return Err(CodecError::invalid(
                "schema-bound Change format marker is invalid",
            ));
        }
        if prefix[MAGIC.len()..] != self.schema_fingerprint {
            return Err(CodecError::SchemaMismatch);
        }
        stream::parse_record_batch(
            encoded,
            PREFIX_BYTES,
            Arc::clone(&self.physical_schema),
            Arc::clone(&self.logical_schema),
        )
    }

    fn require_schema(&self, actual: &SchemaRef) -> Result<(), CodecError> {
        if Arc::ptr_eq(&self.logical_schema, actual)
            || self.logical_schema.as_ref() == actual.as_ref()
        {
            Ok(())
        } else {
            Err(CodecError::SchemaMismatch)
        }
    }

    fn write_prefix(&self, output: &mut impl Write) -> std::io::Result<()> {
        output.write_all(MAGIC)?;
        output.write_all(&self.schema_fingerprint)
    }
}

fn schema_fingerprint(schema: &Schema) -> Result<[u8; FINGERPRINT_BYTES], CodecError> {
    let options = stream::write_options()?;
    let mut dictionaries = DictionaryTracker::new(true);
    let encoded = IpcDataGenerator {}.schema_to_bytes_with_dictionary_tracker(
        schema,
        &mut dictionaries,
        &options,
    );
    let mut canonical = Vec::new();
    write_message(&mut canonical, encoded, &options)?;
    Ok(*blake3::hash(&canonical).as_bytes())
}

fn encode_batch(
    change: &Change,
    physical_schema: SchemaRef,
    options: &IpcWriteOptions,
) -> Result<EncodedData, CodecError> {
    let physical = stream::physical_batch(change, physical_schema)?;
    let mut dictionaries = DictionaryTracker::new(true);
    let (encoded_dictionaries, encoded) = IpcDataGenerator {}.encode(
        &physical,
        &mut dictionaries,
        options,
        &mut IpcWriteContext::default(),
    )?;
    if !encoded_dictionaries.is_empty() {
        return Err(CodecError::invalid(
            "dictionary batches are not supported in schema-bound Changes",
        ));
    }
    Ok(encoded)
}
