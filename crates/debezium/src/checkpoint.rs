use std::fmt;

use crate::{Error, ErrorKind};

const MAGIC: &[u8; 8] = b"DPDBCP01";
const VERSION: u16 = 1;
const CHECKSUM_BYTES: usize = size_of::<u32>();
pub(crate) const MAX_CHECKPOINT_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_BINDING_BYTES: usize = 1024 * 1024;
const MAX_ENTRY_BYTES: usize = 32 * 1024 * 1024;
const MAX_ENTRIES: u32 = 1_000_000;

/// An opaque, connector-bound image of Kafka Connect's complete offset store.
///
/// The bytes are produced before a delivery is acknowledged and can be stored
/// atomically with that delivery. `DogPaddle` deliberately does not interpret
/// connector-specific positions such as `PostgreSQL` LSNs.
#[derive(Clone, PartialEq, Eq)]
pub struct Checkpoint {
    bytes: Box<[u8]>,
    engine_name: Box<str>,
    connector_class: Box<str>,
}

impl Checkpoint {
    /// Validates checkpoint framing and takes ownership of its bytes.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsupported version, corrupt checksum,
    /// malformed framing, invalid UTF-8 binding, duplicate key, or unsorted
    /// offset entry.
    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> Result<Self, Error> {
        let bytes = bytes.into();
        let (engine_name, connector_class) = validate(&bytes)?;
        Ok(Self {
            bytes: bytes.into_boxed_slice(),
            engine_name: engine_name.into_boxed_str(),
            connector_class: connector_class.into_boxed_str(),
        })
    }

    /// Returns the complete versioned checkpoint representation.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn matches(&self, engine_name: &str, connector_class: &str) -> bool {
        self.engine_name.as_ref() == engine_name && self.connector_class.as_ref() == connector_class
    }
}

impl fmt::Debug for Checkpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Checkpoint")
            .field("bytes", &self.bytes.len())
            .field("engine_name", &self.engine_name)
            .field("connector_class", &self.connector_class)
            .finish()
    }
}

fn validate(bytes: &[u8]) -> Result<(String, String), Error> {
    if bytes.len() > MAX_CHECKPOINT_BYTES {
        return Err(invalid("checkpoint exceeds the 64 MiB protocol limit"));
    }
    if bytes.len() < MAGIC.len() + size_of::<u16>() + CHECKSUM_BYTES {
        return Err(invalid("checkpoint is truncated"));
    }
    let (body, checksum) = bytes.split_at(bytes.len() - CHECKSUM_BYTES);
    let expected = u32::from_be_bytes(
        checksum
            .try_into()
            .map_err(|_| invalid("checkpoint checksum is truncated"))?,
    );
    if crc32fast::hash(body) != expected {
        return Err(invalid("checkpoint checksum does not match"));
    }

    let mut input = Input::new(body);
    if input.take(MAGIC.len())? != MAGIC {
        return Err(invalid("checkpoint magic does not match"));
    }
    if input.u16()? != VERSION {
        return Err(invalid("checkpoint version is not supported"));
    }
    let engine_name = input.utf8_u32("engine name", MAX_BINDING_BYTES)?;
    let connector_class = input.utf8_u32("connector class", MAX_BINDING_BYTES)?;
    if engine_name.trim().is_empty() || connector_class.trim().is_empty() {
        return Err(invalid("checkpoint connector binding must not be blank"));
    }

    let entries = input.u32()?;
    if entries > MAX_ENTRIES {
        return Err(invalid("checkpoint has too many offset entries"));
    }
    let mut previous_key: Option<&[u8]> = None;
    for _ in 0..entries {
        let key = input.bytes_u32(MAX_ENTRY_BYTES, "offset key")?;
        if key.is_empty() {
            return Err(invalid("checkpoint offset key must not be empty"));
        }
        if previous_key.is_some_and(|previous| previous >= key) {
            return Err(invalid(
                "checkpoint offset keys must be unique and strictly sorted",
            ));
        }
        previous_key = Some(key);
        let value_length = input.i32()?;
        if value_length < 0 {
            return Err(invalid("checkpoint offset value has an invalid length"));
        }
        let value_length = usize::try_from(value_length)
            .map_err(|_| invalid("checkpoint value length cannot be represented"))?;
        if value_length > MAX_ENTRY_BYTES {
            return Err(invalid(
                "checkpoint offset value exceeds the protocol limit",
            ));
        }
        input.take(value_length)?;
    }
    if !input.is_empty() {
        return Err(invalid("checkpoint has trailing bytes"));
    }
    Ok((engine_name, connector_class))
}

pub(crate) struct Input<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Input<'a> {
    pub(crate) const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    pub(crate) fn take(&mut self, length: usize) -> Result<&'a [u8], Error> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| invalid("checkpoint length overflows"))?;
        let result = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| invalid("checkpoint is truncated"))?;
        self.position = end;
        Ok(result)
    }

    pub(crate) fn u16(&mut self) -> Result<u16, Error> {
        Ok(u16::from_be_bytes(
            self.take(size_of::<u16>())?
                .try_into()
                .map_err(|_| invalid("checkpoint integer is truncated"))?,
        ))
    }

    pub(crate) fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_be_bytes(
            self.take(size_of::<u32>())?
                .try_into()
                .map_err(|_| invalid("checkpoint integer is truncated"))?,
        ))
    }

    pub(crate) fn i32(&mut self) -> Result<i32, Error> {
        Ok(i32::from_be_bytes(
            self.take(size_of::<i32>())?
                .try_into()
                .map_err(|_| invalid("checkpoint integer is truncated"))?,
        ))
    }

    fn bytes_u32(&mut self, maximum: usize, label: &str) -> Result<&'a [u8], Error> {
        let length = usize::try_from(self.u32()?)
            .map_err(|_| invalid("checkpoint byte length cannot be represented"))?;
        if length > maximum {
            return Err(invalid(format!(
                "checkpoint {label} exceeds the protocol limit"
            )));
        }
        self.take(length)
    }

    fn utf8_u32(&mut self, label: &str, maximum: usize) -> Result<String, Error> {
        let bytes = self.bytes_u32(maximum, label)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| invalid(format!("checkpoint {label} is not valid UTF-8")))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidCheckpoint, message)
}
