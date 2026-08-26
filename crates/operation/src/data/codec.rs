use std::{borrow::Cow, num::NonZeroI64, str};

use dogpaddle_store::{CodecError, StoreValue};

use super::{CanonicalF64, Change, Record, Value, record::MAX_NESTING_DEPTH};

const FORMAT_VERSION: u16 = 1;
const VERSION_BYTES: usize = size_of::<u16>();
const DIFF_BYTES: usize = size_of::<i64>();
const COUNT_BYTES: usize = size_of::<u32>();
const CHANGE_HEADER_BYTES: usize = VERSION_BYTES + DIFF_BYTES;
const MIN_FIELD_BYTES: usize = COUNT_BYTES + COUNT_BYTES + 1;
const MIN_ARRAY_ELEMENT_BYTES: usize = COUNT_BYTES + 1;

const NULL_TAG: u8 = 0x00;
const FALSE_TAG: u8 = 0x01;
const TRUE_TAG: u8 = 0x02;
const I64_TAG: u8 = 0x10;
const U64_TAG: u8 = 0x11;
const F64_TAG: u8 = 0x12;
const STRING_TAG: u8 = 0x20;
const BYTES_TAG: u8 = 0x21;
const ARRAY_TAG: u8 = 0x30;
const OBJECT_TAG: u8 = 0x31;

impl Change {
    /// Decodes only the stable envelope and returns this change's non-zero diff.
    ///
    /// This projection deliberately does not validate the complete record body.
    /// Use the [`StoreValue`] decoder when the operation interprets the record.
    ///
    /// # Errors
    ///
    /// Returns a codec error when the encoding is truncated, uses an unsupported
    /// format version, exceeds the v1 size limit, or contains a zero diff.
    pub fn project_diff(encoded: &[u8]) -> Result<NonZeroI64, CodecError> {
        decode_header(encoded).map(|(diff, _)| diff)
    }
}

impl StoreValue for Change {
    fn encode_value(&self) -> Result<impl AsRef<[u8]>, CodecError> {
        encode_change(self)
    }

    fn decode_value(bytes: Cow<'_, [u8]>) -> Result<Self, CodecError> {
        decode_change(bytes.as_ref())
    }
}

fn encode_change(change: &Change) -> Result<Vec<u8>, CodecError> {
    let record_len = encoded_record_len(change.record(), 0)?;
    let encoded_len = CHANGE_HEADER_BYTES
        .checked_add(record_len)
        .ok_or_else(|| length_overflow("change"))?;
    ensure_u32_len(encoded_len, "change")?;

    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(encoded_len)
        .map_err(|_| CodecError::new("cannot allocate encoded change"))?;
    encoded.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    encoded.extend_from_slice(&change.diff().get().to_be_bytes());
    encode_record(change.record(), 0, &mut encoded)?;
    Ok(encoded)
}

fn decode_change(encoded: &[u8]) -> Result<Change, CodecError> {
    let (diff, record) = decode_header(encoded)?;
    let record = decode_record(record, 0)?;
    Ok(Change::new(diff, record))
}

fn decode_header(encoded: &[u8]) -> Result<(NonZeroI64, &[u8]), CodecError> {
    ensure_u32_len(encoded.len(), "change")?;
    let mut cursor = Cursor::new(encoded);
    let version = cursor.read_u16()?;
    if version != FORMAT_VERSION {
        return Err(CodecError::new(format!(
            "unsupported change format version {version}"
        )));
    }
    let diff = NonZeroI64::new(cursor.read_i64()?)
        .ok_or_else(|| CodecError::new("change diff is zero"))?;
    if cursor.remaining().len() < COUNT_BYTES {
        return Err(truncated());
    }
    Ok((diff, cursor.remaining()))
}

fn encoded_record_len(record: &Record, depth: usize) -> Result<usize, CodecError> {
    ensure_u32_len(record.as_fields().len(), "record field count")?;
    let mut encoded_len = COUNT_BYTES;
    let mut previous_name: Option<&[u8]> = None;

    for (name, value) in record.as_fields() {
        let name = name.as_bytes();
        if previous_name.is_some_and(|previous| previous >= name) {
            return Err(CodecError::new(
                "record fields are not in strictly increasing name order",
            ));
        }
        previous_name = Some(name);
        ensure_u32_len(name.len(), "record field name")?;

        let value_len = encoded_value_len(value, depth)?;
        ensure_u32_len(value_len, "record field value")?;
        encoded_len = checked_sum(
            encoded_len,
            &[COUNT_BYTES, name.len(), COUNT_BYTES, value_len],
            "record",
        )?;
    }
    Ok(encoded_len)
}

fn encoded_value_len(value: &Value, depth: usize) -> Result<usize, CodecError> {
    match value {
        Value::Null | Value::Bool(_) => Ok(1),
        Value::I64(_) | Value::U64(_) | Value::F64(_) => Ok(1 + size_of::<u64>()),
        Value::String(value) => checked_add(1, value.len(), "string value"),
        Value::Bytes(value) => checked_add(1, value.len(), "bytes value"),
        Value::Array(values) => {
            let nested_depth = enter_container(depth)?;
            ensure_u32_len(values.len(), "array element count")?;
            let mut encoded_len = 1 + COUNT_BYTES;
            for value in values {
                let value_len = encoded_value_len(value, nested_depth)?;
                ensure_u32_len(value_len, "array element")?;
                encoded_len = checked_sum(encoded_len, &[COUNT_BYTES, value_len], "array value")?;
            }
            Ok(encoded_len)
        }
        Value::Object(record) => {
            let nested_depth = enter_container(depth)?;
            checked_add(1, encoded_record_len(record, nested_depth)?, "object value")
        }
    }
}

fn encode_record(record: &Record, depth: usize, encoded: &mut Vec<u8>) -> Result<(), CodecError> {
    encode_u32(record.as_fields().len(), "record field count", encoded)?;
    let mut previous_name: Option<&[u8]> = None;
    for (name, value) in record.as_fields() {
        let name = name.as_bytes();
        if previous_name.is_some_and(|previous| previous >= name) {
            return Err(CodecError::new(
                "record fields are not in strictly increasing name order",
            ));
        }
        previous_name = Some(name);

        encode_u32(name.len(), "record field name", encoded)?;
        encoded.extend_from_slice(name);
        let value_len = encoded_value_len(value, depth)?;
        encode_u32(value_len, "record field value", encoded)?;
        encode_value(value, depth, encoded)?;
    }
    Ok(())
}

fn encode_value(value: &Value, depth: usize, encoded: &mut Vec<u8>) -> Result<(), CodecError> {
    match value {
        Value::Null => encoded.push(NULL_TAG),
        Value::Bool(false) => encoded.push(FALSE_TAG),
        Value::Bool(true) => encoded.push(TRUE_TAG),
        Value::I64(value) => {
            encoded.push(I64_TAG);
            encoded.extend_from_slice(&value.to_be_bytes());
        }
        Value::U64(value) => {
            encoded.push(U64_TAG);
            encoded.extend_from_slice(&value.to_be_bytes());
        }
        Value::F64(value) => {
            encoded.push(F64_TAG);
            encoded.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        Value::String(value) => {
            encoded.push(STRING_TAG);
            encoded.extend_from_slice(value.as_bytes());
        }
        Value::Bytes(value) => {
            encoded.push(BYTES_TAG);
            encoded.extend_from_slice(value);
        }
        Value::Array(values) => {
            let nested_depth = enter_container(depth)?;
            encoded.push(ARRAY_TAG);
            encode_u32(values.len(), "array element count", encoded)?;
            for value in values {
                let value_len = encoded_value_len(value, nested_depth)?;
                encode_u32(value_len, "array element", encoded)?;
                encode_value(value, nested_depth, encoded)?;
            }
        }
        Value::Object(record) => {
            let nested_depth = enter_container(depth)?;
            encoded.push(OBJECT_TAG);
            encode_record(record, nested_depth, encoded)?;
        }
    }
    Ok(())
}

fn decode_record(encoded: &[u8], depth: usize) -> Result<Record, CodecError> {
    let mut cursor = Cursor::new(encoded);
    let field_count = cursor.read_len()?;
    if field_count > cursor.remaining().len() / MIN_FIELD_BYTES {
        return Err(CodecError::new(
            "record field count exceeds the remaining encoding",
        ));
    }

    let mut fields = Vec::new();
    let mut previous_name: Option<&[u8]> = None;
    for _ in 0..field_count {
        fields
            .try_reserve(1)
            .map_err(|_| CodecError::new("cannot allocate decoded record fields"))?;
        let name_len = cursor.read_len()?;
        let name_bytes = cursor.take(name_len)?;
        let name = str::from_utf8(name_bytes)
            .map_err(|error| CodecError::new(format!("invalid UTF-8 field name: {error}")))?;
        if previous_name.is_some_and(|previous| previous >= name_bytes) {
            return Err(CodecError::new(
                "record fields are not in strictly increasing name order",
            ));
        }
        previous_name = Some(name_bytes);

        let value_len = cursor.read_len()?;
        if value_len == 0 {
            return Err(CodecError::new("record field value has zero length"));
        }
        let value = decode_value(cursor.take(value_len)?, depth)?;
        fields.push((copy_string(name)?, value));
    }
    cursor.finish("record")?;
    Ok(Record::from_canonical_fields(fields))
}

fn decode_value(encoded: &[u8], depth: usize) -> Result<Value, CodecError> {
    let Some((&tag, payload)) = encoded.split_first() else {
        return Err(CodecError::new("encoded value is empty"));
    };
    match tag {
        NULL_TAG => {
            require_empty(payload, "null")?;
            Ok(Value::Null)
        }
        FALSE_TAG => {
            require_empty(payload, "false")?;
            Ok(Value::Bool(false))
        }
        TRUE_TAG => {
            require_empty(payload, "true")?;
            Ok(Value::Bool(true))
        }
        I64_TAG => Ok(Value::I64(i64::from_be_bytes(fixed(payload, "i64")?))),
        U64_TAG => Ok(Value::U64(u64::from_be_bytes(fixed(payload, "u64")?))),
        F64_TAG => {
            let bits = u64::from_be_bytes(fixed(payload, "f64")?);
            let value = CanonicalF64::try_from_canonical_bits(bits)
                .ok_or_else(|| CodecError::new("f64 encoding is not canonical"))?;
            Ok(Value::F64(value))
        }
        STRING_TAG => {
            let value = str::from_utf8(payload)
                .map_err(|error| CodecError::new(format!("invalid UTF-8 string: {error}")))?;
            Ok(Value::String(copy_string(value)?))
        }
        BYTES_TAG => Ok(Value::Bytes(copy_bytes(payload)?)),
        ARRAY_TAG => decode_array(payload, enter_container(depth)?),
        OBJECT_TAG => decode_record(payload, enter_container(depth)?).map(Value::Object),
        _ => Err(CodecError::new(format!("unknown value tag {tag:#04x}"))),
    }
}

fn decode_array(encoded: &[u8], depth: usize) -> Result<Value, CodecError> {
    let mut cursor = Cursor::new(encoded);
    let element_count = cursor.read_len()?;
    if element_count > cursor.remaining().len() / MIN_ARRAY_ELEMENT_BYTES {
        return Err(CodecError::new(
            "array element count exceeds the remaining encoding",
        ));
    }

    let mut values = Vec::new();
    for _ in 0..element_count {
        values
            .try_reserve(1)
            .map_err(|_| CodecError::new("cannot allocate decoded array elements"))?;
        let value_len = cursor.read_len()?;
        if value_len == 0 {
            return Err(CodecError::new("array element has zero length"));
        }
        values.push(decode_value(cursor.take(value_len)?, depth)?);
    }
    cursor.finish("array")?;
    Ok(Value::Array(values))
}

fn enter_container(depth: usize) -> Result<usize, CodecError> {
    let nested = depth
        .checked_add(1)
        .ok_or_else(|| CodecError::new("value nesting depth overflow"))?;
    if nested > MAX_NESTING_DEPTH {
        Err(CodecError::new(format!(
            "value nesting depth exceeds {MAX_NESTING_DEPTH}"
        )))
    } else {
        Ok(nested)
    }
}

fn checked_add(left: usize, right: usize, field: &'static str) -> Result<usize, CodecError> {
    left.checked_add(right)
        .ok_or_else(|| length_overflow(field))
}

fn checked_sum(
    mut total: usize,
    values: &[usize],
    field: &'static str,
) -> Result<usize, CodecError> {
    for value in values {
        total = checked_add(total, *value, field)?;
    }
    Ok(total)
}

fn ensure_u32_len(length: usize, field: &'static str) -> Result<u32, CodecError> {
    u32::try_from(length).map_err(|_| length_overflow(field))
}

fn encode_u32(value: usize, field: &'static str, encoded: &mut Vec<u8>) -> Result<(), CodecError> {
    encoded.extend_from_slice(&ensure_u32_len(value, field)?.to_be_bytes());
    Ok(())
}

fn length_overflow(field: &'static str) -> CodecError {
    CodecError::new(format!("{field} is too large for the change format"))
}

fn require_empty(encoded: &[u8], name: &'static str) -> Result<(), CodecError> {
    if encoded.is_empty() {
        Ok(())
    } else {
        Err(CodecError::new(format!(
            "{name} value contains trailing bytes"
        )))
    }
}

fn fixed<const N: usize>(encoded: &[u8], name: &'static str) -> Result<[u8; N], CodecError> {
    encoded
        .try_into()
        .map_err(|_| CodecError::new(format!("invalid {name} value length")))
}

fn copy_string(value: &str) -> Result<String, CodecError> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(value.len())
        .map_err(|_| CodecError::new("cannot allocate decoded string"))?;
    owned.push_str(value);
    Ok(owned)
}

fn copy_bytes(value: &[u8]) -> Result<Vec<u8>, CodecError> {
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(value.len())
        .map_err(|_| CodecError::new("cannot allocate decoded bytes"))?;
    owned.extend_from_slice(value);
    Ok(owned)
}

fn truncated() -> CodecError {
    CodecError::new("change encoding is truncated")
}

struct Cursor<'a> {
    remaining: &'a [u8],
}

impl<'a> Cursor<'a> {
    const fn new(remaining: &'a [u8]) -> Self {
        Self { remaining }
    }

    const fn remaining(&self) -> &'a [u8] {
        self.remaining
    }

    fn read_u16(&mut self) -> Result<u16, CodecError> {
        Ok(u16::from_be_bytes(
            self.take(VERSION_BYTES)?
                .try_into()
                .map_err(|_| CodecError::new("invalid change version length"))?,
        ))
    }

    fn read_i64(&mut self) -> Result<i64, CodecError> {
        Ok(i64::from_be_bytes(
            self.take(DIFF_BYTES)?
                .try_into()
                .map_err(|_| CodecError::new("invalid change diff length"))?,
        ))
    }

    fn read_len(&mut self) -> Result<usize, CodecError> {
        let encoded: [u8; COUNT_BYTES] = self
            .take(COUNT_BYTES)?
            .try_into()
            .map_err(|_| CodecError::new("invalid encoded length"))?;
        usize::try_from(u32::from_be_bytes(encoded))
            .map_err(|_| CodecError::new("encoded length does not fit this platform"))
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], CodecError> {
        let (value, remaining) = self
            .remaining
            .split_at_checked(length)
            .ok_or_else(truncated)?;
        self.remaining = remaining;
        Ok(value)
    }

    fn finish(self, container: &'static str) -> Result<(), CodecError> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(CodecError::new(format!(
                "{container} contains trailing bytes"
            )))
        }
    }
}
