use crate::checkpoint::{Input, MAX_CHECKPOINT_BYTES};
use crate::connector::{Header, Record};
use crate::{Checkpoint, Error, ErrorKind};

const MAGIC: &[u8; 8] = b"DPDBDV01";
const VERSION: u16 = 1;
const CHECKSUM_BYTES: usize = size_of::<u32>();

#[derive(Debug)]
pub(crate) struct DecodedDelivery {
    pub(crate) checkpoint: Checkpoint,
    pub(crate) records: Box<[Record]>,
}

pub(crate) fn decode_delivery(
    bytes: &[u8],
    max_delivery_bytes: usize,
) -> Result<DecodedDelivery, Error> {
    if bytes.len() > max_delivery_bytes {
        return Err(Error::new(
            ErrorKind::DeliveryTooLarge,
            "Java bridge returned a delivery larger than its configured bound",
        ));
    }
    if bytes.len() < MAGIC.len() + size_of::<u16>() + CHECKSUM_BYTES {
        return Err(protocol("delivery is truncated"));
    }
    let (body, checksum) = bytes.split_at(bytes.len() - CHECKSUM_BYTES);
    let expected = u32::from_be_bytes(
        checksum
            .try_into()
            .map_err(|_| protocol("delivery checksum is truncated"))?,
    );
    if crc32fast::hash(body) != expected {
        return Err(protocol("delivery checksum does not match"));
    }

    let mut input = Input::new(body);
    if input.take(MAGIC.len()).map_err(as_protocol)? != MAGIC {
        return Err(protocol("delivery magic does not match"));
    }
    if input.u16().map_err(as_protocol)? != VERSION {
        return Err(protocol("delivery version is not supported"));
    }
    let checkpoint = Checkpoint::from_bytes(take_u32_bytes_bounded(
        &mut input,
        MAX_CHECKPOINT_BYTES,
        "delivery checkpoint",
    )?)
    .map_err(|_| protocol("delivery contains an invalid checkpoint"))?;

    let record_count = usize_from_u32(input.u32().map_err(as_protocol)?)?;
    let mut records = Vec::with_capacity(record_count.min(1024));
    for _ in 0..record_count {
        let topic = take_nullable_utf8(&mut input, "record topic")?;
        let kafka_partition = take_optional_i32(&mut input, "record partition")?;
        let timestamp = take_optional_i64(&mut input, "record timestamp")?;
        let key = take_nullable_bytes(&mut input)?;
        let value = take_nullable_bytes(&mut input)?;
        let header_count = usize_from_u32(input.u32().map_err(as_protocol)?)?;
        let mut headers = Vec::with_capacity(header_count.min(64));
        for _ in 0..header_count {
            let key = take_u32_utf8(&mut input, "header key")?;
            let value = take_nullable_bytes(&mut input)?;
            headers.push(Header::new(key, value));
        }
        records.push(Record::new(
            topic,
            kafka_partition,
            timestamp,
            key,
            value,
            headers.into_boxed_slice(),
        ));
    }
    if !input.is_empty() {
        return Err(protocol("delivery has trailing bytes"));
    }
    if records.is_empty() {
        return Err(protocol("delivery must contain at least one record"));
    }

    Ok(DecodedDelivery {
        checkpoint,
        records: records.into_boxed_slice(),
    })
}

fn take_optional_i32(input: &mut Input<'_>, label: &str) -> Result<Option<i32>, Error> {
    match input.take(1).map_err(as_protocol)?[0] {
        0 => Ok(None),
        1 => Ok(Some(input.i32().map_err(as_protocol)?)),
        _ => Err(protocol(format!("{label} presence flag is invalid"))),
    }
}

fn take_optional_i64(input: &mut Input<'_>, label: &str) -> Result<Option<i64>, Error> {
    match input.take(1).map_err(as_protocol)?[0] {
        0 => Ok(None),
        1 => Ok(Some(i64::from_be_bytes(
            input
                .take(size_of::<i64>())
                .map_err(as_protocol)?
                .try_into()
                .map_err(|_| protocol(format!("{label} is truncated")))?,
        ))),
        _ => Err(protocol(format!("{label} presence flag is invalid"))),
    }
}

fn take_nullable_bytes(input: &mut Input<'_>) -> Result<Option<Box<[u8]>>, Error> {
    let length = input.i32().map_err(as_protocol)?;
    match length {
        -1 => Ok(None),
        value if value >= 0 => Ok(Some(
            input
                .take(
                    usize::try_from(value)
                        .map_err(|_| protocol("delivery field length cannot be represented"))?,
                )
                .map_err(as_protocol)?
                .to_vec()
                .into_boxed_slice(),
        )),
        _ => Err(protocol("delivery field has an invalid negative length")),
    }
}

fn take_nullable_utf8(input: &mut Input<'_>, label: &str) -> Result<Option<Box<str>>, Error> {
    take_nullable_bytes(input)?
        .map(|bytes| {
            String::from_utf8(bytes.into_vec())
                .map(String::into_boxed_str)
                .map_err(|_| protocol(format!("{label} is not valid UTF-8")))
        })
        .transpose()
}

fn take_u32_utf8(input: &mut Input<'_>, label: &str) -> Result<Box<str>, Error> {
    String::from_utf8(take_u32_bytes(input)?)
        .map(String::into_boxed_str)
        .map_err(|_| protocol(format!("{label} is not valid UTF-8")))
}

fn take_u32_bytes(input: &mut Input<'_>) -> Result<Vec<u8>, Error> {
    let length = usize_from_u32(input.u32().map_err(as_protocol)?)?;
    Ok(input.take(length).map_err(as_protocol)?.to_vec())
}

fn take_u32_bytes_bounded(
    input: &mut Input<'_>,
    maximum: usize,
    label: &str,
) -> Result<Vec<u8>, Error> {
    let length = usize_from_u32(input.u32().map_err(as_protocol)?)?;
    if length > maximum {
        return Err(protocol(format!("{label} exceeds the protocol limit")));
    }
    Ok(input.take(length).map_err(as_protocol)?.to_vec())
}

fn usize_from_u32(value: u32) -> Result<usize, Error> {
    usize::try_from(value).map_err(|_| protocol("delivery length cannot be represented"))
}

fn as_protocol(_: Error) -> Error {
    protocol("delivery is truncated")
}

fn protocol(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Protocol, message)
}
