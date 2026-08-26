use super::Record;

const CANONICAL_NAN_BITS: u64 = 0x7ff8_0000_0000_0000;
const ABSOLUTE_MASK: u64 = 0x7fff_ffff_ffff_ffff;
const EXPONENT_MASK: u64 = 0x7ff0_0000_0000_0000;
const FRACTION_MASK: u64 = 0x000f_ffff_ffff_ffff;

/// An IEEE-754 `f64` with one representation for zero and one for NaN.
///
/// Negative zero is normalized to positive zero, and every NaN is normalized
/// to the same quiet NaN. All other bit patterns are preserved exactly.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CanonicalF64(u64);

impl CanonicalF64 {
    /// Creates a canonical floating-point value.
    #[must_use]
    pub const fn new(value: f64) -> Self {
        Self::from_bits(value.to_bits())
    }

    /// Creates a value while normalizing zero and NaN bit patterns.
    #[must_use]
    pub const fn from_bits(bits: u64) -> Self {
        Self(canonicalize_bits(bits))
    }

    /// Returns the represented floating-point value.
    #[must_use]
    pub const fn get(self) -> f64 {
        f64::from_bits(self.0)
    }

    /// Returns the canonical IEEE-754 bit pattern.
    #[must_use]
    pub const fn to_bits(self) -> u64 {
        self.0
    }

    pub(crate) const fn try_from_canonical_bits(bits: u64) -> Option<Self> {
        if canonicalize_bits(bits) == bits {
            Some(Self(bits))
        } else {
            None
        }
    }
}

impl From<f64> for CanonicalF64 {
    fn from(value: f64) -> Self {
        Self::new(value)
    }
}

impl From<CanonicalF64> for f64 {
    fn from(value: CanonicalF64) -> Self {
        value.get()
    }
}

/// A value in `DogPaddle`'s database-independent record model.
///
/// Numeric variants retain their type identity: for example, `I64(1)` and
/// `U64(1)` are different values.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Value {
    /// The explicit null value.
    Null,
    /// A Boolean value.
    Bool(bool),
    /// A signed 64-bit integer.
    I64(i64),
    /// An unsigned 64-bit integer.
    U64(u64),
    /// A canonical 64-bit floating-point value.
    F64(CanonicalF64),
    /// UTF-8 text.
    String(String),
    /// Arbitrary bytes.
    Bytes(Vec<u8>),
    /// An ordered sequence of values.
    Array(Vec<Self>),
    /// A nested canonical record.
    Object(Record),
}

const fn canonicalize_bits(bits: u64) -> u64 {
    if bits & ABSOLUTE_MASK == 0 {
        return 0;
    }
    if bits & EXPONENT_MASK == EXPONENT_MASK && bits & FRACTION_MASK != 0 {
        return CANONICAL_NAN_BITS;
    }
    bits
}
