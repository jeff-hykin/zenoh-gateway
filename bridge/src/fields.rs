//! The fields format: named numbers, arrays and text that a [`CodecOutput::Fields`](crate::CodecOutput::Fields)
//! codec builds with [`Fields`] and the browser client decodes into a plain object (`msg.decoded`).
//!
//! Little endian: `u8 version=1 | u8 fieldCount | per field: u8 nameLen | name utf8 | u8 dtype |
//! u8 components (1..4) | u8 flags (bit0 scaled, bit1 scalar) | u32 count | [scaled: f64 offset[components] |
//! f64 scale[components]] | zero padding to a multiple of the dtype's size from the message start |
//! count × components values`. See SPEC "Fields".

use anyhow::{Result, bail, ensure};
use std::collections::BTreeMap;

/// The format version this crate writes and reads.
pub const VERSION: u8 = 1;
const SCALED: u8 = 1;
const SCALAR: u8 = 2;

/// A field's element type, as its wire code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Dtype {
    /// `u8`
    U8 = 0,
    /// `i8`
    I8 = 1,
    /// `u16`
    U16 = 2,
    /// `i16`
    I16 = 3,
    /// `u32`
    U32 = 4,
    /// `i32`
    I32 = 5,
    /// `f32`
    F32 = 6,
    /// `f64`
    F64 = 7,
    /// UTF-8 text (`count` bytes, one component); the client decodes it to a string
    Utf8 = 8,
}

impl Dtype {
    const ALL: [Dtype; 9] = [Dtype::U8, Dtype::I8, Dtype::U16, Dtype::I16, Dtype::U32, Dtype::I32, Dtype::F32, Dtype::F64, Dtype::Utf8];

    /// Bytes per value.
    pub fn size(self) -> usize {
        match self {
            Dtype::U8 | Dtype::I8 | Dtype::Utf8 => 1,
            Dtype::U16 | Dtype::I16 => 2,
            Dtype::U32 | Dtype::I32 | Dtype::F32 => 4,
            Dtype::F64 => 8,
        }
    }

    fn is_integer(self) -> bool {
        !matches!(self, Dtype::F32 | Dtype::F64 | Dtype::Utf8)
    }

    /// One value from its `size()` bytes.
    fn read(self, bytes: &[u8]) -> f64 {
        let mut word = [0u8; 8];
        word[..bytes.len()].copy_from_slice(bytes);
        let [a, b, c, d, ..] = word;
        match self {
            Dtype::U8 | Dtype::Utf8 => a as f64,
            Dtype::I8 => a as i8 as f64,
            Dtype::U16 => u16::from_le_bytes([a, b]) as f64,
            Dtype::I16 => i16::from_le_bytes([a, b]) as f64,
            Dtype::U32 => u32::from_le_bytes([a, b, c, d]) as f64,
            Dtype::I32 => i32::from_le_bytes([a, b, c, d]) as f64,
            Dtype::F32 => f32::from_le_bytes([a, b, c, d]) as f64,
            Dtype::F64 => f64::from_le_bytes(word),
        }
    }
}

/// A number type a field can hold (`u8`, `i8`, `u16`, `i16`, `u32`, `i32`, `f32`, `f64`).
pub trait Element: Copy + sealed::Sealed {
    /// Its wire type.
    const DTYPE: Dtype;
    /// Appends it little endian.
    fn put(self, out: &mut Vec<u8>);
}

mod sealed {
    pub trait Sealed {}
}

macro_rules! element {
    ($($type:ty => $dtype:ident),*) => {$(
        impl sealed::Sealed for $type {}
        impl Element for $type {
            const DTYPE: Dtype = Dtype::$dtype;
            fn put(self, out: &mut Vec<u8>) {
                out.extend_from_slice(&self.to_le_bytes());
            }
        }
    )*};
}
element!(u8 => U8, i8 => I8, u16 => U16, i16 => I16, u32 => U32, i32 => I32, f32 => F32, f64 => F64);

/// Builds a fields message; the methods panic on misuse (a name over 255 bytes, more than 255
/// fields, components outside 1..4 or not dividing the values, scaling a float).
///
/// ```
/// let bytes = zenoh_web::Fields::new()
///     .scalar("width", 2u32)
///     .array("data", &[1u16, 2, 3, 4])
///     .scaled("positions", &[10.0, 0.0, 0.0], &[0.001; 3], &[1i16, 2, 3])
///     .text("encoding", "16UC1")
///     .build();
/// let fields = zenoh_web::fields::parse(&bytes).unwrap();
/// assert_eq!(fields["positions"].values(), [10.001, 0.002, 0.003]);
/// ```
#[derive(Debug, Clone)]
pub struct Fields {
    bytes: Vec<u8>,
}

impl Default for Fields {
    fn default() -> Self {
        Fields { bytes: vec![VERSION, 0] }
    }
}

impl Fields {
    /// An empty message.
    pub fn new() -> Self {
        Self::default()
    }

    /// One number (the client gets a `number`).
    pub fn scalar<T: Element>(self, name: &str, value: T) -> Self {
        self.push(name, (T::DTYPE, 1, 1), SCALAR, None, |out| value.put(out))
    }

    /// One value per element (the client gets the matching typed array).
    pub fn array<T: Element>(self, name: &str, values: &[T]) -> Self {
        self.vectors(name, 1, values)
    }

    /// `components` values per element, interleaved (e.g. x, y, z per point).
    pub fn vectors<T: Element>(self, name: &str, components: u8, values: &[T]) -> Self {
        self.push(name, (T::DTYPE, components, values.len()), 0, None, |out| values.iter().for_each(|value| value.put(out)))
    }

    /// Quantized integers the client turns into a `Float32Array` of `offset[c] + value × scale[c]`,
    /// one offset and scale per component (`offset.len()` components).
    pub fn scaled<T: Element>(self, name: &str, offset: &[f64], scale: &[f64], values: &[T]) -> Self {
        assert!(T::DTYPE.is_integer(), "fields: scaled field {name:?} must hold integers");
        assert_eq!(offset.len(), scale.len(), "fields: {name:?} needs one offset and one scale per component");
        self.push(name, (T::DTYPE, offset.len() as u8, values.len()), SCALED, Some((offset, scale)), |out| values.iter().for_each(|value| value.put(out)))
    }

    /// UTF-8 text (the client gets a `string`).
    pub fn text(self, name: &str, text: &str) -> Self {
        self.push(name, (Dtype::Utf8, 1, text.len()), 0, None, |out| out.extend_from_slice(text.as_bytes()))
    }

    /// The message bytes.
    pub fn build(self) -> Vec<u8> {
        self.bytes
    }

    fn push(mut self, name: &str, (dtype, components, values): (Dtype, u8, usize), flags: u8, scaling: Option<(&[f64], &[f64])>, write: impl FnOnce(&mut Vec<u8>)) -> Self {
        assert!(name.len() <= 255 && self.bytes[1] < 255, "fields: at most 255 fields with names of at most 255 bytes ({name:?})");
        assert!((1..=4).contains(&components) && values.is_multiple_of(components as usize), "fields: {name:?} has {values} values, not a whole number of {components}-component elements");
        let count = u32::try_from(values / components as usize).expect("fields: more than u32::MAX elements");
        let out = &mut self.bytes;
        out[1] += 1;
        out.push(name.len() as u8);
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(&[dtype as u8, components, flags]);
        out.extend_from_slice(&count.to_le_bytes());
        if let Some((offset, scale)) = scaling {
            offset.iter().chain(scale).for_each(|value| value.put(out));
        }
        out.resize(out.len().next_multiple_of(dtype.size()), 0);
        write(out);
        self
    }
}

/// One parsed field (Rust readers and tests; browsers use the client's `decodeFields`).
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    /// Element type.
    pub dtype: Dtype,
    /// Values per element.
    pub components: u8,
    /// Elements.
    pub count: u32,
    /// Written with [`Fields::scalar`] (the client gets a number, not an array).
    pub scalar: bool,
    /// `(offset, scale)` per component, for scaled fields.
    pub scaling: Option<(Vec<f64>, Vec<f64>)>,
    /// The raw values, little endian.
    pub data: Vec<u8>,
}

impl Field {
    /// Every value as f64, scaling applied.
    pub fn values(&self) -> Vec<f64> {
        let size = self.dtype.size();
        self.data.chunks_exact(size).enumerate().map(|(index, bytes)| {
            let value = self.dtype.read(bytes);
            match &self.scaling {
                Some((offset, scale)) => offset[index % offset.len()] + value * scale[index % scale.len()],
                None => value,
            }
        }).collect()
    }

    /// A text field's string.
    pub fn text(&self) -> Option<&str> {
        (self.dtype == Dtype::Utf8).then(|| std::str::from_utf8(&self.data).ok()).flatten()
    }
}

/// Parses a fields message by name.
pub fn parse(bytes: &[u8]) -> Result<BTreeMap<String, Field>> {
    ensure!(bytes.len() >= 2 && bytes[0] == VERSION, "not a fields v{VERSION} message");
    let mut at = 2usize;
    // the next n bytes, after padding to a multiple of `align`
    let mut take = |n: usize, align: usize| -> Result<&[u8]> {
        let start = at.next_multiple_of(align);
        ensure!(start + n <= bytes.len(), "fields message truncated at byte {start}");
        at = start + n;
        Ok(&bytes[start..at])
    };
    let mut fields = BTreeMap::new();
    for _ in 0..bytes[1] {
        let name_len = take(1, 1)?[0] as usize;
        let name = String::from_utf8(take(name_len, 1)?.to_vec())?;
        let &[dtype, components, flags, a, b, c, d] = take(7, 1)? else { unreachable!() };
        let Some(&dtype) = Dtype::ALL.get(dtype as usize) else { bail!("field {name:?}: unknown dtype {dtype}") };
        let count = u32::from_le_bytes([a, b, c, d]);
        let scaling = if flags & SCALED != 0 {
            let numbers: Vec<f64> = take(16 * components as usize, 1)?.as_chunks::<8>().0.iter().map(|bytes| f64::from_le_bytes(*bytes)).collect();
            Some((numbers[..components as usize].to_vec(), numbers[components as usize..].to_vec()))
        } else {
            None
        };
        let data = take(count as usize * components as usize * dtype.size(), dtype.size())?.to_vec();
        fields.insert(name, Field { dtype, components, count, scalar: flags & SCALAR != 0, scaling, data });
    }
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_alignment() {
        let bytes = Fields::new()
            .text("encoding", "32FC1")
            .scalar("width", 3u32)
            .array("data", &[1.5f32, -2.0, 0.25])
            .vectors("origin", 3, &[1.0f64, 2.0, 3.0])
            .scaled("positions", &[1.0, 2.0, 3.0], &[0.5, 0.25, 2.0], &[2i16, -4, 1, 0, 0, 0])
            .array("intensity", &[7u8, 9])
            .build();
        let fields = parse(&bytes).unwrap();
        assert_eq!(fields["encoding"].text(), Some("32FC1"));
        assert_eq!(fields["width"].values(), [3.0]);
        assert!(fields["width"].scalar && !fields["intensity"].scalar);
        assert_eq!(fields["data"].values(), [1.5, -2.0, 0.25]);
        assert_eq!((fields["origin"].components, fields["origin"].count), (3, 1));
        assert_eq!(fields["positions"].values(), [2.0, 1.0, 5.0, 1.0, 2.0, 3.0]);
        assert_eq!(fields["intensity"].values(), [7.0, 9.0]);
        // each field's values start at a multiple of their size from the message start
        let data_at = bytes.windows(12).position(|window| window == [0, 0, 0xc0, 0x3f, 0, 0, 0, 0xc0, 0, 0, 0x80, 0x3e]).unwrap();
        assert_eq!(data_at % 4, 0);
        assert!(parse(&bytes[..bytes.len() - 1]).is_err(), "truncated");
        assert!(parse(&[2, 0]).is_err(), "unknown version");
    }

    #[test]
    #[should_panic(expected = "must hold integers")]
    fn scaled_floats_are_refused() {
        Fields::new().scaled("x", &[0.0], &[1.0], &[1.0f32]);
    }
}
