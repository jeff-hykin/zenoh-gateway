//! Lossless depth on a data channel: nearest-neighbor downscale (never interpolated) + zstd.
//!
//! Wire format (little endian), see SPEC "Wire formats":
//! `u8 version=1 | u8 encoding (1 16UC1, 2 32FC1, 3 mono16) | u16 stride | u32 width | u32 height |
//!  u32 sourceWidth | u32 sourceHeight | zstd(width*height values, u16 or f32 LE)`

use crate::codec::image::{Depth, DepthValues};
use anyhow::Result;

pub const HEADER_LEN: usize = 20;
const ZSTD_LEVEL: i32 = 3;

/// Integer downscale factor for a quality: 1 at quality 1, up to 8 at quality 0.
pub fn stride(quality: f64) -> u32 {
    let linear_scale = 1.0 / 8.0 + 7.0 / 8.0 * quality.clamp(0.0, 1.0);
    (1.0 / linear_scale).round().max(1.0) as u32
}

/// Fraction of full-quality pixels sent at `quality` (the allocator's size prior).
pub fn size_factor(quality: f64) -> f64 {
    1.0 / (stride(quality) as f64).powi(2)
}

pub fn encode(depth: &Depth, quality: f64) -> Result<Vec<u8>> {
    let stride = stride(quality);
    let width = depth.width.div_ceil(stride);
    let height = depth.height.div_ceil(stride);
    let mut raw = Vec::with_capacity(width as usize * height as usize * 4);
    let source_index = |x: u32, y: u32| (y * stride) as usize * depth.width as usize + (x * stride) as usize;
    match &depth.values {
        DepthValues::U16(values) => {
            for y in 0..height {
                for x in 0..width {
                    raw.extend_from_slice(&values[source_index(x, y)].to_le_bytes());
                }
            }
        }
        DepthValues::F32(values) => {
            for y in 0..height {
                for x in 0..width {
                    raw.extend_from_slice(&values[source_index(x, y)].to_le_bytes());
                }
            }
        }
    }
    let mut out = Vec::with_capacity(HEADER_LEN + raw.len() / 2);
    out.push(1);
    out.push(depth.encoding as u8);
    out.extend_from_slice(&(stride as u16).to_le_bytes());
    for value in [width, height, depth.width, depth.height] {
        out.extend_from_slice(&value.to_le_bytes());
    }
    out.extend_from_slice(&zstd::bulk::compress(&raw, ZSTD_LEVEL)?);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::image::DepthEncoding;

    #[test]
    fn strides() {
        assert_eq!(stride(1.0), 1);
        assert_eq!(stride(0.5), 2);
        assert_eq!(stride(0.0), 8);
    }

    #[test]
    fn full_quality_round_trips_and_downscale_is_nearest() {
        let values: Vec<u16> = (0..12u16).collect();
        let depth = Depth { width: 4, height: 3, encoding: DepthEncoding::U16, values: DepthValues::U16(values.clone()) };
        let full = encode(&depth, 1.0).unwrap();
        assert_eq!(&full[..4], &[1, 1, 1, 0]);
        let decoded = zstd::bulk::decompress(&full[HEADER_LEN..], 1 << 20).unwrap();
        let decoded: Vec<u16> = decoded.as_chunks::<2>().0.iter().map(|&b| u16::from_le_bytes(b)).collect();
        assert_eq!(decoded, values);
        let half = encode(&depth, 0.5).unwrap();
        assert_eq!(u32::from_le_bytes(half[4..8].try_into().unwrap()), 2);
        assert_eq!(u32::from_le_bytes(half[8..12].try_into().unwrap()), 2);
        let decoded = zstd::bulk::decompress(&half[HEADER_LEN..], 1 << 20).unwrap();
        let decoded: Vec<u16> = decoded.as_chunks::<2>().0.iter().map(|&b| u16::from_le_bytes(b)).collect();
        assert_eq!(decoded, vec![0, 2, 8, 10], "every value is a source value, never a blend");
    }
}
