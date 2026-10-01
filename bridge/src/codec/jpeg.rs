//! Video codecs' frames as JPEG files on the data channel (subscribe option `imageTransport:
//! "jpeg"`), for browsers where decoding a small JPEG with `createImageBitmap` beats the WebRTC
//! video pipeline (no jitter buffer, no decoder warm-up, every frame independent).
//!
//! Quality `q` (0..1) sets the same resolution scale as H.264 (`video::scaled_size`) and a JPEG
//! quality of `35 + 55 q`. A source that already is a JPEG goes out untouched at full resolution
//! (`q = 1`), without a decode.

use crate::codec::VideoImage;
use crate::codec::video::{scaled_size, to_i420};
use anyhow::{Result, anyhow};
use jpeg_encoder::{Encoder, ImageBuffer, JpegColorType};

/// The JPEG quality setting (1..100) at allocator quality `quality`.
pub fn jpeg_quality(quality: f64) -> u8 {
    (35.0 + 55.0 * quality.clamp(0.0, 1.0)).round() as u8
}

/// Whether `quality` sends the source at its own size (so a JPEG source can pass through).
pub fn is_full_size(quality: f64) -> bool {
    quality >= 1.0 - 1e-9
}

/// Modeled bytes per JPEG at `quality` for a `width × height` source: the allocator's prior until
/// sizes are measured, and the shape between measured qualities.
pub fn bytes_per_frame(width: u32, height: u32, quality: f64) -> f64 {
    let (scaled_width, scaled_height) = scaled_size(width, height, quality);
    scaled_width as f64 * scaled_height as f64 * (0.25 + 1.0 * quality.clamp(0.0, 1.0)) / 8.0
}

/// Limited-range (BT.601, what the I420 path carries) to the full range a JFIF file holds.
struct FullRange {
    luma: [u8; 256],
    chroma: [u8; 256],
}

impl FullRange {
    fn new() -> Self {
        let scale = |value: usize, offset: f64, range: f64, center: f64| ((value as f64 - offset) * 255.0 / range + center).round().clamp(0.0, 255.0) as u8;
        FullRange { luma: std::array::from_fn(|value| scale(value, 16.0, 219.0, 0.0)), chroma: std::array::from_fn(|value| scale(value, 128.0, 224.0, 128.0)) }
    }
}

/// Planar I420 rows handed to the encoder as interleaved full-range YCbCr.
struct I420<'a> {
    planes: &'a [u8],
    width: usize,
    height: usize,
    range: &'a FullRange,
}

impl ImageBuffer for I420<'_> {
    fn get_jpeg_color_type(&self) -> JpegColorType {
        JpegColorType::Ycbcr
    }

    fn width(&self) -> u16 {
        self.width as u16
    }

    fn height(&self) -> u16 {
        self.height as u16
    }

    fn fill_buffers(&self, y: u16, buffers: &mut [Vec<u8>; 4]) {
        let (width, y) = (self.width, y as usize);
        let luma = &self.planes[y * width..][..width];
        let chroma_offset = (y / 2) * (width / 2);
        let u = &self.planes[width * self.height + chroma_offset..][..width / 2];
        let v = &self.planes[width * self.height * 5 / 4 + chroma_offset..][..width / 2];
        for (x, &value) in luma.iter().enumerate() {
            buffers[0].push(self.range.luma[value as usize]);
            buffers[1].push(self.range.chroma[u[x / 2] as usize]);
            buffers[2].push(self.range.chroma[v[x / 2] as usize]);
        }
    }
}

/// One encoded JPEG file and the size it shows.
pub struct EncodedJpeg {
    pub data: Vec<u8>,
    pub width: u32,
    pub height: u32,
}

/// `image` scaled and encoded for allocator quality `quality`.
pub fn encode(image: &VideoImage, quality: f64) -> Result<EncodedJpeg> {
    let (width, height) = scaled_size(image.width(), image.height(), quality);
    if width > u16::MAX as u32 || height > u16::MAX as u32 {
        return Err(anyhow!("{width}x{height} is too large for a JPEG"));
    }
    let planes = to_i420(image, width, height);
    let range = FullRange::new();
    let mut data = Vec::with_capacity((bytes_per_frame(image.width(), image.height(), quality) * 1.5) as usize);
    let encoder = Encoder::new(&mut data, jpeg_quality(quality));
    encoder.encode_image(I420 { planes: &planes, width: width as usize, height: height as usize, range: &range }).map_err(|error| anyhow!("jpeg: {error}"))?;
    Ok(EncodedJpeg { data, width, height })
}

/// A JPEG's width and height from its header, without decoding it.
pub fn jpeg_size(data: &[u8]) -> Option<(u32, u32)> {
    let mut decoder = zune_jpeg::JpegDecoder::new(zune_jpeg::zune_core::bytestream::ZCursor::new(data));
    decoder.decode_headers().ok()?;
    decoder.dimensions().map(|(width, height)| (width as u32, height as u32))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::image::Rgb8;

    fn quadrants(width: u32, height: u32) -> VideoImage {
        let mut pixels = Vec::with_capacity((width * height * 3) as usize);
        for y in 0..height {
            for x in 0..width {
                pixels.extend_from_slice(match (y < height / 2, x < width / 2) {
                    (true, true) => &[255, 0, 0],
                    (true, false) => &[0, 255, 0],
                    (false, true) => &[0, 0, 255],
                    (false, false) => &[255, 255, 255],
                });
            }
        }
        Rgb8 { width, height, pixels }.into()
    }

    fn decode(data: &[u8]) -> (usize, usize, Vec<u8>) {
        use zune_jpeg::zune_core::{bytestream::ZCursor, colorspace::ColorSpace, options::DecoderOptions};
        let mut decoder = zune_jpeg::JpegDecoder::new_with_options(ZCursor::new(data), DecoderOptions::default().jpeg_set_out_colorspace(ColorSpace::RGB));
        let pixels = decoder.decode().unwrap();
        let (width, height) = decoder.dimensions().unwrap();
        (width, height, pixels)
    }

    #[test]
    fn encodes_scaled_quadrants_that_decode_to_the_same_colors() {
        for quality in [1.0, 0.5, 0.0] {
            let frame = encode(&quadrants(320, 240), quality).unwrap();
            assert_eq!((frame.width, frame.height), scaled_size(320, 240, quality));
            let (width, height, pixels) = decode(&frame.data);
            assert_eq!((width as u32, height as u32), (frame.width, frame.height));
            for ((x, y), expected) in [((0.25, 0.25), [255, 0, 0]), ((0.75, 0.25), [0, 255, 0]), ((0.25, 0.75), [0, 0, 255]), ((0.75, 0.75), [255, 255, 255])] {
                let at = (((y * height as f64) as usize) * width + (x * width as f64) as usize) * 3;
                for channel in 0..3 {
                    let error = (pixels[at + channel] as i32 - expected[channel]).abs();
                    assert!(error <= 12, "q{quality} quadrant ({x},{y}) channel {channel}: {:?} vs {expected:?}", &pixels[at..at + 3]);
                }
            }
        }
    }

    #[test]
    fn lower_quality_is_smaller_and_the_model_is_monotone() {
        let image = quadrants(320, 240);
        let high = encode(&image, 1.0).unwrap().data.len();
        let low = encode(&image, 0.2).unwrap().data.len();
        assert!(low < high, "{low} >= {high}");
        assert!(bytes_per_frame(320, 240, 0.2) < bytes_per_frame(320, 240, 0.8));
        assert_eq!(jpeg_quality(0.0), 35);
        assert_eq!(jpeg_quality(1.0), 90);
    }

    #[test]
    fn reads_the_size_from_the_header() {
        let frame = encode(&quadrants(320, 240), 1.0).unwrap();
        assert_eq!(jpeg_size(&frame.data), Some((320, 240)));
        assert_eq!(jpeg_size(b"not a jpeg"), None);
    }
}
