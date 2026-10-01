//! Pixel decoding: raw `sensor_msgs/Image` encodings and compressed files (jpeg, png, webp, jxl)
//! to packed RGB8 (for video) or to lossless depth values (u16 / f32).

use crate::codec::wire::RawImage;
use crate::codec::{PixelFormat, VideoImage};
use anyhow::{Context, Result, bail, ensure};
use std::io::Cursor;

/// Packed 8-bit RGB, row-major, no padding.
pub struct Rgb8 {
    pub width: u32,
    pub height: u32,
    pub pixels: Vec<u8>,
}

impl From<Rgb8> for VideoImage {
    fn from(rgb: Rgb8) -> Self {
        VideoImage { width: rgb.width, height: rgb.height, format: PixelFormat::Rgb8, data: rgb.pixels }
    }
}

/// Depth wire encodings (see SPEC "Wire formats": depth header byte 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepthEncoding {
    /// 16UC1: u16, millimeters by convention
    U16 = 1,
    /// 32FC1: f32, meters by convention
    F32 = 2,
    /// mono16: u16 intensity (e.g. an IR camera); delivered losslessly like depth when asked for
    Mono16 = 3,
}

pub enum DepthValues {
    U16(Vec<u16>),
    F32(Vec<f32>),
}

pub struct Depth {
    pub width: u32,
    pub height: u32,
    pub encoding: DepthEncoding,
    pub values: DepthValues,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    Jpeg,
    Png,
    Webp,
    Jxl,
}

/// The file format, from magic bytes first and the message's format string second.
pub fn sniff_format(data: &[u8], hint: &str) -> Result<FileFormat> {
    if data.starts_with(&[0xff, 0xd8, 0xff]) {
        return Ok(FileFormat::Jpeg);
    }
    if data.starts_with(&[0x89, b'P', b'N', b'G']) {
        return Ok(FileFormat::Png);
    }
    if data.len() >= 12 && &data[0..4] == b"RIFF" && &data[8..12] == b"WEBP" {
        return Ok(FileFormat::Webp);
    }
    if data.starts_with(&[0xff, 0x0a]) || data.starts_with(&[0, 0, 0, 0x0c, b'J', b'X', b'L', b' ']) {
        return Ok(FileFormat::Jxl);
    }
    let hint = hint.to_ascii_lowercase();
    for (name, format) in [("jpeg", FileFormat::Jpeg), ("jpg", FileFormat::Jpeg), ("png", FileFormat::Png), ("webp", FileFormat::Webp), ("jxl", FileFormat::Jxl)] {
        if hint.contains(name) {
            return Ok(format);
        }
    }
    bail!("unrecognized compressed image (format {hint:?}, magic {:02x?})", &data[..data.len().min(8)])
}

fn row<'a>(image: &RawImage<'a>, y: u32, bytes_per_pixel: usize) -> Result<&'a [u8]> {
    let width_bytes = image.width as usize * bytes_per_pixel;
    let step = if image.step == 0 { width_bytes } else { image.step as usize };
    ensure!(step >= width_bytes, "step {step} < width {} x {bytes_per_pixel} bytes", image.width);
    let start = y as usize * step;
    image.data.get(start..start + width_bytes).with_context(|| format!("image data too short for row {y} ({} bytes)", image.data.len()))
}

fn read_u16(bytes: &[u8], big_endian: bool) -> u16 {
    if big_endian { u16::from_be_bytes([bytes[0], bytes[1]]) } else { u16::from_le_bytes([bytes[0], bytes[1]]) }
}

/// Writes one pixel's RGB from its source bytes (and the message's big-endian flag).
type PixelConverter = fn(&[u8], bool, &mut [u8]);

/// A raw image (or a dimos Image carrying a jpeg/png file) as RGB8. 16-bit gray keeps its top 8 bits.
pub fn raw_to_rgb(image: &RawImage) -> Result<Rgb8> {
    let encoding = image.encoding.to_ascii_lowercase();
    if matches!(encoding.as_str(), "jpeg" | "jpg" | "png" | "webp" | "jxl") {
        return compressed_to_rgb(image.data, &encoding);
    }
    // (bytes per pixel, writes one pixel's RGB from its source bytes)
    let (bytes_per_pixel, convert): (usize, PixelConverter) = match encoding.as_str() {
        "rgb8" => (3, |s, _, d| d.copy_from_slice(&s[..3])),
        "bgr8" | "8uc3" => (3, |s, _, d| d.copy_from_slice(&[s[2], s[1], s[0]])),
        "rgba8" => (4, |s, _, d| d.copy_from_slice(&s[..3])),
        "bgra8" | "8uc4" => (4, |s, _, d| d.copy_from_slice(&[s[2], s[1], s[0]])),
        "mono8" | "8uc1" => (1, |s, _, d| d.fill(s[0])),
        "mono16" | "16uc1" => (2, |s, big, d| d.fill((read_u16(s, big) >> 8) as u8)),
        other => bail!("image encoding {other:?} has no color conversion (supported: rgb8 bgr8 rgba8 bgra8 mono8 mono16 16UC1 jpeg png)"),
    };
    ensure!(image.width > 0 && image.height > 0, "empty image");
    let mut pixels = vec![0u8; image.width as usize * image.height as usize * 3];
    for y in 0..image.height {
        let source = row(image, y, bytes_per_pixel)?;
        let destination = &mut pixels[y as usize * image.width as usize * 3..][..image.width as usize * 3];
        for (source_pixel, destination_pixel) in source.chunks_exact(bytes_per_pixel).zip(destination.as_chunks_mut::<3>().0.iter_mut()) {
            convert(source_pixel, image.big_endian, destination_pixel);
        }
    }
    Ok(Rgb8 { width: image.width, height: image.height, pixels })
}

/// Expands decoded samples of `channels` per pixel (1 gray, 2 gray+alpha, 3 rgb, 4 rgba) to RGB8.
fn to_rgb(samples: &[u8], channels: usize, width: u32, height: u32) -> Result<Rgb8> {
    ensure!(samples.len() >= width as usize * height as usize * channels, "decoder returned too few samples");
    let pixels = match channels {
        3 => samples[..width as usize * height as usize * 3].to_vec(),
        1 | 2 => samples.chunks_exact(channels).flat_map(|p| [p[0], p[0], p[0]]).collect(),
        4 => samples.as_chunks::<4>().0.iter().flat_map(|p| [p[0], p[1], p[2]]).collect(),
        other => bail!("{other} channels per pixel"),
    };
    Ok(Rgb8 { width, height, pixels })
}

/// A raw image for the video path: like [`raw_to_rgb`], except a JPEG goes straight to I420
/// (see [`compressed_to_video`]).
pub fn raw_to_video(image: &RawImage) -> Result<VideoImage> {
    let encoding = image.encoding.to_ascii_lowercase();
    if matches!(encoding.as_str(), "jpeg" | "jpg" | "png" | "webp" | "jxl") {
        return compressed_to_video(image.data, &encoding);
    }
    Ok(raw_to_rgb(image)?.into())
}

/// A compressed image for the video path. A YCbCr JPEG with even sides is decoded to I420 without
/// ever becoming RGB: the encoder wants YUV anyway, and the YCbCr -> RGB -> YUV round trip was
/// over half the decode on an ARM core (37 ms of a 1920x1536 frame on a Jetson Orin, against 16).
/// Anything else decodes to RGB.
pub fn compressed_to_video(data: &[u8], hint: &str) -> Result<VideoImage> {
    if sniff_format(data, hint)? == FileFormat::Jpeg
        && let Some(image) = jpeg_to_i420(data)?
    {
        return Ok(image);
    }
    Ok(compressed_to_rgb(data, hint)?.into())
}

/// JFIF's full-range YCbCr as BT.601 limited-range I420 (what the H.264 stream is tagged as, and
/// what [`crate::codec::video`]'s RGB conversion produces), chroma from each 2x2 block's mean.
/// `None` for a JPEG that isn't YCbCr or has an odd side.
fn jpeg_to_i420(data: &[u8]) -> Result<Option<VideoImage>> {
    use zune_jpeg::zune_core::bytestream::ZCursor;
    use zune_jpeg::zune_core::colorspace::ColorSpace;
    use zune_jpeg::zune_core::options::DecoderOptions;
    let options = DecoderOptions::default().jpeg_set_out_colorspace(ColorSpace::YCbCr);
    let mut decoder = zune_jpeg::JpegDecoder::new_with_options(ZCursor::new(data), options);
    decoder.decode_headers().map_err(|e| anyhow::anyhow!("jpeg: {e:?}"))?;
    let info = decoder.info().context("jpeg: no header")?;
    let (width, height) = (info.width as usize, info.height as usize);
    if decoder.input_colorspace() != Some(ColorSpace::YCbCr) || width == 0 || height == 0 || width % 2 == 1 || height % 2 == 1 {
        return Ok(None);
    }
    let pixels = decoder.decode().map_err(|e| anyhow::anyhow!("jpeg: {e:?}"))?;
    ensure!(pixels.len() >= width * height * 3, "jpeg: decoder returned too few samples");
    let luma_table: [u8; 256] = std::array::from_fn(|value| (16 + (value as u32 * 219 + 127) / 255) as u8);
    let chroma_table: [u8; 1021] = std::array::from_fn(|sum| (16 + (sum as u32 * 224 + 510) / 1020) as u8);
    let mut out = vec![0u8; width * height * 3 / 2];
    let (luma, chroma) = out.split_at_mut(width * height);
    let (u_plane, v_plane) = chroma.split_at_mut(width * height / 4);
    for (luma_value, pixel) in luma.iter_mut().zip(pixels.as_chunks::<3>().0) {
        *luma_value = luma_table[pixel[0] as usize];
    }
    let half_width = width / 2;
    for (block_row, (u_row, v_row)) in u_plane.chunks_exact_mut(half_width).zip(v_plane.chunks_exact_mut(half_width)).enumerate() {
        let top = pixels[block_row * 2 * width * 3..][..width * 3].as_chunks::<6>().0;
        let bottom = pixels[(block_row * 2 + 1) * width * 3..][..width * 3].as_chunks::<6>().0;
        for ((u, v), (a, b)) in u_row.iter_mut().zip(v_row.iter_mut()).zip(top.iter().zip(bottom)) {
            *u = chroma_table[a[1] as usize + a[4] as usize + b[1] as usize + b[4] as usize];
            *v = chroma_table[a[2] as usize + a[5] as usize + b[2] as usize + b[5] as usize];
        }
    }
    Ok(Some(VideoImage::i420(width as u32, height as u32, out)?))
}

pub fn compressed_to_rgb(data: &[u8], hint: &str) -> Result<Rgb8> {
    match sniff_format(data, hint)? {
        FileFormat::Jpeg => {
            use zune_jpeg::zune_core::bytestream::ZCursor;
            use zune_jpeg::zune_core::colorspace::ColorSpace;
            use zune_jpeg::zune_core::options::DecoderOptions;
            let options = DecoderOptions::default().jpeg_set_out_colorspace(ColorSpace::RGB);
            let mut decoder = zune_jpeg::JpegDecoder::new_with_options(ZCursor::new(data), options);
            let pixels = decoder.decode().map_err(|e| anyhow::anyhow!("jpeg: {e:?}"))?;
            let info = decoder.info().context("jpeg: no header")?;
            to_rgb(&pixels, 3, info.width as u32, info.height as u32)
        }
        FileFormat::Png => {
            let (info, samples) = decode_png(data, png::Transformations::EXPAND | png::Transformations::STRIP_16)?;
            to_rgb(&samples, info.color_type.samples(), info.width, info.height)
        }
        FileFormat::Webp => {
            let mut decoder = image_webp::WebPDecoder::new(Cursor::new(data)).map_err(|e| anyhow::anyhow!("webp: {e}"))?;
            let (width, height) = decoder.dimensions();
            let channels = if decoder.has_alpha() { 4 } else { 3 };
            let mut samples = vec![0u8; decoder.output_buffer_size().context("webp: image too large")?];
            decoder.read_image(&mut samples).map_err(|e| anyhow::anyhow!("webp: {e}"))?;
            to_rgb(&samples, channels, width, height)
        }
        FileFormat::Jxl => {
            let image = jxl_oxide::JxlImage::builder().read(data).map_err(|e| anyhow::anyhow!("jxl: {e}"))?;
            let render = image.render_frame(0).map_err(|e| anyhow::anyhow!("jxl: {e}"))?;
            let mut stream = render.stream_no_alpha();
            let (width, height, channels) = (stream.width(), stream.height(), stream.channels() as usize);
            let mut samples = vec![0u8; width as usize * height as usize * channels];
            stream.write_to_buffer(&mut samples);
            to_rgb(&samples, channels, width, height)
        }
    }
}

fn decode_png(data: &[u8], transformations: png::Transformations) -> Result<(png::OutputInfo, Vec<u8>)> {
    let mut decoder = png::Decoder::new(Cursor::new(data));
    decoder.set_transformations(transformations);
    let mut reader = decoder.read_info().map_err(|e| anyhow::anyhow!("png: {e}"))?;
    let mut samples = vec![0u8; reader.output_buffer_size().context("png: image too large")?];
    let info = reader.next_frame(&mut samples).map_err(|e| anyhow::anyhow!("png: {e}"))?;
    samples.truncate(info.line_size * info.height as usize);
    Ok((info, samples))
}

/// A raw 16UC1 / 32FC1 / mono16 image as lossless depth values.
pub fn raw_to_depth(image: &RawImage) -> Result<Depth> {
    let (width, height) = (image.width, image.height);
    let pixel_count = width as usize * height as usize;
    match image.encoding.as_str() {
        "16UC1" | "mono16" => {
            let mut values = Vec::with_capacity(pixel_count);
            for y in 0..height {
                values.extend(row(image, y, 2)?.as_chunks::<2>().0.iter().map(|s| read_u16(s, image.big_endian)));
            }
            let encoding = if image.encoding == "mono16" { DepthEncoding::Mono16 } else { DepthEncoding::U16 };
            Ok(Depth { width, height, encoding, values: DepthValues::U16(values) })
        }
        "32FC1" => {
            let mut values = Vec::with_capacity(pixel_count);
            for y in 0..height {
                values.extend(row(image, y, 4)?.as_chunks::<4>().0.iter().map(|&bytes| {
                    if image.big_endian { f32::from_be_bytes(bytes) } else { f32::from_le_bytes(bytes) }
                }));
            }
            Ok(Depth { width, height, encoding: DepthEncoding::F32, values: DepthValues::F32(values) })
        }
        other => bail!("image encoding {other:?} is not depth (supported: 16UC1 32FC1 mono16)"),
    }
}

/// A 16-bit single-channel png or jxl (also ROS `compressedDepth` png) as lossless u16 depth.
pub fn compressed_to_depth(data: &[u8], format: &str) -> Result<Depth> {
    // compressed_depth_image_transport prefixes a 12-byte config header (format enum + 2 floats)
    let data = if format.contains("compressedDepth") {
        ensure!(!format.contains("32FC1"), "compressedDepth 32FC1 is quantized inverse depth, not lossless; unsupported");
        data.get(12..).context("compressedDepth payload shorter than its header")?
    } else {
        data
    };
    match sniff_format(data, format)? {
        FileFormat::Png => {
            let (info, samples) = decode_png(data, png::Transformations::IDENTITY)?;
            ensure!(info.color_type == png::ColorType::Grayscale && info.bit_depth == png::BitDepth::Sixteen,
                "png depth must be 16-bit grayscale, got {:?} {:?}", info.color_type, info.bit_depth);
            let values = samples.as_chunks::<2>().0.iter().map(|&s| u16::from_be_bytes(s)).collect();
            Ok(Depth { width: info.width, height: info.height, encoding: DepthEncoding::U16, values: DepthValues::U16(values) })
        }
        FileFormat::Jxl => {
            let image = jxl_oxide::JxlImage::builder().read(data).map_err(|e| anyhow::anyhow!("jxl: {e}"))?;
            let bits = image.image_header().metadata.bit_depth.bits_per_sample();
            ensure!(bits > 8 && bits <= 16, "jxl depth must be 9..16 bits per sample, got {bits}");
            let render = image.render_frame(0).map_err(|e| anyhow::anyhow!("jxl: {e}"))?;
            let mut stream = render.stream_no_alpha();
            ensure!(stream.channels() == 1, "jxl depth must have one channel, got {}", stream.channels());
            let (width, height) = (stream.width(), stream.height());
            let mut values = vec![0u16; width as usize * height as usize];
            stream.write_to_buffer(&mut values);
            Ok(Depth { width, height, encoding: DepthEncoding::U16, values: DepthValues::U16(values) })
        }
        other => bail!("{other:?} can't carry lossless 16-bit depth (use png or jxl)"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jpeg_goes_to_i420_like_rgb_would() {
        let jpeg = include_bytes!("../../examples/test_image.jpg");
        let direct = compressed_to_video(jpeg, "jpeg").unwrap();
        assert_eq!(direct.format(), PixelFormat::I420, "a YCbCr jpeg skips rgb");
        let rgb = compressed_to_rgb(jpeg, "jpeg").unwrap();
        let through_rgb = crate::codec::video::rgb_to_i420(&rgb.pixels, rgb.width as usize, rgb.height as usize);
        // RGB clips out-of-gamut YCbCr, so a few saturated pixels may differ more; on average they agree
        let mut differences: Vec<i32> = direct.data().iter().zip(&through_rgb).map(|(a, b)| (*a as i32 - *b as i32).abs()).collect();
        differences.sort();
        let mean = differences.iter().sum::<i32>() as f64 / differences.len() as f64;
        let p99 = differences[differences.len() * 99 / 100];
        assert!(mean < 1.0 && p99 <= 5, "I420 straight from the jpeg differs from going through RGB: mean {mean:.2}, p99 {p99}");
    }
    use crate::codec::wire::{Protocol, parse_compressed_image, parse_image, tests::fixture};

    fn quadrant_means(rgb: &Rgb8) -> [[f64; 3]; 4] {
        let mut sums = [[0f64; 3]; 4];
        let mut counts = [0f64; 4];
        let (half_width, half_height) = (rgb.width / 2, rgb.height / 2);
        for y in 0..rgb.height {
            for x in 0..rgb.width {
                let quadrant = (if y < half_height { 0 } else { 2 }) + (if x < half_width { 0 } else { 1 });
                let pixel = &rgb.pixels[(y * rgb.width + x) as usize * 3..][..3];
                for channel in 0..3 {
                    sums[quadrant][channel] += pixel[channel] as f64;
                }
                counts[quadrant] += 1.0;
            }
        }
        std::array::from_fn(|quadrant| sums[quadrant].map(|sum| sum / counts[quadrant]))
    }

    const PATTERN: [[f64; 3]; 4] = [[255.0, 0.0, 0.0], [0.0, 255.0, 0.0], [0.0, 0.0, 255.0], [255.0, 255.0, 255.0]];

    fn assert_pattern(rgb: &Rgb8, tolerance: f64, what: &str) {
        assert_eq!((rgb.width, rgb.height), (320, 240), "{what}");
        let means = quadrant_means(rgb);
        for (quadrant, expected) in PATTERN.iter().enumerate() {
            for channel in 0..3 {
                assert!((means[quadrant][channel] - expected[channel]).abs() <= tolerance, "{what}: quadrant {quadrant} = {:?}", means[quadrant]);
            }
        }
    }

    #[test]
    fn raw_color_encodings() {
        for file in ["ros2/image_rgb8.cdr", "ros2/image_bgr8.cdr"] {
            assert_pattern(&raw_to_rgb(&parse_image(Protocol::Ros2, &fixture(file)).unwrap()).unwrap(), 0.0, file);
        }
        for file in ["dimos/image_rgb8.bin", "dimos/image_bgr8.bin"] {
            assert_pattern(&raw_to_rgb(&parse_image(Protocol::Dimos, &fixture(file)).unwrap()).unwrap(), 0.0, file);
        }
        assert_pattern(&raw_to_rgb(&parse_image(Protocol::Dimos, &fixture("dimos/image_jpeg_in_Image.bin")).unwrap()).unwrap(), 8.0, "jpeg in Image");
        let mono = raw_to_rgb(&parse_image(Protocol::Ros2, &fixture("ros2/image_mono16.cdr")).unwrap()).unwrap();
        assert_eq!(quadrant_means(&mono).map(|m| m[0]), [0.0, 85.0, 170.0, 255.0]);
    }

    #[test]
    fn compressed_formats() {
        for (format, tolerance) in [("jpeg", 8.0), ("png", 0.0), ("webp", 0.0), ("jxl", 8.0)] {
            for (protocol, file) in [(Protocol::Ros2, format!("ros2/compressed_{format}.cdr")), (Protocol::Dimos, format!("dimos/compressed_{format}.bin"))] {
                let payload = fixture(&file);
                let message = parse_compressed_image(protocol, &payload).unwrap();
                assert_pattern(&compressed_to_rgb(message.data, &message.format).unwrap(), tolerance, &file);
            }
        }
    }

    #[test]
    fn depth_is_exact() {
        let payload = fixture("ros2/depth_16UC1.cdr");
        let image = parse_image(Protocol::Ros2, &payload).unwrap();
        let depth = raw_to_depth(&image).unwrap();
        let DepthValues::U16(values) = depth.values else { panic!("u16 expected") };
        assert!(values.iter().enumerate().all(|(i, &v)| v as usize == 1000 + i % 320 + 4 * (i / 320)));
        let payload = fixture("dimos/depth_32FC1.bin");
        let image = parse_image(Protocol::Dimos, &payload).unwrap();
        let DepthValues::F32(values) = raw_to_depth(&image).unwrap().values else { panic!("f32 expected") };
        assert!(values.iter().enumerate().all(|(i, &v)| v == 0.5 + (i % 320) as f32 / 128.0 + (i / 320) as f32 / 64.0));
        let message_bytes = fixture("ros2/compressed_jxl_depth16.cdr");
        let message = parse_compressed_image(Protocol::Ros2, &message_bytes).unwrap();
        let DepthValues::U16(values) = compressed_to_depth(message.data, &message.format).unwrap().values else { panic!() };
        assert!(values.iter().enumerate().all(|(i, &v)| v as usize == 1000 + i % 320 + 4 * (i / 320)), "jxl depth is lossless");
        assert!(raw_to_depth(&parse_image(Protocol::Ros2, &fixture("ros2/image_rgb8.cdr")).unwrap()).is_err());
    }
}
