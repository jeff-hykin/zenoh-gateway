//! PointCloud2 to a compact data-channel format: optional voxel downsample, xyz quantized to int16
//! around a per-message origin, optional u8 intensity, zstd.
//!
//! Wire format (little endian), see SPEC "Wire formats":
//! `u8 version=1 | u8 flags (bit0 intensity) | u16 reserved | u32 pointCount | u32 sourcePointCount |
//!  f32 originX | f32 originY | f32 originZ | f32 scale | f32 voxelSize | f32 intensityMin |
//!  f32 intensityScale | zstd(i16 x,y,z per point, then u8 intensity per point if flagged)`
//!
//! Decoded `x = originX + qx * scale`. Without voxels (quality 1) the error per axis is at most
//! `scale / 2`, where `scale = (largest bounding-box extent / 2) / 32767` (plus f32 rounding,
//! ~1e-7 relative). With voxels, each occupied voxel becomes one point at the voxel's center (its
//! intensity the mean), on a quantization grid aligned to the voxels, so the error per axis
//! against any original point in that voxel is at most `voxelSize / 2`.

use crate::codec::wire::{PointCloud, PointField};
use anyhow::{Context, Result, bail, ensure};
use std::borrow::Cow;
use std::collections::HashMap;

pub const HEADER_LEN: usize = 40;
const ZSTD_LEVEL: i32 = 3;
/// Voxel edge at quality 0, meters; quality q uses `MAX_VOXEL * (1 - q)`, none at quality 1.
pub const MAX_VOXEL: f64 = 0.2;
const QUANT_MAX: f64 = 32767.0;

pub fn voxel_size(quality: f64) -> f64 {
    let size = MAX_VOXEL * (1.0 - quality.clamp(0.0, 1.0));
    if size < 1e-3 { 0.0 } else { size }
}

/// Reads one numeric field of a point as f64, for any PointField datatype.
#[derive(Clone, Copy)]
struct FieldReader {
    offset: usize,
    datatype: u8,
    big_endian: bool,
}

impl FieldReader {
    fn new(field: &PointField, point_step: u32, big_endian: bool) -> Result<Self> {
        let size = match field.datatype {
            1 | 2 => 1,
            3 | 4 => 2,
            5..=7 => 4,
            8 => 8,
            other => bail!("PointField {:?} has unknown datatype {other}", field.name),
        };
        ensure!(field.offset as usize + size <= point_step as usize, "PointField {:?} (offset {}) outside point_step {point_step}", field.name, field.offset);
        Ok(FieldReader { offset: field.offset as usize, datatype: field.datatype, big_endian })
    }

    fn read(&self, point: &[u8]) -> f64 {
        let bytes = &point[self.offset..];
        macro_rules! num {
            ($ty:ty, $n:expr) => {{
                let array: [u8; $n] = bytes[..$n].try_into().unwrap();
                (if self.big_endian { <$ty>::from_be_bytes(array) } else { <$ty>::from_le_bytes(array) }) as f64
            }};
        }
        match self.datatype {
            1 => bytes[0] as i8 as f64,
            2 => bytes[0] as f64,
            3 => num!(i16, 2),
            4 => num!(u16, 2),
            5 => num!(i32, 4),
            6 => num!(u32, 4),
            7 => num!(f32, 4),
            _ => num!(f64, 8),
        }
    }
}

#[derive(Clone)]
struct Point {
    xyz: [f64; 3],
    intensity: f64,
}

/// A cloud's finite points, owned (the decoded frame shared across frontends).
pub struct Points {
    points: Vec<Point>,
    has_intensity: bool,
}

/// Every finite point of the cloud, honoring offsets, point_step, row_step and endianness.
pub fn read_points(cloud: &PointCloud) -> Result<Points> {
    let field = |name: &str| cloud.fields.iter().find(|field| field.name == name);
    let reader = |name: &str| -> Result<FieldReader> {
        FieldReader::new(field(name).with_context(|| format!("PointCloud2 has no {name:?} field"))?, cloud.point_step, cloud.big_endian)
    };
    let (x, y, z) = (reader("x")?, reader("y")?, reader("z")?);
    let intensity = field("intensity").map(|field| FieldReader::new(field, cloud.point_step, cloud.big_endian)).transpose()?;
    let point_step = cloud.point_step as usize;
    ensure!(point_step > 0, "point_step is 0");
    let row_step = if cloud.row_step == 0 { point_step * cloud.width as usize } else { cloud.row_step as usize };
    ensure!(row_step >= point_step * cloud.width as usize, "row_step {row_step} < width x point_step");
    let mut points = Vec::with_capacity(cloud.width as usize * cloud.height as usize);
    for row in 0..cloud.height as usize {
        for column in 0..cloud.width as usize {
            let start = row * row_step + column * point_step;
            let point = cloud.data.get(start..start + point_step).with_context(|| format!("PointCloud2 data too short at row {row} column {column}"))?;
            let xyz = [x.read(point), y.read(point), z.read(point)];
            if xyz.iter().all(|value| value.is_finite()) {
                points.push(Point { xyz, intensity: intensity.map_or(0.0, |reader| reader.read(point)) });
            }
        }
    }
    Ok(Points { points, has_intensity: intensity.is_some() })
}

/// Replaces the points in each occupied voxel with one point at the voxel's center (mean intensity).
fn voxel_downsample(points: &[Point], voxel: f64) -> Cow<'_, [Point]> {
    if voxel <= 0.0 {
        return Cow::Borrowed(points);
    }
    let mut cells: HashMap<[i64; 3], (f64, u32)> = HashMap::with_capacity(points.len() / 2);
    let mut order = Vec::new();
    for point in points {
        let cell = point.xyz.map(|value| (value / voxel).floor() as i64);
        let entry = cells.entry(cell).or_insert_with(|| {
            order.push(cell);
            (0.0, 0)
        });
        entry.0 += point.intensity;
        entry.1 += 1;
    }
    order
        .into_iter()
        .map(|cell| {
            let (intensity_sum, count) = cells[&cell];
            Point { xyz: cell.map(|index| (index as f64 + 0.5) * voxel), intensity: intensity_sum / count as f64 }
        })
        .collect::<Vec<_>>()
        .into()
}

/// (origin, scale) so every point fits int16. With voxels the grid is voxel-aligned: centers land
/// on exact integers, which keeps the quantized values regular (and zstd effective).
fn quantization_grid(low: [f64; 3], high: [f64; 3], voxel: f64) -> ([f64; 3], f64) {
    let half_extent = (0..3).map(|axis| (high[axis] - low[axis]) / 2.0).fold(0.0, f64::max);
    let finest = half_extent / QUANT_MAX;
    let center: [f64; 3] = std::array::from_fn(|axis| (low[axis] + high[axis]) / 2.0);
    if voxel > 0.0 {
        // centers sit at odd multiples of voxel/2, so a finer step would carry no information
        if voxel / 2.0 >= finest {
            return (center.map(|value| (value / voxel).round() * voxel), voxel / 2.0);
        }
    }
    (center, finest.max(f32::MIN_POSITIVE as f64))
}

#[cfg(test)]
pub fn encode(cloud: &PointCloud, quality: f64) -> Result<Vec<u8>> {
    encode_points(&read_points(cloud)?, quality)
}

pub fn encode_points(cloud: &Points, quality: f64) -> Result<Vec<u8>> {
    let has_intensity = cloud.has_intensity;
    let source_count = cloud.points.len();
    let voxel = voxel_size(quality);
    let points = voxel_downsample(&cloud.points, voxel);
    let mut low = [f64::INFINITY; 3];
    let mut high = [f64::NEG_INFINITY; 3];
    let (mut intensity_low, mut intensity_high) = (f64::INFINITY, f64::NEG_INFINITY);
    for point in points.iter() {
        for axis in 0..3 {
            low[axis] = low[axis].min(point.xyz[axis]);
            high[axis] = high[axis].max(point.xyz[axis]);
        }
        intensity_low = intensity_low.min(point.intensity);
        intensity_high = intensity_high.max(point.intensity);
    }
    let (origin, scale) = if points.is_empty() { ([0.0; 3], 1.0) } else { quantization_grid(low, high, voxel) };
    let origin = origin.map(|value| value as f32);
    let scale = scale as f32;
    let intensity_scale = if intensity_high > intensity_low { ((intensity_high - intensity_low) / 255.0) as f32 } else { 1.0 };
    let intensity_min = if points.is_empty() { 0.0 } else { intensity_low as f32 };

    let mut body = Vec::with_capacity(points.len() * if has_intensity { 7 } else { 6 });
    for point in points.iter() {
        for (value, axis_origin) in point.xyz.iter().zip(origin) {
            let quantized = ((value - axis_origin as f64) / scale as f64).round().clamp(-QUANT_MAX, QUANT_MAX) as i16;
            body.extend_from_slice(&quantized.to_le_bytes());
        }
    }
    if has_intensity {
        body.extend(points.iter().map(|point| ((point.intensity - intensity_min as f64) / intensity_scale as f64).round().clamp(0.0, 255.0) as u8));
    }
    let mut out = Vec::with_capacity(HEADER_LEN + body.len() / 2);
    out.push(1);
    out.push(has_intensity as u8);
    out.extend_from_slice(&[0, 0]);
    out.extend_from_slice(&(points.len() as u32).to_le_bytes());
    out.extend_from_slice(&(source_count as u32).to_le_bytes());
    for value in [origin[0], origin[1], origin[2], scale, voxel as f32, intensity_min, intensity_scale] {
        out.extend_from_slice(&value.to_le_bytes());
    }
    out.extend_from_slice(&zstd::bulk::compress(&body, ZSTD_LEVEL)?);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::wire::{Protocol, parse_point_cloud, tests::fixture};

    fn decode(bytes: &[u8]) -> (Vec<[f32; 3]>, Option<Vec<u8>>, f32) {
        let f32_at = |offset: usize| f32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        let count = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
        let (origin, scale) = ([f32_at(12), f32_at(16), f32_at(20)], f32_at(24));
        let body = zstd::bulk::decompress(&bytes[HEADER_LEN..], 1 << 24).unwrap();
        let positions = (0..count)
            .map(|index| std::array::from_fn(|axis| origin[axis] + i16::from_le_bytes([body[index * 6 + axis * 2], body[index * 6 + axis * 2 + 1]]) as f32 * scale))
            .collect();
        let intensity = (bytes[1] & 1 == 1).then(|| body[count * 6..].to_vec());
        (positions, intensity, scale)
    }

    #[test]
    fn full_quality_within_bound() {
        for (protocol, file, intensity) in [(Protocol::Ros2, "ros2/pointcloud_xyz.cdr", false), (Protocol::Dimos, "dimos/pointcloud_xyzi.bin", true)] {
            let payload = fixture(file);
            let encoded = encode(&parse_point_cloud(protocol, &payload).unwrap(), 1.0).unwrap();
            let (positions, intensities, scale) = decode(&encoded);
            assert_eq!(positions.len(), 20000, "{file}");
            let bound = scale / 2.0 + 1e-5;
            for (index, position) in positions.iter().enumerate() {
                let expected = [(index % 200) as f32 * 0.05, (index / 200) as f32 * 0.05, (index % 7) as f32 * 0.125];
                for axis in 0..3 {
                    assert!((position[axis] - expected[axis]).abs() <= bound, "{file} point {index} axis {axis}: {position:?} vs {expected:?}");
                }
            }
            assert_eq!(intensities.is_some(), intensity);
            if let Some(intensities) = intensities {
                assert!(intensities.iter().enumerate().all(|(index, &value)| value as usize == index % 256));
            }
        }
    }

    #[test]
    fn lower_quality_is_smaller() {
        let payload = fixture("ros2/pointcloud_xyzi.cdr");
        let cloud = parse_point_cloud(Protocol::Ros2, &payload).unwrap();
        let full = encode(&cloud, 1.0).unwrap();
        let half = encode(&cloud, 0.5).unwrap();
        let lowest = encode(&cloud, 0.0).unwrap();
        assert!(half.len() * 4 < full.len() * 3 && lowest.len() < half.len(), "{} / {} / {}", full.len(), half.len(), lowest.len());
        assert!(u32::from_le_bytes(half[4..8].try_into().unwrap()) < 20000);
    }

    #[test]
    fn skips_nan_and_honors_layout() {
        // big endian, padded point_step 20, row_step with 4 bytes of padding, one NaN point
        let fields = vec![
            PointField { name: "z".into(), offset: 8, datatype: 7, count: 1 },
            PointField { name: "x".into(), offset: 0, datatype: 8, count: 1 },
            PointField { name: "y".into(), offset: 12, datatype: 3, count: 1 },
        ];
        let mut data = Vec::new();
        for (x, y, z) in [(1.0f64, 2i16, 3.0f32), (f64::NAN, 0, 0.0), (-1.0, -2, -3.0)] {
            data.extend_from_slice(&x.to_be_bytes());
            data.extend_from_slice(&z.to_be_bytes());
            data.extend_from_slice(&y.to_be_bytes());
            data.extend_from_slice(&[0; 6]);
        }
        data.extend_from_slice(&[0; 4]);
        let cloud = PointCloud { height: 1, width: 3, fields, big_endian: true, point_step: 20, row_step: 64, data: &data };
        let (positions, intensity, scale) = decode(&encode(&cloud, 1.0).unwrap());
        assert!(intensity.is_none());
        assert_eq!(positions.len(), 2);
        for (position, expected) in positions.iter().zip([[1.0, 2.0, 3.0], [-1.0, -2.0, -3.0]]) {
            for axis in 0..3 {
                assert!((position[axis] - expected[axis]).abs() <= scale, "{position:?}");
            }
        }
    }
}
