//! Message parsers for the two input protocols, both zero-copy over the sample payload:
//! - ROS 2 over rmw_zenoh: CDR (XCDR1) with a 4-byte encapsulation header; little or big endian.
//! - dimos over zenoh: LCM (big endian, no alignment) with an 8-byte type fingerprint up front.
//!
//! Only the fields a codec needs are kept; headers (stamp, frame_id) are skipped.

use anyhow::{Context, Result, bail, ensure};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Protocol {
    Ros2,
    Dimos,
}

/// `sensor_msgs/Image`
#[derive(Debug)]
pub struct RawImage<'a> {
    pub width: u32,
    pub height: u32,
    pub encoding: String,
    pub big_endian: bool,
    pub step: u32,
    pub data: &'a [u8],
}

/// `sensor_msgs/CompressedImage`
#[derive(Debug)]
pub struct CompressedImage<'a> {
    pub format: String,
    pub data: &'a [u8],
}

#[derive(Debug, Clone, PartialEq)]
pub struct PointField {
    pub name: String,
    pub offset: u32,
    /// sensor_msgs/PointField datatype: 1 INT8 .. 8 FLOAT64
    pub datatype: u8,
    pub count: u32,
}

/// `sensor_msgs/PointCloud2`
#[derive(Debug)]
pub struct PointCloud<'a> {
    pub height: u32,
    pub width: u32,
    pub fields: Vec<PointField>,
    pub big_endian: bool,
    pub point_step: u32,
    pub row_step: u32,
    pub data: &'a [u8],
}

/// LCM fingerprints of the dimos (dimos-lcm) types, as `lcm-gen` computes them.
const LCM_IMAGE: [u8; 8] = [0x53, 0x5c, 0xfa, 0xce, 0x1f, 0x4f, 0x57, 0x17];
const LCM_COMPRESSED_IMAGE: [u8; 8] = [0xb8, 0xd0, 0x11, 0xc1, 0x04, 0x12, 0xb9, 0xa1];
const LCM_POINT_CLOUD2: [u8; 8] = [0xf5, 0xeb, 0x3d, 0xa1, 0xc2, 0x85, 0x31, 0x75];

/// Reads CDR: primitives aligned to their size relative to the end of the encapsulation header.
struct Cdr<'a> {
    body: &'a [u8],
    position: usize,
    little_endian: bool,
}

impl<'a> Cdr<'a> {
    fn new(payload: &'a [u8]) -> Result<Self> {
        ensure!(payload.len() >= 4, "CDR payload shorter than its encapsulation header");
        // representation identifier: 0x0000 CDR_BE, 0x0001 CDR_LE (XCDR2 forms 0x0006..0x000b too)
        let little_endian = match payload[1] {
            0x00 | 0x02 | 0x06 | 0x08 | 0x0a => false,
            0x01 | 0x03 | 0x07 | 0x09 | 0x0b => true,
            other => bail!("unknown CDR representation 0x{:02x}{other:02x}", payload[0]),
        };
        Ok(Cdr { body: &payload[4..], position: 0, little_endian })
    }

    fn take(&mut self, length: usize, alignment: usize) -> Result<&'a [u8]> {
        self.position = self.position.next_multiple_of(alignment);
        let end = self.position.checked_add(length).context("CDR length overflow")?;
        ensure!(end <= self.body.len(), "CDR message truncated (need {end} bytes, have {})", self.body.len());
        let bytes = &self.body[self.position..end];
        self.position = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1, 1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        let bytes: [u8; 4] = self.take(4, 4)?.try_into()?;
        Ok(if self.little_endian { u32::from_le_bytes(bytes) } else { u32::from_be_bytes(bytes) })
    }

    fn string(&mut self) -> Result<String> {
        let length = self.u32()? as usize;
        let bytes = self.take(length, 1)?;
        Ok(String::from_utf8_lossy(bytes.strip_suffix(&[0]).unwrap_or(bytes)).into_owned())
    }

    fn byte_sequence(&mut self) -> Result<&'a [u8]> {
        let length = self.u32()? as usize;
        self.take(length, 1)
    }

    /// std_msgs/Header: builtin_interfaces/Time stamp (i32 sec, u32 nanosec) + string frame_id
    fn skip_header(&mut self) -> Result<()> {
        self.u32()?;
        self.u32()?;
        self.string()?;
        Ok(())
    }
}

/// Reads LCM: big endian, packed, strings as `i32 length (incl. NUL) | bytes | NUL`.
struct Lcm<'a> {
    body: &'a [u8],
    position: usize,
}

impl<'a> Lcm<'a> {
    fn new(payload: &'a [u8], fingerprint: [u8; 8], type_name: &str) -> Result<Self> {
        ensure!(payload.len() >= 8, "LCM payload shorter than its fingerprint");
        ensure!(payload[..8] == fingerprint, "not an LCM {type_name} (fingerprint {:02x?})", &payload[..8]);
        Ok(Lcm { body: payload, position: 8 })
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self.position.checked_add(length).context("LCM length overflow")?;
        ensure!(end <= self.body.len(), "LCM message truncated (need {end} bytes, have {})", self.body.len());
        let bytes = &self.body[self.position..end];
        self.position = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into()?))
    }

    fn length(&mut self) -> Result<usize> {
        let value = self.i32()?;
        usize::try_from(value).context("negative LCM length")
    }

    fn string(&mut self) -> Result<String> {
        let length = self.length()?;
        let bytes = self.take(length)?;
        Ok(String::from_utf8_lossy(bytes.strip_suffix(&[0]).unwrap_or(bytes)).into_owned())
    }

    /// std_msgs.Header: i32 seq, std_msgs.Time stamp (i32 sec, i32 nsec), string frame_id
    fn skip_header(&mut self) -> Result<()> {
        self.take(12)?;
        self.string()?;
        Ok(())
    }
}

pub fn parse_image(protocol: Protocol, payload: &[u8]) -> Result<RawImage<'_>> {
    match protocol {
        Protocol::Ros2 => {
            let mut cdr = Cdr::new(payload)?;
            cdr.skip_header()?;
            let height = cdr.u32()?;
            let width = cdr.u32()?;
            let encoding = cdr.string()?;
            let big_endian = cdr.u8()? != 0;
            let step = cdr.u32()?;
            let data = cdr.byte_sequence()?;
            Ok(RawImage { width, height, encoding, big_endian, step, data })
        }
        Protocol::Dimos => {
            let mut lcm = Lcm::new(payload, LCM_IMAGE, "sensor_msgs.Image")?;
            let data_length = lcm.length()?;
            lcm.skip_header()?;
            let height = lcm.i32()? as u32;
            let width = lcm.i32()? as u32;
            let encoding = lcm.string()?;
            let big_endian = lcm.u8()? != 0;
            let step = lcm.i32()? as u32;
            let data = lcm.take(data_length)?;
            Ok(RawImage { width, height, encoding, big_endian, step, data })
        }
    }
}

pub fn parse_compressed_image(protocol: Protocol, payload: &[u8]) -> Result<CompressedImage<'_>> {
    match protocol {
        Protocol::Ros2 => {
            let mut cdr = Cdr::new(payload)?;
            cdr.skip_header()?;
            let format = cdr.string()?;
            let data = cdr.byte_sequence()?;
            Ok(CompressedImage { format, data })
        }
        Protocol::Dimos => {
            let mut lcm = Lcm::new(payload, LCM_COMPRESSED_IMAGE, "sensor_msgs.CompressedImage")?;
            let data_length = lcm.length()?;
            lcm.skip_header()?;
            let format = lcm.string()?;
            let data = lcm.take(data_length)?;
            Ok(CompressedImage { format, data })
        }
    }
}

pub fn parse_point_cloud(protocol: Protocol, payload: &[u8]) -> Result<PointCloud<'_>> {
    match protocol {
        Protocol::Ros2 => {
            let mut cdr = Cdr::new(payload)?;
            cdr.skip_header()?;
            let height = cdr.u32()?;
            let width = cdr.u32()?;
            let field_count = cdr.u32()? as usize;
            ensure!(field_count <= 1024, "PointCloud2 with {field_count} fields");
            let mut fields = Vec::with_capacity(field_count);
            for _ in 0..field_count {
                let name = cdr.string()?;
                let offset = cdr.u32()?;
                let datatype = cdr.u8()?;
                let count = cdr.u32()?;
                fields.push(PointField { name, offset, datatype, count });
            }
            let big_endian = cdr.u8()? != 0;
            let point_step = cdr.u32()?;
            let row_step = cdr.u32()?;
            let data = cdr.byte_sequence()?;
            Ok(PointCloud { height, width, fields, big_endian, point_step, row_step, data })
        }
        Protocol::Dimos => {
            let mut lcm = Lcm::new(payload, LCM_POINT_CLOUD2, "sensor_msgs.PointCloud2")?;
            let field_count = lcm.length()?;
            ensure!(field_count <= 1024, "PointCloud2 with {field_count} fields");
            let data_length = lcm.length()?;
            lcm.skip_header()?;
            let height = lcm.i32()? as u32;
            let width = lcm.i32()? as u32;
            let mut fields = Vec::with_capacity(field_count);
            for _ in 0..field_count {
                let name = lcm.string()?;
                let offset = lcm.i32()? as u32;
                let datatype = lcm.u8()?;
                let count = lcm.i32()? as u32;
                fields.push(PointField { name, offset, datatype, count });
            }
            let big_endian = lcm.u8()? != 0;
            let point_step = lcm.i32()? as u32;
            let row_step = lcm.i32()? as u32;
            let data = lcm.take(data_length)?;
            Ok(PointCloud { height, width, fields, big_endian, point_step, row_step, data })
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::path::PathBuf;

    pub fn fixture(name: &str) -> Vec<u8> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../test/fixtures").join(name);
        std::fs::read(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
    }

    #[test]
    fn images_from_both_protocols() {
        for (protocol, file) in [(Protocol::Ros2, "ros2/image_rgb8.cdr"), (Protocol::Dimos, "dimos/image_rgb8.lcm")] {
            let payload = fixture(file);
            let image = parse_image(protocol, &payload).unwrap();
            assert_eq!((image.width, image.height, image.step), (320, 240, 960), "{file}");
            assert_eq!(image.encoding, "rgb8");
            assert!(!image.big_endian);
            assert_eq!(image.data.len(), 320 * 240 * 3);
            assert_eq!(&image.data[..3], &[255, 0, 0]);
        }
    }

    #[test]
    fn compressed_images_from_both_protocols() {
        for (protocol, file) in [(Protocol::Ros2, "ros2/compressed_png.cdr"), (Protocol::Dimos, "dimos/compressed_png.lcm")] {
            let payload = fixture(file);
            let image = parse_compressed_image(protocol, &payload).unwrap();
            assert!(image.format.contains("png"), "{file}: {}", image.format);
            assert_eq!(image.data, fixture("compressed/pattern.png").as_slice());
        }
    }

    #[test]
    fn point_clouds_from_both_protocols() {
        for (protocol, file, step) in [(Protocol::Ros2, "ros2/pointcloud_xyz.cdr", 12), (Protocol::Dimos, "dimos/pointcloud_xyzi.lcm", 16)] {
            let payload = fixture(file);
            let cloud = parse_point_cloud(protocol, &payload).unwrap();
            assert_eq!((cloud.width, cloud.height, cloud.point_step), (20000, 1, step), "{file}");
            assert_eq!(cloud.fields[0], PointField { name: "x".into(), offset: 0, datatype: 7, count: 1 });
            assert_eq!(cloud.data.len(), 20000 * step as usize);
        }
    }

    #[test]
    fn wrong_type_is_an_error() {
        assert!(parse_image(Protocol::Dimos, &fixture("dimos/compressed_png.lcm")).is_err(), "fingerprint mismatch");
        assert!(parse_image(Protocol::Ros2, &fixture("ros2/compressed_png.cdr")).is_err(), "truncated");
        assert!(parse_point_cloud(Protocol::Ros2, &[0, 1, 0, 0, 1]).is_err());
    }
}
