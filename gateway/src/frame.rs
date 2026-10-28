//! Gateway -> browser frame, little endian:
//! `u16 keyLen | key utf8 | f64 timestampMs | u32 seq | u32 frameId | u32 chunkIndex | u32 chunkCount | u8 flags | chunk bytes`.
//! `seq` numbers messages per channel; a message larger than one chunk is split across frames
//! that share its `seq` (chunk size is per message, at most `CHUNK_BYTES`). `frameId` numbers frames per channel; the page acks frameIds.
//! `flags` bit0 ([`ZSTD`]): the whole message (all its chunks joined) is zstd-compressed; bit1 ([`FIELDS`]): it is a
//! zenoh-gateway fields message, which the client decodes into `msg.decoded`; bit2 ([`DELETE`]): the sample is a delete;
//! bit3 ([`META`]): the message (after decompression) starts with [`encode_meta`]'s header: the sample's encoding and attachment;
//! bit4 ([`HEADER`], 0.5.32+): the message (before decompression) starts with `u16 len | len bytes of`
//! [`encode_header`]: the sample's own message header (stamp, frame id, seq), which an encoding would otherwise drop.

use bytes::{BufMut, BytesMut};

pub const HEADER_FIXED_LEN: usize = 2 + 8 + 4 + 4 + 4 + 4 + 1;
/// `flags` bit: the message is zstd-compressed.
pub const ZSTD: u8 = 1;
/// `flags` bit: the message is zenoh-gateway fields.
pub const FIELDS: u8 = 2;
/// `flags` bit: the sample is a delete (its kind), not a put.
pub const DELETE: u8 = 4;
/// `flags` bit: the message starts with the sample's encoding and attachment (see [`encode_meta`]).
pub const META: u8 = 8;
/// `flags` bit: the message starts with its sample's message header (see [`encode_header`]).
pub const HEADER: u8 = 16;
/// Payload bytes per frame; well under the 256 KiB SCTP message limit, small enough to interleave.
pub const CHUNK_BYTES: usize = 64 * 1024;

pub struct FrameHeader<'a> {
    pub key: &'a str,
    pub timestamp_ms: f64,
    pub seq: u32,
    pub frame_id: u32,
    pub chunk_index: u32,
    pub chunk_count: u32,
    pub flags: u8,
}

pub fn chunk_count(payload_len: usize, chunk_bytes: usize) -> u32 {
    payload_len.div_ceil(chunk_bytes).max(1) as u32
}

pub fn encode(header: &FrameHeader, chunk: &[u8]) -> BytesMut {
    let key_bytes = &header.key.as_bytes()[..header.key.len().min(u16::MAX as usize)];
    let mut frame = BytesMut::with_capacity(HEADER_FIXED_LEN + key_bytes.len() + chunk.len());
    frame.put_u16_le(key_bytes.len() as u16);
    frame.put_slice(key_bytes);
    frame.put_f64_le(header.timestamp_ms);
    frame.put_u32_le(header.seq);
    frame.put_u32_le(header.frame_id);
    frame.put_u32_le(header.chunk_index);
    frame.put_u32_le(header.chunk_count);
    frame.put_u8(header.flags);
    frame.put_slice(chunk);
    frame
}

/// An encoded message's zenoh sample encoding and attachment, when they say something (as a raw message's [`META`]).
pub type SampleMeta = Option<(String, Option<Vec<u8>>)>;

/// A message header as sent, on the data channel (behind [`HEADER`]) and after a video frame's 64 metadata bytes:
/// `f64 stamp (s) | u16 frameIdLen | frameId utf8 | i32 sec | u32 nanosec | u8 flags | u32 seq`, flags bit0: `seq` is
/// set; bit1: the sample's zenoh encoding and attachment follow (`u16 encodingLen | encoding utf8 | u32 attachmentLen |
/// attachment`, e.g. rmw_zenoh's seq, timestamp and gid). (0.5.31 sent only up to `frameId`; readers take what is
/// there.) None for a frame id or encoding past 64 KiB: nothing real is.
pub fn encode_header(header: &crate::MessageHeader, sample: &SampleMeta) -> Option<Vec<u8>> {
    let frame_id = header.frame_id.as_bytes();
    let frame_id_len = u16::try_from(frame_id.len()).ok()?;
    let mut out = Vec::with_capacity(8 + 2 + frame_id.len() + 13);
    out.extend_from_slice(&header.stamp().to_le_bytes());
    out.extend_from_slice(&frame_id_len.to_le_bytes());
    out.extend_from_slice(frame_id);
    out.extend_from_slice(&header.sec.to_le_bytes());
    out.extend_from_slice(&header.nanosec.to_le_bytes());
    out.push(header.seq.is_some() as u8 | if sample.is_some() { 2 } else { 0 });
    out.extend_from_slice(&header.seq.unwrap_or(0).to_le_bytes());
    if let Some((encoding, attachment)) = sample {
        out.extend_from_slice(&u16::try_from(encoding.len()).ok()?.to_le_bytes());
        out.extend_from_slice(encoding.as_bytes());
        let attachment = attachment.as_deref().unwrap_or_default();
        out.extend_from_slice(&u32::try_from(attachment.len()).ok()?.to_le_bytes());
        out.extend_from_slice(attachment);
    }
    Some(out)
}

/// `message` behind its [`HEADER`] block (`u16 len | encode_header`).
pub fn with_header(header: &[u8], message: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + header.len() + message.len());
    out.extend_from_slice(&(header.len() as u16).to_le_bytes());
    out.extend_from_slice(header);
    out.extend_from_slice(message);
    out
}

/// Reads [`encode_header`]'s bytes (or 0.5.31's shorter form, which has no exact stamp or seq): the header and the
/// sample's encoding and attachment, if they came.
#[cfg(any(test, feature = "client"))]
pub fn decode_header(bytes: &[u8]) -> Option<(crate::MessageHeader, SampleMeta)> {
    let stamp = f64::from_le_bytes(bytes.get(..8)?.try_into().ok()?);
    let frame_id_len = u16::from_le_bytes(bytes.get(8..10)?.try_into().ok()?) as usize;
    let frame_id = std::str::from_utf8(bytes.get(10..10 + frame_id_len)?).ok()?.to_owned();
    let at = 10 + frame_id_len;
    let Some(exact) = bytes.get(at..at + 13) else {
        return Some((crate::MessageHeader::from_stamp(stamp, frame_id), None));
    };
    let word = |at: usize| u32::from_le_bytes(exact[at..at + 4].try_into().unwrap());
    let header = crate::MessageHeader { sec: word(0) as i32, nanosec: word(4), frame_id, seq: (exact[8] & 1 != 0).then(|| word(9)) };
    if exact[8] & 2 == 0 {
        return Some((header, None));
    }
    let rest = &bytes[at + 13..];
    let encoding_len = u16::from_le_bytes(rest.get(..2)?.try_into().ok()?) as usize;
    let encoding = std::str::from_utf8(rest.get(2..2 + encoding_len)?).ok()?.to_owned();
    let attachment_len = u32::from_le_bytes(rest.get(2 + encoding_len..6 + encoding_len)?.try_into().ok()?) as usize;
    let attachment = rest.get(6 + encoding_len..6 + encoding_len + attachment_len)?;
    Some((header, Some((encoding, (!attachment.is_empty()).then(|| attachment.to_vec())))))
}

/// Splits a [`HEADER`] message into (its header, the sample's encoding and attachment, the rest).
#[cfg(any(test, feature = "client"))]
pub fn split_header(message: &[u8]) -> Option<(crate::MessageHeader, SampleMeta, &[u8])> {
    let len = u16::from_le_bytes(message.get(..2)?.try_into().ok()?) as usize;
    let (header, sample) = decode_header(message.get(2..2 + len)?)?;
    Some((header, sample, &message[2 + len..]))
}

/// A frame's header and chunk (what [`encode`] wrote), None if it is malformed.
/// `u16 encodingLen | encoding utf8 | u32 attachmentLen | attachment bytes`, ahead of the payload in a [`META`] message.
pub fn encode_meta(encoding: &str, attachment: Option<&[u8]>, payload: &[u8]) -> Vec<u8> {
    let encoding = &encoding.as_bytes()[..encoding.len().min(u16::MAX as usize)];
    let attachment = attachment.unwrap_or_default();
    let mut out = Vec::with_capacity(2 + encoding.len() + 4 + attachment.len() + payload.len());
    out.extend_from_slice(&(encoding.len() as u16).to_le_bytes());
    out.extend_from_slice(encoding);
    out.extend_from_slice(&(attachment.len() as u32).to_le_bytes());
    out.extend_from_slice(attachment);
    out.extend_from_slice(payload);
    out
}

/// Splits a [`META`] message into (encoding, attachment, payload).
#[cfg(any(test, feature = "client"))]
pub fn decode_meta(message: &[u8]) -> Option<(&str, &[u8], &[u8])> {
    let encoding_len = u16::from_le_bytes(message.get(..2)?.try_into().ok()?) as usize;
    let encoding = std::str::from_utf8(message.get(2..2 + encoding_len)?).ok()?;
    let at = 2 + encoding_len;
    let attachment_len = u32::from_le_bytes(message.get(at..at + 4)?.try_into().ok()?) as usize;
    let attachment = message.get(at + 4..at + 4 + attachment_len)?;
    Some((encoding, attachment, &message[at + 4 + attachment_len..]))
}

#[cfg(any(test, feature = "client"))]
pub fn decode(frame: &[u8]) -> Option<(FrameHeader<'_>, &[u8])> {
    let key_len = u16::from_le_bytes(frame.get(..2)?.try_into().ok()?) as usize;
    let key = std::str::from_utf8(frame.get(2..2 + key_len)?).ok()?;
    let fixed = frame.get(2 + key_len..HEADER_FIXED_LEN + key_len)?;
    let word = |at: usize| u32::from_le_bytes(fixed[at..at + 4].try_into().unwrap());
    let header = FrameHeader { key, timestamp_ms: f64::from_le_bytes(fixed[..8].try_into().unwrap()), seq: word(8), frame_id: word(12), chunk_index: word(16), chunk_count: word(20), flags: fixed[24] };
    Some((header, &frame[HEADER_FIXED_LEN + key_len..]))
}

/// The `index`-th chunk of `payload`.
pub fn chunk(payload: &[u8], index: u32, chunk_bytes: usize) -> &[u8] {
    let start = index as usize * chunk_bytes;
    &payload[start.min(payload.len())..(start + chunk_bytes).min(payload.len())]
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_message_header_round_trips_exactly_and_reads_0_5_31_s_shorter_form() {
        let header = crate::MessageHeader { sec: 1_761_350_400, nanosec: 123_456_789, frame_id: "mid360_link".into(), seq: Some(7) };
        let bytes = super::encode_header(&header, &None).unwrap();
        assert_eq!(super::decode_header(&bytes), Some((header.clone(), None)), "nanoseconds an f64 stamp would lose survive");
        let message = super::with_header(&bytes, b"payload");
        assert_eq!(super::split_header(&message), Some((header.clone(), None, &b"payload"[..])));
        let sample = Some(("application/cdr".to_owned(), Some(vec![1, 2, 3])));
        let with_sample = super::encode_header(&header, &sample).unwrap();
        assert_eq!(super::decode_header(&with_sample), Some((header.clone(), sample)), "the zenoh sample's encoding and attachment");
        assert_eq!(super::decode_header(&with_sample[..bytes.len()]), None, "cut short");
        let short = &bytes[..10 + header.frame_id.len()];
        let (old, _) = super::decode_header(short).unwrap();
        assert_eq!((old.sec, old.frame_id.as_str(), old.seq), (header.sec, "mid360_link", None), "0.5.31: stamp and frame id only");
        assert!((old.nanosec as i64 - 123_456_789).abs() < 1_000, "rounded from the f64 stamp");
        assert_eq!(super::encode_header(&crate::MessageHeader { frame_id: "x".repeat(70_000), ..header }, &None), None, "no frame id is that long");
        assert_eq!(super::decode_header(&bytes[..9]), None, "cut short");
    }

    use super::*;

    #[test]
    fn layout() {
        let header = FrameHeader { key: "a/b", timestamp_ms: 1.5, seq: 7, frame_id: 9, chunk_index: 1, chunk_count: 3, flags: ZSTD };
        let frame = encode(&header, &[9, 9]);
        assert_eq!(&frame[0..2], &3u16.to_le_bytes());
        assert_eq!(&frame[2..5], b"a/b");
        assert_eq!(&frame[5..13], &1.5f64.to_le_bytes());
        assert_eq!(&frame[13..17], &7u32.to_le_bytes());
        assert_eq!(&frame[17..21], &9u32.to_le_bytes());
        assert_eq!(&frame[21..25], &1u32.to_le_bytes());
        assert_eq!(&frame[25..29], &3u32.to_le_bytes());
        assert_eq!(frame[29], ZSTD);
        assert_eq!(&frame[30..], &[9, 9]);
        let (decoded, chunk) = decode(&frame).unwrap();
        assert_eq!((decoded.key, decoded.timestamp_ms, decoded.seq, decoded.frame_id, decoded.chunk_index, decoded.chunk_count, decoded.flags, chunk), ("a/b", 1.5, 7, 9, 1, 3, ZSTD, &[9u8, 9][..]));
        assert!(decode(&frame[..20]).is_none());
    }

    #[test]
    fn chunking_covers_payload_exactly() {
        let payload: Vec<u8> = (0..(CHUNK_BYTES * 2 + 5)).map(|i| i as u8).collect();
        assert_eq!(chunk_count(payload.len(), CHUNK_BYTES), 3);
        assert_eq!(chunk_count(0, CHUNK_BYTES), 1, "an empty payload is still one frame");
        let joined: Vec<u8> = (0..3).flat_map(|i| chunk(&payload, i, CHUNK_BYTES).to_vec()).collect();
        assert_eq!(joined, payload);
        assert_eq!(chunk(&payload, 2, CHUNK_BYTES).len(), 5);
        let small: Vec<u8> = (0..chunk_count(payload.len(), 1000)).flat_map(|i| chunk(&payload, i, 1000).to_vec()).collect();
        assert_eq!(small, payload);
    }
}
