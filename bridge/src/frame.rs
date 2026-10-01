//! Bridge -> browser frame, little endian:
//! `u16 keyLen | key utf8 | f64 timestampMs | u32 seq | u32 frameId | u32 chunkIndex | u32 chunkCount | u8 flags | chunk bytes`.
//! `seq` numbers messages per channel; a message larger than one chunk is split across frames
//! that share its `seq` (chunk size is per message, at most `CHUNK_BYTES`). `frameId` numbers frames per channel; the page acks frameIds.
//! `flags` bit0 ([`ZSTD`]): the whole message (all its chunks joined) is zstd-compressed.

use bytes::{BufMut, BytesMut};

pub const HEADER_FIXED_LEN: usize = 2 + 8 + 4 + 4 + 4 + 4 + 1;
/// `flags` bit: the message is zstd-compressed.
pub const ZSTD: u8 = 1;
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

/// A frame's header and chunk (what [`encode`] wrote), None if it is malformed.
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
