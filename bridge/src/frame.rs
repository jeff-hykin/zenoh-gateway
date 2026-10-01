//! Bridge -> browser frame, little endian:
//! `u16 keyLen | key utf8 | f64 timestampMs | u32 seq | u32 frameId | u32 chunkIndex | u32 chunkCount | chunk bytes`.
//! `seq` numbers messages per channel; a message larger than `CHUNK_BYTES` is split across frames
//! that share its `seq`. `frameId` numbers frames per channel; the page acks frameIds.

use bytes::{BufMut, BytesMut};

pub const HEADER_FIXED_LEN: usize = 2 + 8 + 4 + 4 + 4 + 4;
/// Payload bytes per frame; well under the 256 KiB SCTP message limit, small enough to interleave.
pub const CHUNK_BYTES: usize = 64 * 1024;

pub struct FrameHeader<'a> {
    pub key: &'a str,
    pub timestamp_ms: f64,
    pub seq: u32,
    pub frame_id: u32,
    pub chunk_index: u32,
    pub chunk_count: u32,
}

pub fn chunk_count(payload_len: usize) -> u32 {
    payload_len.div_ceil(CHUNK_BYTES).max(1) as u32
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
    frame.put_slice(chunk);
    frame
}

/// The `index`-th chunk of `payload`.
pub fn chunk(payload: &[u8], index: u32) -> &[u8] {
    let start = index as usize * CHUNK_BYTES;
    &payload[start.min(payload.len())..(start + CHUNK_BYTES).min(payload.len())]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout() {
        let header = FrameHeader { key: "a/b", timestamp_ms: 1.5, seq: 7, frame_id: 9, chunk_index: 1, chunk_count: 3 };
        let frame = encode(&header, &[9, 9]);
        assert_eq!(&frame[0..2], &3u16.to_le_bytes());
        assert_eq!(&frame[2..5], b"a/b");
        assert_eq!(&frame[5..13], &1.5f64.to_le_bytes());
        assert_eq!(&frame[13..17], &7u32.to_le_bytes());
        assert_eq!(&frame[17..21], &9u32.to_le_bytes());
        assert_eq!(&frame[21..25], &1u32.to_le_bytes());
        assert_eq!(&frame[25..29], &3u32.to_le_bytes());
        assert_eq!(&frame[29..], &[9, 9]);
    }

    #[test]
    fn chunking_covers_payload_exactly() {
        let payload: Vec<u8> = (0..(CHUNK_BYTES * 2 + 5)).map(|i| i as u8).collect();
        assert_eq!(chunk_count(payload.len()), 3);
        assert_eq!(chunk_count(0), 1, "an empty payload is still one frame");
        let joined: Vec<u8> = (0..3).flat_map(|i| chunk(&payload, i).to_vec()).collect();
        assert_eq!(joined, payload);
        assert_eq!(chunk(&payload, 2).len(), 5);
    }
}
