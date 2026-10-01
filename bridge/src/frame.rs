//! Bridge -> browser frame: `u16 keyLen | key utf8 | f64 timestampMs | u32 seq | payload`, little endian.

use bytes::{BufMut, BytesMut};

pub const HEADER_FIXED_LEN: usize = 2 + 8 + 4;

pub fn encode(key: &str, timestamp_ms: f64, seq: u32, payload: &[u8]) -> BytesMut {
    let key_bytes = &key.as_bytes()[..key.len().min(u16::MAX as usize)];
    let mut frame = BytesMut::with_capacity(HEADER_FIXED_LEN + key_bytes.len() + payload.len());
    frame.put_u16_le(key_bytes.len() as u16);
    frame.put_slice(key_bytes);
    frame.put_f64_le(timestamp_ms);
    frame.put_u32_le(seq);
    frame.put_slice(payload);
    frame
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout() {
        let frame = encode("a/b", 1.5, 7, &[9, 9]);
        assert_eq!(&frame[0..2], &3u16.to_le_bytes());
        assert_eq!(&frame[2..5], b"a/b");
        assert_eq!(&frame[5..13], &1.5f64.to_le_bytes());
        assert_eq!(&frame[13..17], &7u32.to_le_bytes());
        assert_eq!(&frame[17..], &[9, 9]);
    }
}
