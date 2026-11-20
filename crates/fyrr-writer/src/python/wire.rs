//! Frames of the provider-host protocol; the Python side is
//! `connectors/src/fyrr_connectors/host.py`, which documents the messages.
//!
//! A frame is a kind byte, a big-endian `u32` payload length and the payload.
//! The length is checked against the bound of its kind before anything is
//! allocated, so a corrupt or hostile length costs nothing.

use std::io::{self, Read, Write};

/// Bumped with the Python side when the frames or messages change.
pub(super) const PROTOCOL_VERSION: u64 = 1;

const HEADER_LEN: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    /// A JSON control message.
    Control,
    /// The JSON result of `inventory` or `discover`.
    Document,
    /// The contract header of a batch without rows.
    Header,
    /// The contract header of a batch whose rows follow in a `Rows` frame.
    Batch,
    /// Arrow IPC stream bytes.
    Rows,
}

impl Kind {
    fn byte(self) -> u8 {
        match self {
            Self::Control => b'J',
            Self::Document => b'R',
            Self::Header => b'H',
            Self::Batch => b'B',
            Self::Rows => b'A',
        }
    }

    fn parse(byte: u8) -> Option<Self> {
        [
            Self::Control,
            Self::Document,
            Self::Header,
            Self::Batch,
            Self::Rows,
        ]
        .into_iter()
        .find(|kind| kind.byte() == byte)
    }
}

/// Why a frame could not be read; none carries a peer-supplied value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WireError {
    /// The peer closed the socket, or it failed, before the frame was whole.
    Closed,
    /// An unknown kind byte.
    Malformed,
    /// A length above the bound of its kind.
    Oversize,
}

pub(super) fn write_frame(stream: &mut impl Write, kind: Kind, payload: &[u8]) -> io::Result<()> {
    let length = u32::try_from(payload.len()).map_err(|_| io::ErrorKind::InvalidInput)?;
    let mut frame = Vec::with_capacity(HEADER_LEN + payload.len());
    frame.push(kind.byte());
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(payload);
    stream.write_all(&frame)
}

/// Reads one frame whose payload is at most `bound(kind)` bytes.
pub(super) fn read_frame(
    stream: &mut impl Read,
    bound: impl Fn(Kind) -> u64,
) -> Result<(Kind, Vec<u8>), WireError> {
    let mut header = [0u8; HEADER_LEN];
    stream
        .read_exact(&mut header)
        .map_err(|_| WireError::Closed)?;
    let kind = Kind::parse(header[0]).ok_or(WireError::Malformed)?;
    let length = u64::from(u32::from_be_bytes([
        header[1], header[2], header[3], header[4],
    ]));
    if length > bound(kind) {
        return Err(WireError::Oversize);
    }
    // The vector grows with the bytes that arrive, so a length the peer
    // never delivers does not reserve memory either.
    let mut payload = Vec::new();
    let read = stream
        .take(length)
        .read_to_end(&mut payload)
        .map_err(|_| WireError::Closed)?;
    if read as u64 != length {
        return Err(WireError::Closed);
    }
    Ok((kind, payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_length_over_the_bound_is_refused_before_any_payload_is_read() {
        let mut frame = vec![b'A'];
        frame.extend_from_slice(&u32::MAX.to_be_bytes());
        frame.extend_from_slice(b"short");
        assert_eq!(
            read_frame(&mut frame.as_slice(), |kind| match kind {
                Kind::Rows => 1 << 20,
                _ => 0,
            }),
            Err(WireError::Oversize)
        );
        // Each kind has its own bound.
        let mut small = Vec::new();
        write_frame(&mut small, Kind::Control, &[b' '; 100]).unwrap();
        assert_eq!(
            read_frame(&mut small.as_slice(), |_| 99),
            Err(WireError::Oversize)
        );
        assert!(read_frame(&mut small.as_slice(), |_| 100).is_ok());
    }

    #[test]
    fn an_unknown_kind_and_a_truncated_payload_are_refused() {
        assert_eq!(
            read_frame(&mut &b"X\0\0\0\0"[..], |_| 10),
            Err(WireError::Malformed)
        );
        assert_eq!(
            read_frame(&mut &b"J\0\0\0\x05abc"[..], |_| 10),
            Err(WireError::Closed)
        );
        assert_eq!(
            read_frame(&mut &b"J\0\0"[..], |_| 10),
            Err(WireError::Closed)
        );
    }
}
