use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use std::os::unix::fs::MetadataExt;

use crate::error::Error;
use crate::error::Result;

pub const WAL_FRAME_HEADER_SIZE: u64 = 24;
pub const WAL_HEADER_SIZE: u64 = 32;

static WAL_HEADER_CHECKSUM_OFFSET: u64 = 24;
static WAL_FRAME_HEADER_CHECKSUM_OFFSET: u64 = 16;

pub const WAL_HEADER_BIG_ENDIAN_MAGIC: [u8; 4] = [0x37, 0x7f, 0x06, 0x83];
pub const WAL_HEADER_LITTLE_ENDIAN_MAGIC: [u8; 4] = [0x37, 0x7f, 0x06, 0x82];

// SQLite checkpoint modes.
static CHECKPOINT_MODE_PASSIVE: &str = "PASSIVE";
static CHECKPOINT_MODE_FULL: &str = "FULL";
static CHECKPOINT_MODE_RESTART: &str = "RESTART";
static CHECKPOINT_MODE_TRUNCATE: &str = "TRUNCATE";

#[derive(Clone)]
pub enum CheckpointMode {
    Passive,
    Full,
    Restart,
    Truncate,
}

impl CheckpointMode {
    pub fn as_str(&self) -> &str {
        match self {
            CheckpointMode::Passive => CHECKPOINT_MODE_PASSIVE,
            CheckpointMode::Full => CHECKPOINT_MODE_FULL,
            CheckpointMode::Restart => CHECKPOINT_MODE_RESTART,
            CheckpointMode::Truncate => CHECKPOINT_MODE_TRUNCATE,
        }
    }
}

// implementation of sqlite check algorithm
pub fn checksum(data: &[u8], s1: u32, s2: u32, is_big_endian: bool) -> (u32, u32) {
    assert_eq!(data.len() % 8, 0, "data length must be a multiple of 8");
    let mut i = 0;
    let mut s1: u32 = s1;
    let mut s2: u32 = s2;
    while i < data.len() {
        let bytes1 = &data[i..i + 4];
        let bytes2 = &data[i + 4..i + 8];
        let (n1, n2) = if is_big_endian {
            (
                u32::from_be_bytes([bytes1[0], bytes1[1], bytes1[2], bytes1[3]]),
                u32::from_be_bytes([bytes2[0], bytes2[1], bytes2[2], bytes2[3]]),
            )
        } else {
            (
                u32::from_le_bytes([bytes1[0], bytes1[1], bytes1[2], bytes1[3]]),
                u32::from_le_bytes([bytes2[0], bytes2[1], bytes2[2], bytes2[3]]),
            )
        };

        // use `wrapping_add` instead of `+` directly, or else will be overflow panic
        s1 = s1.wrapping_add(n1).wrapping_add(s2);
        s2 = s2.wrapping_add(n2).wrapping_add(s1);

        i += 8;
    }

    (s1, s2)
}

pub fn read_last_checksum(file_name: &str, page_size: u64) -> Result<(u32, u32)> {
    let file = OpenOptions::new().read(true).open(file_name)?;
    let metadata = file.metadata()?;
    let fsize = metadata.size();
    let offset = if fsize > WAL_HEADER_SIZE {
        let sz = align_frame(page_size, fsize);
        sz - page_size - WAL_FRAME_HEADER_SIZE + WAL_FRAME_HEADER_CHECKSUM_OFFSET
    } else {
        WAL_HEADER_CHECKSUM_OFFSET
    };

    let mut buf = [0u8; 8];
    let n = file.read_at(&mut buf, offset)?;
    if n != buf.len() {
        return Err(Error::UnexpectedEofError(
            "UnexpectedEOFError when read last checksum".to_string(),
        ));
    }

    let checksum1 = from_be_bytes_at(&buf, 0)?;
    let checksum2 = from_be_bytes_at(&buf, 4)?;

    Ok((checksum1, checksum2))
}

// returns a frame-aligned offset.
// Returns zero if offset is less than the WAL header size.
pub fn align_frame(page_size: u64, offset: u64) -> u64 {
    if offset < WAL_HEADER_SIZE {
        return 0;
    }

    let page_size = page_size as i64;
    let offset = offset as i64;
    let frame_size = WAL_FRAME_HEADER_SIZE as i64 + page_size;
    let frame_num = (offset - WAL_HEADER_SIZE as i64) / frame_size;

    (frame_num * frame_size) as u64 + WAL_HEADER_SIZE
}

pub(crate) fn from_be_bytes_at(data: &[u8], offset: usize) -> Result<u32> {
    let end = offset.saturating_add(4);
    let p = data.get(offset..end).ok_or_else(|| {
        Error::UnexpectedEofError(format!(
            "cannot read 4 bytes at offset {offset}, buffer size {}",
            data.len()
        ))
    })?;
    Ok(u32::from_be_bytes([p[0], p[1], p[2], p[3]]))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== align_frame tests ==========

    #[test]
    fn test_align_frame_at_header_boundary() {
        // Exactly at header size should return header size
        assert_eq!(WAL_HEADER_SIZE, align_frame(4096, WAL_HEADER_SIZE));
    }

    #[test]
    fn test_align_frame_below_header() {
        // Below header size should return 0
        assert_eq!(0, align_frame(4096, 0));
        assert_eq!(0, align_frame(4096, 16));
        assert_eq!(0, align_frame(4096, 31));
    }

    #[test]
    fn test_align_frame_first_frame() {
        // First frame starts at header size (32)
        let page_size = 4096;
        let frame_size = WAL_FRAME_HEADER_SIZE + page_size;

        // Offset within the first frame should align to header size
        assert_eq!(
            WAL_HEADER_SIZE,
            align_frame(page_size, WAL_HEADER_SIZE + 100)
        );
        assert_eq!(
            WAL_HEADER_SIZE,
            align_frame(page_size, WAL_HEADER_SIZE + frame_size - 1)
        );
    }

    #[test]
    fn test_align_frame_second_frame() {
        let page_size = 4096;
        let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
        let second_frame_start = WAL_HEADER_SIZE + frame_size;

        // Offset at second frame start
        assert_eq!(
            second_frame_start,
            align_frame(page_size, second_frame_start)
        );
        // Offset within second frame
        assert_eq!(
            second_frame_start,
            align_frame(page_size, second_frame_start + 1000)
        );
    }

    #[test]
    fn test_align_frame_various_page_sizes() {
        for page_size in [1024u64, 2048, 4096, 8192, 16384, 32768, 65536] {
            let frame_size = WAL_FRAME_HEADER_SIZE + page_size;

            // At exact frame boundary
            let exact_offset = WAL_HEADER_SIZE + frame_size * 5;
            assert_eq!(exact_offset, align_frame(page_size, exact_offset));

            // Just past frame boundary
            assert_eq!(exact_offset, align_frame(page_size, exact_offset + 1));
        }
    }

    // ========== checksum tests ==========

    #[test]
    fn test_checksum_empty_initial_seeds() {
        // 8 bytes of zeros
        let data = [0u8; 8];
        let (s1, s2) = checksum(&data, 0, 0, false);
        // With all zeros, the result depends on the algorithm
        // s1 = 0 + 0 + 0 = 0, s2 = 0 + 0 + 0 = 0
        assert_eq!((0, 0), (s1, s2));
    }

    #[test]
    fn test_checksum_with_nonzero_seeds() {
        let data = [0u8; 8];
        let (s1, s2) = checksum(&data, 100, 200, false);
        // With seeds but zero data:
        // s1 = 100 + 0 + 200 = 300
        // s2 = 200 + 0 + 300 = 500
        assert_eq!(300, s1);
        assert_eq!(500, s2);
    }

    #[test]
    fn test_checksum_little_endian() {
        // Test data: [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
        let data = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let (s1, s2) = checksum(&data, 0, 0, false);

        // Little-endian: n1 = 0x04030201, n2 = 0x08070605
        let n1 = u32::from_le_bytes([0x01, 0x02, 0x03, 0x04]);
        let n2 = u32::from_le_bytes([0x05, 0x06, 0x07, 0x08]);

        // s1 = 0 + n1 + 0 = n1
        // s2 = 0 + n2 + s1 = n2 + n1
        let expected_s1 = n1;
        let expected_s2 = n2.wrapping_add(expected_s1);

        assert_eq!(expected_s1, s1);
        assert_eq!(expected_s2, s2);
    }

    #[test]
    fn test_checksum_big_endian() {
        let data = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let (s1, s2) = checksum(&data, 0, 0, true);

        // Big-endian: n1 = 0x01020304, n2 = 0x05060708
        let n1 = u32::from_be_bytes([0x01, 0x02, 0x03, 0x04]);
        let n2 = u32::from_be_bytes([0x05, 0x06, 0x07, 0x08]);

        let expected_s1 = n1;
        let expected_s2 = n2.wrapping_add(expected_s1);

        assert_eq!(expected_s1, s1);
        assert_eq!(expected_s2, s2);
    }

    #[test]
    fn test_checksum_multiple_iterations() {
        // 24 bytes = 3 iterations of 8 bytes
        let data = [0xFFu8; 24];
        let (s1, s2) = checksum(&data, 0, 0, false);

        // This should not panic (overflow is handled with wrapping_add)
        // Just verify it produces consistent results
        assert!(s1 > 0 || s2 > 0); // At least one should be non-zero
    }

    #[test]
    fn test_checksum_wrapping_overflow() {
        // Use max values to trigger overflow
        let data = [0xFF; 8];
        // Start with seeds near max
        let (s1, s2) = checksum(&data, u32::MAX - 100, u32::MAX - 100, false);

        // Should not panic due to wrapping_add
        // Just verify it runs without panic
        let _ = (s1, s2);
    }

    #[test]
    fn test_checksum_consistency() {
        let data = [0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0];

        // Same input should produce same output
        let (s1_a, s2_a) = checksum(&data, 0, 0, false);
        let (s1_b, s2_b) = checksum(&data, 0, 0, false);

        assert_eq!(s1_a, s1_b);
        assert_eq!(s2_a, s2_b);
    }

    #[test]
    #[should_panic(expected = "data length must be a multiple of 8")]
    fn test_checksum_invalid_length_panics() {
        let data = [0u8; 7]; // Not a multiple of 8
        checksum(&data, 0, 0, false);
    }

    // ========== CheckpointMode tests ==========

    #[test]
    fn test_checkpoint_mode_as_str() {
        assert_eq!("PASSIVE", CheckpointMode::Passive.as_str());
        assert_eq!("FULL", CheckpointMode::Full.as_str());
        assert_eq!("RESTART", CheckpointMode::Restart.as_str());
        assert_eq!("TRUNCATE", CheckpointMode::Truncate.as_str());
    }

    // ========== from_be_bytes_at tests ==========

    #[test]
    fn test_from_be_bytes_at_zero_offset() {
        let data = [0x12, 0x34, 0x56, 0x78, 0x00, 0x00, 0x00, 0x00];
        let result = from_be_bytes_at(&data, 0).unwrap();
        assert_eq!(0x12345678, result);
    }

    #[test]
    fn test_from_be_bytes_at_nonzero_offset() {
        let data = [0x00, 0x00, 0x00, 0x00, 0xAB, 0xCD, 0xEF, 0x01];
        let result = from_be_bytes_at(&data, 4).unwrap();
        assert_eq!(0xABCDEF01, result);
    }

    #[test]
    fn test_from_be_bytes_at_max_value() {
        let data = [0xFF, 0xFF, 0xFF, 0xFF];
        let result = from_be_bytes_at(&data, 0).unwrap();
        assert_eq!(u32::MAX, result);
    }

    #[test]
    fn test_from_be_bytes_at_min_value() {
        let data = [0x00, 0x00, 0x00, 0x00];
        let result = from_be_bytes_at(&data, 0).unwrap();
        assert_eq!(0, result);
    }

    #[test]
    fn test_from_be_bytes_at_out_of_bounds_returns_error() {
        let data = [0x00, 0x01, 0x02];
        let err = from_be_bytes_at(&data, 0).unwrap_err();
        assert_eq!(Error::UNEXPECTED_EOF_ERROR, err.code());
    }
}
