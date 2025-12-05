#![no_main]

use libfuzzer_sys::fuzz_target;
use replited::base::{parse_snapshot_path, parse_wal_path, parse_wal_segment_path};

fuzz_target!(|data: &str| {
    // Test path parsing logic with arbitrary strings.
    // Regex parsing can sometimes be vulnerable or panic on specific inputs if not careful.
    let _ = parse_wal_path(data);
    let _ = parse_snapshot_path(data);
    let _ = parse_wal_segment_path(data);
});
