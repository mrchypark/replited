#![no_main]

use libfuzzer_sys::fuzz_target;
use replited::sqlite::checksum;

fuzz_target!(|data: (Vec<u8>, u32, u32, bool)| {
    let (buf, s1, s2, is_big_endian) = data;
    // Length check to improve fuzzing efficiency and match valid use cases
    if buf.len() > 65536 || buf.len() % 8 != 0 {
        return;
    }
    let _ = checksum(&buf, s1, s2, is_big_endian);
});
