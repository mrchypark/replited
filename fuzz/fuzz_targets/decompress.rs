#![no_main]

use libfuzzer_sys::fuzz_target;
use replited::base::decompressed_data;

fuzz_target!(|data: &[u8]| {
    // Attempt to decompress arbitrary bytes.
    // Invalid formats should return an Error, not panic.
    let _ = decompressed_data(data.to_vec());
});
