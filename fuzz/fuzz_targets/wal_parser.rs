#![no_main]

use libfuzzer_sys::fuzz_target;
use replited::sqlite::{WALFrame, WALHeader};
use std::io::Cursor;

fuzz_target!(|data: &[u8]| {
    let mut reader = Cursor::new(data);

    // 1. WAL Header parsing attempt
    // Parsing should not panic even if data is insufficient or invalid.
    let _ = WALHeader::read_from(&mut reader);

    reader.set_position(0);

    // 2. WAL Frame parsing attempt
    // Fixed page size for testing, though in reality it varies.
    // The goal is to ensure the parser handles arbitrary data gracefully.
    let page_size = 4096;
    let _ = WALFrame::read(&mut reader, page_size);
});
