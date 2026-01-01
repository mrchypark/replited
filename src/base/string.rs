/// Mask a string by "******", but keep `unmask_len` of suffix.
#[inline]
pub fn mask_string(s: &str, unmask_len: usize) -> String {
    if s.len() <= unmask_len {
        s.to_string()
    } else {
        let mut ret = "******".to_string();
        ret.push_str(&s[(s.len() - unmask_len)..]);
        ret
    }
}

pub fn u8_array_as_hex(arr: &[u8]) -> String {
    let hex_str: String = arr
        .iter()
        .map(|byte| format!("0x{byte:02X}"))
        .collect::<Vec<_>>()
        .join(" ");

    hex_str
}

pub fn format_integer_with_leading_zeros(num: u32) -> String {
    format!("{num:08X}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;

    #[test]
    fn test_format_integer_with_leading_zeros() -> Result<()> {
        let num = 0xab12;
        let hex = format_integer_with_leading_zeros(num);
        assert_eq!(&hex, "0000AB12");
        Ok(())
    }

    #[test]
    fn test_format_integer_with_leading_zeros_zero() {
        assert_eq!("00000000", format_integer_with_leading_zeros(0));
    }

    #[test]
    fn test_format_integer_with_leading_zeros_max() {
        assert_eq!("FFFFFFFF", format_integer_with_leading_zeros(u32::MAX));
    }

    // ========== mask_string tests ==========

    #[test]
    fn test_mask_string_normal() {
        // String longer than unmask_len
        let result = mask_string("secretpassword", 4);
        assert_eq!("******word", result);
    }

    #[test]
    fn test_mask_string_equal_length() {
        // String length equals unmask_len - should return original
        let result = mask_string("1234", 4);
        assert_eq!("1234", result);
    }

    #[test]
    fn test_mask_string_shorter() {
        // String shorter than unmask_len - should return original
        let result = mask_string("ab", 4);
        assert_eq!("ab", result);
    }

    #[test]
    fn test_mask_string_empty() {
        let result = mask_string("", 4);
        assert_eq!("", result);
    }

    #[test]
    fn test_mask_string_zero_unmask() {
        // unmask_len is 0 - entire suffix is masked
        let result = mask_string("secret", 0);
        assert_eq!("******", result);
    }

    // ========== u8_array_as_hex tests ==========

    #[test]
    fn test_u8_array_as_hex_simple() {
        let arr = [0x00, 0x0F, 0xFF];
        let result = u8_array_as_hex(&arr);
        assert_eq!("0x00 0x0F 0xFF", result);
    }

    #[test]
    fn test_u8_array_as_hex_empty() {
        let arr: [u8; 0] = [];
        let result = u8_array_as_hex(&arr);
        assert_eq!("", result);
    }

    #[test]
    fn test_u8_array_as_hex_single() {
        let arr = [0xAB];
        let result = u8_array_as_hex(&arr);
        assert_eq!("0xAB", result);
    }

    #[test]
    fn test_u8_array_as_hex_leading_zeros() {
        let arr = [0x01, 0x02, 0x03];
        let result = u8_array_as_hex(&arr);
        assert_eq!("0x01 0x02 0x03", result);
    }
}
