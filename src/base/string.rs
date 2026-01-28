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

#[cfg(test)]
mod tests {
    use super::*;

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
}
