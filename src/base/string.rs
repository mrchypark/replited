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
    use proptest::prelude::*;

    #[test]
    fn keeps_requested_suffix_visible_when_input_is_longer_than_unmask_len() {
        let result = mask_string("secretpassword", 4);
        assert_eq!("******word", result);
    }

    #[test]
    fn returns_original_string_when_unmask_len_covers_entire_input() {
        let result = mask_string("1234", 4);
        assert_eq!("1234", result);
    }

    #[test]
    fn returns_original_string_when_input_is_shorter_than_unmask_len() {
        let result = mask_string("ab", 4);
        assert_eq!("ab", result);
    }

    #[test]
    fn returns_original_string_when_input_is_empty() {
        let result = mask_string("", 4);
        assert_eq!("", result);
    }

    #[test]
    fn masks_entire_string_when_unmask_len_is_zero() {
        let result = mask_string("secret", 0);
        assert_eq!("******", result);
    }

    fn ascii_string_strategy() -> impl Strategy<Value = String> {
        proptest::collection::vec(0x20u8..0x7fu8, 0..32)
            .prop_map(|bytes| String::from_utf8(bytes).expect("ascii bytes should be valid utf-8"))
    }

    proptest! {
        #[test]
        fn returns_input_unchanged_when_unmask_len_is_at_least_ascii_input_length(
            input in ascii_string_strategy(),
            extra in 0usize..8,
        ) {
            let unmask_len = input.len() + extra;

            prop_assert_eq!(mask_string(&input, unmask_len), input);
        }

        #[test]
        fn keeps_mask_prefix_and_suffix_when_ascii_input_exceeds_unmask_len(
            input in ascii_string_strategy(),
            unmask_len in 0usize..32,
        ) {
            prop_assume!(input.len() > unmask_len);

            let masked = mask_string(&input, unmask_len);
            let expected_suffix = &input[input.len() - unmask_len..];

            prop_assert!(masked.starts_with("******"));
            prop_assert_eq!(masked.len(), 6 + unmask_len);
            prop_assert_eq!(&masked[6..], expected_suffix);
        }
    }
}
