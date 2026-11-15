//! Whole-unit, zero-overlap chunking. Short units are unchanged.

pub const MAX_BYTES: usize = 16_384;

pub fn chunks(text: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut rest = text;
    while rest.len() > MAX_BYTES {
        let mut bound = MAX_BYTES;
        while !rest.is_char_boundary(bound) {
            bound -= 1;
        }
        let prefix = &rest[..bound];
        // Prefer structural boundaries before lines, sentences and words.
        let split = ["\n\n", "\n", ". ", " "]
            .iter()
            .find_map(|separator| {
                prefix
                    .rfind(separator)
                    .filter(|&p| p > bound / 2)
                    .map(|p| p + separator.len())
            })
            .unwrap_or(bound);
        result.push(&rest[..split]);
        rest = &rest[split..];
    }
    if !rest.is_empty() {
        result.push(rest);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn whole_units_preserve_utf8_bytes_without_overlap() {
        for text in [
            "exact\n\ntext\n".to_owned(),
            "ž".repeat(20_000),
            "a line\n".repeat(8_000),
        ] {
            let pieces = chunks(&text);
            assert_eq!(pieces.concat(), text);
            assert!(pieces.iter().all(|piece| piece.len() <= MAX_BYTES));
        }
    }
}
