use super::*;
use fyrr_core::limits::mcp::EXCERPT_CODEPOINTS;

fn select(text: &str, query: &str, cap: u32) -> Excerpt {
    Terms::new(query).select(text, cap)
}
#[test]
fn excerpt_cases() {
    let unicode = "\u{85}😀 No.\n \n\t\n  Target café! End?";
    let long = format!("{} needle {}", "x".repeat(1600), "y".repeat(1600));
    let sentences = format!(
        "{}. Target. {}. {}.",
        "l".repeat(700),
        "r".repeat(700),
        "z".repeat(700)
    );
    for (name, text, query, cap, start, expected) in [
        (
            "unicode offsets",
            unicode,
            "CAFÉ",
            EXCERPT_CODEPOINTS,
            1,
            unicode.chars().skip(1).collect::<String>(),
        ),
        (
            "hard cut",
            long.as_str(),
            "needle",
            EXCERPT_CODEPOINTS,
            1201,
            long[1201..2401].into(),
        ),
        (
            "no match prefix",
            long.as_str(),
            "absent",
            EXCERPT_CODEPOINTS,
            0,
            long[..1200].into(),
        ),
        (
            "empty input",
            "",
            "needle",
            EXCERPT_CODEPOINTS,
            0,
            String::new(),
        ),
        (
            "right first expansion",
            sentences.as_str(),
            "target",
            EXCERPT_CODEPOINTS,
            702,
            format!("Target. {}.", "r".repeat(700)),
        ),
        (
            "duplicate terms",
            "a. xx. a. b.",
            "a b",
            6,
            7,
            "a. b.".into(),
        ),
    ] {
        let actual = select(text, query, cap);
        assert_eq!(
            actual,
            Excerpt {
                start,
                end: start + u32::try_from(expected.chars().count()).unwrap(),
                text: expected
            },
            "{name}"
        );
    }
    let query = (0..100)
        .map(|n| format!("term{n}"))
        .collect::<Vec<_>>()
        .join(" ");
    let text = "😀 Absent. TERM99 café. Last.";
    let actual = select(text, &query, 20);
    assert!(actual.text.contains("TERM99"));
    assert_eq!(
        actual.text,
        text.chars()
            .skip(actual.start as usize)
            .take((actual.end - actual.start) as usize)
            .collect::<String>()
    );
}
