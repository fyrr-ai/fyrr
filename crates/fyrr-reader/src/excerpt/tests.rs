use super::*;
use fyrr_core::limits::mcp::EXCERPT_CODEPOINTS;
use lance_index::scalar::InvertedIndexParams;

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
                text: expected,
                highlights: None,
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

fn highlights(params: InvertedIndexParams, text: &str, start: u32, query: &str) -> Vec<[u32; 2]> {
    let mut tokenizer = params
        .base_tokenizer("simple".into())
        .lower_case(true)
        .build()
        .unwrap();
    let mut excerpt = Excerpt {
        text: text.into(),
        start,
        end: start + u32::try_from(text.chars().count()).unwrap(),
        highlights: None,
    };
    highlight(tokenizer.as_mut(), query, &mut excerpt).unwrap();
    excerpt.highlights.unwrap()
}

#[test]
fn highlights_follow_the_analyzer_in_codepoints_of_the_record() {
    // Codepoints: 😀 0, Runs 2-5, café 7-10, running 13-19, RUNNER 21-26; the
    // excerpt starts at record offset 100.
    let text = "😀 Runs café, running RUNNER";
    let plain = InvertedIndexParams::default();
    let stem = InvertedIndexParams::default().stem(true);
    assert_eq!(
        highlights(stem.clone(), text, 100, "run"),
        [[102, 106], [113, 120]]
    );
    assert_eq!(
        highlights(plain.clone(), text, 100, "run"),
        Vec::<[u32; 2]>::new()
    );
    assert_eq!(highlights(plain.clone(), text, 100, "runner"), [[121, 127]]);
    // Folding matches a multi-byte letter and keeps the offsets of the original.
    let fold = InvertedIndexParams::default().ascii_folding(true);
    assert_eq!(
        highlights(fold, text, 0, "cafe RUNNER"),
        [[7, 11], [21, 27]]
    );
    assert_eq!(highlights(plain.clone(), text, 0, "café"), [[7, 11]]);
}

#[test]
fn highlights_are_bounded() {
    let text = "a ".repeat(MAX_HIGHLIGHTS + 50);
    let ranges = highlights(InvertedIndexParams::default(), &text, 0, "a");
    assert_eq!(ranges.len(), MAX_HIGHLIGHTS);
    assert_eq!(ranges[1], [2, 3]);
}
