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

/// The analyzer a test names in full: Lance's defaults stem, fold and drop stop words.
fn analyzer(stem: bool, fold: bool) -> InvertedIndexParams {
    InvertedIndexParams::default()
        .base_tokenizer("simple".into())
        .max_token_length(Some(40))
        .language("English")
        .unwrap()
        .lower_case(true)
        .stem(stem)
        .ascii_folding(fold)
        .remove_stop_words(false)
}

/// The ranges of `query` in the passage `[start, end)` codepoints of `record`.
fn highlights(
    params: InvertedIndexParams,
    record: &str,
    (start, end): (usize, usize),
    query: &str,
) -> Vec<[u32; 2]> {
    let mut tokenizer = params.build().unwrap();
    let mut excerpt = Excerpt {
        text: record.chars().skip(start).take(end - start).collect(),
        start: u32::try_from(start).unwrap(),
        end: u32::try_from(end).unwrap(),
        highlights: None,
    };
    highlight(tokenizer.as_mut(), query, record, &mut excerpt).unwrap();
    excerpt.highlights.unwrap()
}

#[test]
fn highlights_follow_the_analyzer_in_codepoints_of_the_record() {
    // Codepoints: 😀 0, Runs 2-5, café 7-10, running 13-19, RUNNER 21-26.
    let record = "😀 Runs café, running RUNNER";
    let all = (0, 27);
    assert_eq!(
        highlights(analyzer(true, false), record, all, "run"),
        [[2, 6], [13, 20]]
    );
    assert_eq!(
        highlights(analyzer(false, false), record, all, "run"),
        Vec::<[u32; 2]>::new()
    );
    assert_eq!(
        highlights(analyzer(false, false), record, all, "runner"),
        [[21, 27]]
    );
    // Folding matches a multi-byte letter and keeps the offsets of the original.
    assert_eq!(
        highlights(analyzer(false, true), record, all, "cafe RUNNER"),
        [[7, 11], [21, 27]]
    );
    assert_eq!(
        highlights(analyzer(false, false), record, all, "café"),
        [[7, 11]]
    );
    // A passage reports offsets of the record, not of itself.
    assert_eq!(
        highlights(analyzer(true, false), record, (7, 27), "run"),
        [[13, 20]]
    );
}

#[test]
fn highlights_skip_a_token_the_passage_cuts() {
    let plain = || analyzer(false, false);
    // `cat` of `catapult` is no token of the index, at either edge.
    let record = "catapult cat";
    assert_eq!(
        highlights(plain(), record, (0, 3), "cat"),
        Vec::<[u32; 2]>::new()
    );
    assert_eq!(highlights(plain(), record, (9, 12), "cat"), [[9, 12]]);
    let record = "concat é";
    assert_eq!(
        highlights(plain(), record, (3, 6), "cat"),
        Vec::<[u32; 2]>::new()
    );
    // Selection and highlight together: the first window of the record.
    let mut excerpt = select("catapult cat", "cat", 3);
    assert_eq!((excerpt.start, excerpt.text.as_str()), (0, "cat"));
    highlight(
        plain().build().unwrap().as_mut(),
        "cat",
        "catapult cat",
        &mut excerpt,
    )
    .unwrap();
    assert_eq!(excerpt.highlights, Some(Vec::new()));
    // A token set off by punctuation or a space is whole.
    let record = "a cat, b";
    assert_eq!(highlights(plain(), record, (2, 5), "cat"), [[2, 5]]);
}

#[test]
fn highlights_are_bounded() {
    let record = "a ".repeat(MAX_HIGHLIGHTS + 50);
    let end = record.chars().count();
    let ranges = highlights(analyzer(false, false), &record, (0, end), "a");
    assert_eq!(ranges.len(), MAX_HIGHLIGHTS);
    assert_eq!(ranges[1], [2, 3]);
}
