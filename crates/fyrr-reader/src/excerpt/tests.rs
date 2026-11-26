use super::*;
use fyrr_core::limits::mcp::EXCERPT_CODEPOINTS;
use lance_index::scalar::InvertedIndexParams;

/// A namespace with `extra` keys next to its name.
fn namespace(extra: serde_json::Value) -> NamespaceConfig {
    let mut namespace = serde_json::json!({"name": "docs"});
    namespace.as_object_mut().unwrap().extend(
        extra
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone())),
    );
    serde_json::from_value(namespace).unwrap()
}
/// The terms of `query` as the reader builds them for `namespace`.
fn terms(namespace: &NamespaceConfig, query: &str) -> Terms {
    let tokenizer = fts_params(namespace).unwrap().build().unwrap();
    Terms::new(tokenizer.as_ref(), query).unwrap()
}
fn select_in(extra: serde_json::Value, text: &str, query: &str, cap: u32) -> Excerpt {
    terms(&namespace(extra), query).select(text, cap).unwrap()
}
/// A selection in a default namespace.
fn select(text: &str, query: &str, cap: u32) -> Excerpt {
    select_in(serde_json::json!({}), text, query, cap)
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

#[test]
fn selection_follows_the_analyzer() {
    let cap = 30;
    let text = "Die Katze schläft lange. Wir sahen viele Häuser dort. Am Ende blieb Ruhe.";
    let german = || serde_json::json!({"analyzer": {"language": "german"}});
    // Stemming: `haus` and `Häuser` share a stem.
    let stemmed = select_in(german(), text, "haus", cap);
    assert_eq!(stemmed.text, "Wir sahen viele Häuser dort.");
    assert_eq!(stemmed.start, 25);
    // The default analyzer has no stemmer: the word is not found, the prefix is taken.
    let plain = select(text, "haus", cap);
    assert_eq!(
        (plain.start, plain.text.as_str()),
        (0, "Die Katze schläft lange. Wir s")
    );
    // Stop words match nothing: the query is empty and the excerpt is the prefix.
    let stop = select_in(german(), text, "die der", cap);
    assert_eq!(
        (stop.start, stop.text.as_str()),
        (0, "Die Katze schläft lange. Wir s")
    );
    // English stemming of the analyzer's language.
    let text = "Cats sleep. Dogs ran home. Birds fly.";
    let stemmed = select_in(
        serde_json::json!({"analyzer": {"language": "english"}}),
        text,
        "running dog",
        14,
    );
    assert_eq!(stemmed.text, "Dogs ran home.");
    // Folding: the accent of the text meets the plain letter of the query.
    let text = "Prague is old. The café is closed. Bye.";
    let folded = select_in(
        serde_json::json!({"analyzer": {"ascii_folding": true}}),
        text,
        "cafe",
        20,
    );
    assert_eq!(folded.text, "The café is closed.");
    let plain = select(text, "cafe", 20);
    assert_eq!(
        (plain.start, plain.text.as_str()),
        (0, "Prague is old. The c")
    );
    // The hard fallback finds a match inside a sentence longer than the cap.
    let long = format!("{} Häuser {}", "x ".repeat(100), "y ".repeat(100));
    let hard = select_in(german(), &long, "haus", cap);
    assert!(hard.text.contains("Häuser"), "{hard:?}");
}

#[test]
fn a_default_namespace_selects_with_the_tokens_of_the_index() {
    // The index drops a token over 40 bytes, so the analyzer finds no term in
    // this query and the excerpt is the start of the record.
    let word = "a".repeat(50);
    let text = format!("First sentence. {word}. Last.");
    let default = select(&text, &word, 52);
    assert_eq!(default.start, 0);
    // The index cuts `foo_bar` into `foo` and `bar`, and the first sentence
    // holds both of them: the excerpt is it, not the later `foo_bar`.
    let text = "foo bar now. Then foo_bar.";
    let default = select(text, "foo_bar", 14);
    assert_eq!((default.start, default.text.as_str()), (0, "foo bar now."));
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
    // Selection and highlight together: the window of the whole token, not the
    // `cat` that starts `catapult`, which is no token of the index.
    let mut excerpt = select("catapult cat", "cat", 3);
    assert_eq!((excerpt.start, excerpt.text.as_str()), (9, "cat"));
    highlight(
        plain().build().unwrap().as_mut(),
        "cat",
        "catapult cat",
        &mut excerpt,
    )
    .unwrap();
    assert_eq!(excerpt.highlights, Some(vec![[9, 12]]));
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
