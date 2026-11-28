//! Anchored excerpt passages: sentence windows over byte spans, scored by the
//! query's terms they hold, its tokens as the index analyzer of the namespace
//! cuts them; offsets and the cap count codepoints.
use fyrr_core::attributes::analyzer::{Analyzer, Tokenizer};
use fyrr_core::config::NamespaceConfig;
use fyrr_core::limits::mcp::MAX_HIGHLIGHTS;
use fyrr_core::mcp::Excerpt;
use lance_index::scalar::InvertedIndexParams;
use lance_index::scalar::inverted::tokenizer::document_tokenizer::LanceTokenizer;

/// The analyzer of the full-text index of `searchable_text`, which the writer
/// builds the index with and highlights and excerpts match with: the `analyzer`
/// of the namespace.
pub fn fts_params(namespace: &NamespaceConfig) -> lance_core::Result<InvertedIndexParams> {
    Ok(analyzer_params(&namespace.settings.analyzer)?
        .with_position(namespace.settings.fts_positions))
}

/// The parameters of a full-text index with an analyzer, without positions. A
/// language stems the terms and removes its stop words; a list of stop words
/// replaces the language's.
pub fn analyzer_params(analyzer: &Analyzer) -> lance_core::Result<InvertedIndexParams> {
    let mut fts = InvertedIndexParams::default()
        .base_tokenizer(
            match analyzer.tokenizer {
                Tokenizer::Simple => "simple",
                Tokenizer::Whitespace => "whitespace",
                Tokenizer::Unicode => "icu",
                Tokenizer::Ngram => "ngram",
            }
            .into(),
        )
        .with_position(false)
        .max_token_length(Some(analyzer.max_token_length as usize))
        .lower_case(true)
        .stem(analyzer.language.is_some())
        .remove_stop_words(analyzer.language.is_some() || analyzer.stop_words.is_some())
        .custom_stop_words(analyzer.stop_words.clone())
        .ascii_folding(analyzer.ascii_folding);
    if let Some(language) = analyzer.language {
        fts = fts.language(&language.lance_name())?;
    }
    if analyzer.tokenizer == Tokenizer::Ngram {
        fts = fts
            .ngram_min_length(analyzer.ngram_min_length)
            .ngram_max_length(analyzer.ngram_max_length)
            .ngram_prefix_only(analyzer.prefix_only);
    }
    Ok(fts)
}

/// The tokens of `query` as the index analyzer cuts and reduces them, sorted
/// and without duplicates.
fn query_terms(tokenizer: &mut dyn LanceTokenizer, query: &str) -> Result<Vec<String>, String> {
    let mut terms = Vec::new();
    let mut stream = tokenizer.token_stream_for_search(query);
    while let Some(token) = stream.try_next()? {
        terms.push(token.text.clone());
    }
    drop(stream);
    terms.sort_unstable();
    terms.dedup();
    Ok(terms)
}

/// Sets the match ranges of `excerpt`, a passage of `record`: the tokens of the
/// record, as the index analyzer cuts and reduces them, that lie whole in the
/// passage and equal a token of `query`. A token the passage cuts is not marked.
/// `kind` is the tokenizer `tokenizer` was built with.
pub(crate) fn highlight(
    tokenizer: &mut dyn LanceTokenizer,
    kind: Tokenizer,
    query: &str,
    record: &str,
    excerpt: &mut Excerpt,
) -> Result<(), String> {
    let terms = query_terms(tokenizer, query)?;
    let codepoints = |text: &str| {
        let Ok(count) = u32::try_from(text.chars().count()) else {
            unreachable!("admitted excerpt codepoint bound");
        };
        count
    };
    let mut chars = record.chars();
    let mut skip = |count: u32| {
        if count > 0 {
            chars.nth(count as usize - 1);
        }
        record.len() - chars.as_str().len()
    };
    let from = skip(excerpt.start);
    let to = skip(excerpt.end - excerpt.start);
    // The record is tokenized from `head` to `tail`, outside the passage, so a
    // token that crosses an edge of the passage starts before it or ends after
    // it and is not marked. `simple` and `whitespace` end a token at the first
    // character of another class (not a letter or digit; ASCII white space),
    // whatever came before: one character beyond each edge is enough. `unicode`
    // segments a run of Chinese, Japanese or Thai letters from the start of the
    // run, so its slice runs from the ASCII white space before the passage to
    // the one after it, included: no run goes on across ASCII white space, and
    // the segmenter starts over at it. Other white space (U+00A0, U+202F) may
    // be inside a token.
    let (head, tail) = if matches!(kind, Tokenizer::Simple | Tokenizer::Whitespace) {
        (
            record[..from]
                .char_indices()
                .next_back()
                .map_or(0, |(at, _)| at),
            record[to..]
                .chars()
                .next()
                .map_or(to, |c| to + c.len_utf8()),
        )
    } else {
        (
            record[..from]
                .rfind(|c: char| c.is_ascii_whitespace())
                .unwrap_or(0),
            record[to..]
                .find(|c: char| c.is_ascii_whitespace())
                .map_or(record.len(), |at| to + at + 1),
        )
    };
    let mut ranges = Vec::new();
    let mut stream = tokenizer.token_stream_for_doc(&record[head..tail]);
    while ranges.len() < MAX_HIGHLIGHTS
        && let Some(token) = stream.try_next()?
    {
        let (at, end) = (head + token.offset_from, head + token.offset_to);
        if at < from || end > to || terms.binary_search(&token.text).is_err() {
            continue;
        }
        let start = excerpt.start + codepoints(&record[from..at]);
        ranges.push([start, start + codepoints(&record[at..end])]);
    }
    drop(stream);
    excerpt.highlights = Some(ranges);
    Ok(())
}

/// The query of an excerpt: the tokens of the index analyzer, sorted and without
/// duplicates; the bit of a term in a window is its position here.
pub(crate) struct Terms {
    terms: Vec<String>,
    tokenizer: Box<dyn LanceTokenizer>,
}
impl Terms {
    /// The terms of `query` as `tokenizer`, the tokenizer of the index, cuts them.
    ///
    /// `fyrr_core` refuses the `ngram` tokenizer for the text at `namespace.analyzer`,
    /// in a configuration file, and in the stored settings of a namespace
    /// (`NamespaceSettings::validate`, run when the policy artifact is read), and
    /// an excerpt is only ever of `searchable_text`: no n-gram tokenizer reaches
    /// `Terms`. N-gram tokens would match every sentence and cost a window per
    /// n-gram, so selection must not be given one.
    pub(crate) fn new(tokenizer: &dyn LanceTokenizer, query: &str) -> Result<Self, String> {
        let mut tokenizer = tokenizer.box_clone();
        let terms = query_terms(tokenizer.as_mut(), query)?;
        Ok(Self { terms, tokenizer })
    }
    pub(crate) fn select(&self, text: &str, cap: u32) -> Result<Excerpt, String> {
        select(
            &mut AnalyzedMatcher {
                terms: &self.terms,
                tokenizer: self.tokenizer.box_clone(),
            },
            text,
            cap,
        )
    }
}

fn select(matcher: &mut AnalyzedMatcher<'_>, text: &str, cap: u32) -> Result<Excerpt, String> {
    let cap = cap as usize;
    let spans = sentence_spans(text);
    let words = matcher.terms().div_ceil(64);
    let mut matches = vec![0u64; words * spans.len()];
    for (index, span) in spans.iter().enumerate() {
        if span.end_cp - span.start_cp <= cap {
            matcher.fill(
                &text[span.start..span.end],
                &mut matches[index * words..(index + 1) * words],
            )?;
        }
    }
    let mut covered = vec![0u64; words];
    let mut best = None;
    let mut counts = vec![0usize; matcher.terms()];
    let (mut left, mut score) = (0, 0);
    // Each sentence enters and leaves once. A redundant left edge never
    // helps a later window: its newer copies expire after that edge does.
    for (right, end) in spans.iter().enumerate() {
        for term in bit_indices(&matches[right * words..(right + 1) * words]) {
            score += usize::from(counts[term] == 0);
            counts[term] += 1;
        }
        while left <= right {
            let bits = &matches[left * words..(left + 1) * words];
            let too_long = end.end_cp - spans[left].start_cp > cap;
            let redundant = left < right && bit_indices(bits).all(|term| counts[term] > 1);
            if !too_long && !redundant {
                break;
            }
            for term in bit_indices(bits) {
                counts[term] -= 1;
                score -= usize::from(counts[term] == 0);
            }
            left += 1;
        }
        if left <= right && score != 0 {
            let span = &spans[left];
            let length = end.end_cp - span.start_cp;
            let candidate = (
                score,
                std::cmp::Reverse(length),
                std::cmp::Reverse(span.start_cp),
                left,
                right,
            );
            if best.is_none_or(|best| candidate > best) {
                best = Some(candidate);
            }
        }
    }
    if let Some((_, _, _, mut left, mut right)) = best {
        loop {
            let mut changed = false;
            if right + 1 < spans.len() && spans[right + 1].end_cp - spans[left].start_cp <= cap {
                right += 1;
                changed = true;
            }
            if left > 0 && spans[right].end_cp - spans[left - 1].start_cp <= cap {
                left -= 1;
                changed = true;
            }
            if !changed {
                break;
            }
        }
        return Ok(passage(
            text,
            spans[left].start,
            spans[right].end,
            spans[left].start_cp,
            spans[right].end_cp,
        ));
    }
    matcher.fallback(text, cap, &mut covered)
}

struct AnalyzedMatcher<'a> {
    terms: &'a [String],
    tokenizer: Box<dyn LanceTokenizer>,
}
impl AnalyzedMatcher<'_> {
    fn terms(&self) -> usize {
        self.terms.len()
    }
    /// Sets the bit of every term that a token of `text` equals.
    fn fill(&mut self, text: &str, bits: &mut [u64]) -> Result<(), String> {
        let mut stream = self.tokenizer.token_stream_for_doc(text);
        while let Some(token) = stream.try_next()? {
            if let Ok(term) = self.terms.binary_search(&token.text) {
                bits[term / 64] |= 1u64 << (term % 64);
            }
        }
        Ok(())
    }
    /// The window of `cap` codepoints with the most distinct terms.
    fn fallback(&mut self, text: &str, cap: usize, covered: &mut [u64]) -> Result<Excerpt, String> {
        // No sentence window within the cap holds a term: score windows around
        // the tokens of the whole text. Offsets are needed only on this
        // fallback, and ASCII text needs no codepoint-to-byte map.
        let ascii = text.is_ascii();
        let offsets = if ascii {
            Vec::new()
        } else {
            text.char_indices()
                .map(|(byte, _)| byte)
                .chain([text.len()])
                .collect::<Vec<_>>()
        };
        let length = if ascii { text.len() } else { offsets.len() - 1 };
        let byte = |cp: usize| if ascii { cp } else { offsets[cp] };
        let codepoint = |byte: usize| {
            if ascii {
                byte
            } else {
                offsets.partition_point(|at| *at < byte)
            }
        };
        // The codepoint range and the term of every matching token, in order.
        let mut found = Vec::new();
        if !self.terms.is_empty() {
            let mut stream = self.tokenizer.token_stream_for_doc(text);
            while let Some(token) = stream.try_next()? {
                if let Ok(term) = self.terms.binary_search(&token.text) {
                    found.push((
                        codepoint(token.offset_from),
                        codepoint(token.offset_to),
                        term,
                    ));
                }
            }
        }
        found.sort_unstable();
        let mut starts = vec![0];
        for (from, _, _) in &found {
            starts.push(from.saturating_sub(cap / 3).min(length.saturating_sub(cap)));
            starts.push(*from);
        }
        starts.sort_unstable();
        starts.dedup();
        let mut best = (0usize, std::cmp::Reverse(0usize));
        for start in starts {
            covered.fill(0);
            let end = (start + cap).min(length);
            let first = found.partition_point(|(from, _, _)| *from < start);
            for (_, to, term) in found[first..].iter().take_while(|(from, _, _)| *from < end) {
                if *to <= end {
                    covered[term / 64] |= 1u64 << (term % 64);
                }
            }
            let score = covered
                .iter()
                .map(|w| w.count_ones() as usize)
                .sum::<usize>();
            best = best.max((score, std::cmp::Reverse(start)));
        }
        let start = best.1.0;
        let end = (start + cap).min(length);
        Ok(passage(text, byte(start), byte(end), start, end))
    }
}

fn bit_indices(bits: &[u64]) -> impl Iterator<Item = usize> + '_ {
    bits.iter().enumerate().flat_map(|(word, bits)| {
        let mut remaining = *bits;
        std::iter::from_fn(move || {
            if remaining == 0 {
                return None;
            }
            let bit = remaining.trailing_zeros() as usize;
            remaining &= remaining - 1;
            Some(word * 64 + bit)
        })
    })
}

fn passage(text: &str, start: usize, end: usize, start_cp: usize, end_cp: usize) -> Excerpt {
    let (Ok(start_cp), Ok(end_cp)) = (u32::try_from(start_cp), u32::try_from(end_cp)) else {
        unreachable!("admitted record codepoint bound");
    };
    Excerpt {
        text: text[start..end].to_owned(),
        start: start_cp,
        end: end_cp,
        highlights: None,
    }
}

struct Span {
    start: usize,
    end: usize,
    start_cp: usize,
    end_cp: usize,
}
fn sentence_spans(text: &str) -> Vec<Span> {
    if text.is_ascii() {
        return ascii_sentence_spans(text);
    }
    let mut spans = Vec::new();
    let mut start = 0;
    let mut start_cp = 0;
    let mut end_cp = 0;
    let mut iter = text.char_indices().enumerate().peekable();
    while let Some((cp, (at, c))) = iter.next() {
        end_cp = cp + 1;
        let mut boundary = if matches!(c, '.' | '!' | '?')
            && iter.peek().is_none_or(|(_, (_, c))| c.is_whitespace())
        {
            Some((at + c.len_utf8(), cp + 1))
        } else {
            None
        };
        if c == '\n' && boundary.is_none() {
            for (cp, (at, c)) in iter.clone() {
                if !c.is_whitespace() {
                    break;
                }
                if c == '\n' {
                    boundary = Some((at + 1, cp + 1));
                }
            }
        }
        if let Some((end, cp)) = boundary {
            while iter.peek().is_some_and(|(_, (at, _))| *at < end) {
                iter.next();
            }
            end_cp = cp;
            push_span(text, &mut spans, start, end, start_cp, cp);
            start = end;
            start_cp = cp;
        }
    }
    push_span(text, &mut spans, start, text.len(), start_cp, end_cp);
    spans
}
fn ascii_sentence_spans(text: &str) -> Vec<Span> {
    let bytes = text.as_bytes();
    // char::is_whitespace includes vertical tab; u8::is_ascii_whitespace does not.
    let space = |byte: u8| matches!(byte, b'\t'..=b'\r' | b' ');
    let (mut start, mut at) = (0, 0);
    let mut spans = Vec::new();
    while at < bytes.len() {
        let mut end = if matches!(bytes[at], b'.' | b'!' | b'?')
            && bytes.get(at + 1).is_none_or(|byte| space(*byte))
        {
            Some(at + 1)
        } else {
            None
        };
        if bytes[at] == b'\n' {
            let mut lookahead = at + 1;
            while lookahead < bytes.len() && space(bytes[lookahead]) {
                if bytes[lookahead] == b'\n' {
                    end = Some(lookahead + 1);
                }
                lookahead += 1;
            }
        }
        if let Some(end) = end {
            push_span(text, &mut spans, start, end, start, end);
            start = end;
            at = end;
        } else {
            at += 1;
        }
    }
    push_span(text, &mut spans, start, bytes.len(), start, bytes.len());
    spans
}
fn push_span(
    text: &str,
    spans: &mut Vec<Span>,
    mut start: usize,
    end: usize,
    mut start_cp: usize,
    end_cp: usize,
) {
    for c in text[start..end].chars().take_while(|c| c.is_whitespace()) {
        start += c.len_utf8();
        start_cp += 1;
    }
    if start < end {
        spans.push(Span {
            start,
            end,
            start_cp,
            end_cp,
        });
    }
}

#[cfg(test)]
#[path = "excerpt/tests.rs"]
mod tests;
