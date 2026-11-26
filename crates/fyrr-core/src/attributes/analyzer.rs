//! How the full-text index of an attribute cuts a text into terms.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The longest n-gram an index may ask for: every position of every token
/// yields up to this many terms.
pub const NGRAM_LENGTH_MAX: u32 = 16;
/// The most words, and the most bytes in one, of a custom stop-word list.
pub const STOP_WORDS_MAX: usize = 1024;
pub const STOP_WORD_BYTES_MAX: usize = 64;
/// The most bytes `max_token_length` may name.
const FTS_MAX_TOKEN_LENGTH_MAX: u64 = 1024;
/// The tokens of an index are shorter than this many bytes when none is asked for.
const MAX_TOKEN_LENGTH_DEFAULT: u32 = 40;
/// Lance's n-gram lengths when none is asked for.
const NGRAM_LENGTH_DEFAULT: u32 = 3;

/// The way a text is cut into tokens.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Tokenizer {
    /// Runs of letters and digits: a text in Chinese, Japanese or Korean is one
    /// token per run of such characters.
    #[default]
    Simple,
    /// Runs of characters that are not white space.
    Whitespace,
    /// Unicode word segmentation, which finds the words of a text in Chinese or
    /// Japanese by a dictionary.
    Unicode,
    /// Every substring of a token between `ngram_min_length` and
    /// `ngram_max_length` characters, or only the ones that start it.
    Ngram,
}

/// A language with a stemmer and a stop-word list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Language {
    Arabic,
    Danish,
    Dutch,
    English,
    Finnish,
    French,
    German,
    Greek,
    Hungarian,
    Italian,
    Norwegian,
    Portuguese,
    Romanian,
    Russian,
    Spanish,
    Swedish,
    Tamil,
    Turkish,
}

impl Language {
    pub const ALL: [Self; 18] = [
        Self::Arabic,
        Self::Danish,
        Self::Dutch,
        Self::English,
        Self::Finnish,
        Self::French,
        Self::German,
        Self::Greek,
        Self::Hungarian,
        Self::Italian,
        Self::Norwegian,
        Self::Portuguese,
        Self::Romanian,
        Self::Russian,
        Self::Spanish,
        Self::Swedish,
        Self::Tamil,
        Self::Turkish,
    ];

    /// The name Lance knows the language by.
    pub fn lance_name(self) -> String {
        format!("{self:?}")
    }
}

/// The settings of a full-text index that decide which terms a text yields.
/// A text is lowercased always. Changing them on an attribute changes its index,
/// which the writer's maintenance rebuilds while the old one keeps serving; the
/// analyzer of `searchable_text` is fixed when the namespace is created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Analyzer {
    #[serde(default)]
    pub tokenizer: Tokenizer,
    /// Stems the terms and removes the stop words of the language. Absent: no
    /// stemming, and no stop words but `stop_words`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<Language>,
    /// The stop words to remove instead of the language's: compared with a term
    /// after it is lowercased and stemmed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_words: Option<Vec<String>>,
    #[serde(default)]
    pub ascii_folding: bool,
    /// Tokens of this many UTF-8 bytes or more are dropped: a token is kept when it is
    /// shorter than this.
    #[serde(default = "max_token_length")]
    #[schemars(range(min = 1, max = FTS_MAX_TOKEN_LENGTH_MAX))]
    pub max_token_length: u32,
    /// Only with the `ngram` tokenizer.
    #[serde(default = "ngram_length")]
    #[schemars(range(min = 1, max = NGRAM_LENGTH_MAX))]
    pub ngram_min_length: u32,
    /// Only with the `ngram` tokenizer; not below `ngram_min_length`.
    #[serde(default = "ngram_length")]
    #[schemars(range(min = 1, max = NGRAM_LENGTH_MAX))]
    pub ngram_max_length: u32,
    /// Only with the `ngram` tokenizer: only the n-grams that start a token.
    #[serde(default)]
    pub prefix_only: bool,
}

fn max_token_length() -> u32 {
    MAX_TOKEN_LENGTH_DEFAULT
}

fn ngram_length() -> u32 {
    NGRAM_LENGTH_DEFAULT
}

impl Default for Analyzer {
    fn default() -> Self {
        Self {
            tokenizer: Tokenizer::default(),
            language: None,
            stop_words: None,
            ascii_folding: false,
            max_token_length: MAX_TOKEN_LENGTH_DEFAULT,
            ngram_min_length: NGRAM_LENGTH_DEFAULT,
            ngram_max_length: NGRAM_LENGTH_DEFAULT,
            prefix_only: false,
        }
    }
}

impl Analyzer {
    /// Whether every setting is within its bounds, and none is set that its
    /// tokenizer ignores: the n-gram ones need the `ngram` tokenizer, which in
    /// turn keeps every n-gram and so takes no stemming and no stop words.
    pub fn is_valid(&self) -> bool {
        let ngram = self.tokenizer == Tokenizer::Ngram;
        let stop_words = self.stop_words.as_deref().unwrap_or_default();
        (1..=FTS_MAX_TOKEN_LENGTH_MAX).contains(&u64::from(self.max_token_length))
            && (1..=NGRAM_LENGTH_MAX).contains(&self.ngram_min_length)
            && (self.ngram_min_length..=NGRAM_LENGTH_MAX).contains(&self.ngram_max_length)
            && stop_words.len() <= STOP_WORDS_MAX
            && stop_words
                .iter()
                .all(|word| (1..=STOP_WORD_BYTES_MAX).contains(&word.len()))
            && if ngram {
                self.language.is_none() && self.stop_words.is_none()
            } else {
                (
                    self.ngram_min_length,
                    self.ngram_max_length,
                    self.prefix_only,
                ) == (NGRAM_LENGTH_DEFAULT, NGRAM_LENGTH_DEFAULT, false)
            }
    }
}
