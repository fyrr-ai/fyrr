//! `[[identity.api_keys]]`: operator-declared bearer secrets for HTTP MCP.
//!
//! A key is a static secret with grants the operator states in the file, like
//! the stdio subject's. The file holds an environment reference, never the
//! secret; the writer publishes only a keyed hash of it.

use chrono::{DateTime, Utc};

use super::*;

/// Every API-key secret starts with this; a bearer token that does not is an
/// OIDC access token.
pub const API_KEY_PREFIX: &str = "fyrr_key_";
/// Fewest characters after the prefix: 43 characters of `[A-Za-z0-9_-]` carry
/// at least 256 bits when generated randomly.
pub const API_KEY_MIN_RANDOM: usize = 43;
/// Most characters after the prefix, so a bearer is bounded before it is hashed.
pub const API_KEY_MAX_RANDOM: usize = 128;
/// At most this many keys per namespace.
pub const API_KEYS_MAX: usize = 256;
/// Longest `user` value, in bytes.
const API_KEY_USER_MAX_BYTES: usize = 512;

/// Whether `text` is a well-formed API-key secret. The reader and the writer
/// check this before they hash a value.
pub fn is_api_key_secret(text: &str) -> bool {
    text.strip_prefix(API_KEY_PREFIX).is_some_and(|random| {
        (API_KEY_MIN_RANDOM..=API_KEY_MAX_RANDOM).contains(&random.len())
            && random
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    })
}

/// One API key. Exactly one of `readers` and `user` is set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApiKeyConfig {
    /// Unique ID of the key, as source IDs are. It names the key in this file
    /// only; it is never logged, and the published artifacts hold its hash.
    #[schemars(regex(pattern = SOURCE_ID_PATTERN))]
    pub name: String,
    /// The bearer secret, `{ env = "VAR" }`: `fyrr_key_` followed by at least
    /// 43 characters from `[A-Za-z0-9_-]`. Only the writer reads the variable.
    pub key: SecretRef,
    /// Reader tokens (`usr:<id>`, `grp:<id>`) the key acts with, as
    /// `identity.stdio_readers` states them for the stdio subject.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub readers: Vec<String>,
    /// Instead of `readers`: act as this canonical directory user (their
    /// corporate e-mail). The key is refused while that user is absent or inactive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = API_KEY_USER_MAX_BYTES), extend("x-maxUtf8Bytes" = API_KEY_USER_MAX_BYTES))]
    pub user: Option<String>,
    /// The reader refuses the key from this instant on, an RFC 3339 timestamp
    /// with an offset (`2027-06-30T00:00:00Z`). Absent, the key does not expire.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "expiry"
    )]
    pub expires: Option<DateTime<Utc>>,
    /// Replaces `reader.http.user_requests_per_minute` for this key. Only with
    /// `readers`: a `user` key shares that user's allowance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub requests_per_minute: Option<u32>,
}

/// TOML writes a timestamp unquoted; a quoted string is accepted as well.
fn expiry<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error> {
    struct Expiry;
    const MESSAGE: &str = "an RFC 3339 timestamp with an offset, e.g. 2027-06-30T00:00:00Z";
    impl<'de> Visitor<'de> for Expiry {
        type Value = Option<DateTime<Utc>>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(MESSAGE)
        }
        fn visit_str<E: de::Error>(self, text: &str) -> Result<Self::Value, E> {
            DateTime::parse_from_rfc3339(text)
                .map(|at| Some(at.with_timezone(&Utc)))
                .map_err(|_| E::custom(MESSAGE))
        }
        fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
            let datetime =
                toml::value::Datetime::deserialize(de::value::MapAccessDeserializer::new(map))?;
            self.visit_str(&datetime.to_string())
        }
    }
    deserializer.deserialize_any(Expiry)
}

impl LoadedConfig {
    /// Checks of the secrets' values, which need the environment. A variable
    /// that is unset is reported by [`LoadedConfig::missing_secrets`], not
    /// here. Messages carry paths only, never a value.
    pub fn api_key_secret_issues(&self, env: &dyn Environment) -> Vec<Issue> {
        let mut issues = Vec::new();
        let mut seen: Vec<(usize, Secret)> = Vec::new();
        for (index, key) in self.config.identity.api_keys.iter().enumerate() {
            let Ok(secret) = key.key.resolve(env) else {
                continue;
            };
            let path = format!("identity.api_keys[{index}].key");
            if !is_api_key_secret(secret.expose()) {
                issues.push(Issue::error(
                    path,
                    format!(
                        "must be {API_KEY_PREFIX} followed by {API_KEY_MIN_RANDOM}–{API_KEY_MAX_RANDOM} characters from [A-Za-z0-9_-]"
                    ),
                ));
            } else if let Some((other, _)) = seen.iter().find(|(_, other)| *other == secret) {
                issues.push(Issue::error(
                    path,
                    format!("holds the same secret as identity.api_keys[{other}].key"),
                ));
            }
            seen.push((index, secret));
        }
        issues
    }
}

pub(super) fn validate(config: &Config, error: &mut impl FnMut(&str, String)) {
    let keys = &config.identity.api_keys;
    if keys.len() > API_KEYS_MAX {
        error("identity.api_keys", format!("at most {API_KEYS_MAX} keys"));
    }
    let mut names = BTreeSet::new();
    let mut variables = BTreeMap::new();
    for (index, key) in keys.iter().enumerate() {
        let at = |field: &str| format!("identity.api_keys[{index}].{field}");
        if !SOURCE_ID.is_match(&key.name) {
            error(
                &at("name"),
                format!("does not match the pattern {SOURCE_ID_PATTERN}"),
            );
        } else if !names.insert(key.name.as_str()) {
            error(&at("name"), "duplicate name".into());
        }
        if let Some(first) = variables.insert(key.key.env.as_str(), index) {
            error(
                &at("key"),
                format!(
                    "uses the environment variable of identity.api_keys[{first}].key: every key needs its own secret"
                ),
            );
        }
        match (key.readers.is_empty(), &key.user) {
            (false, None) => {
                if let Err(message) = check_readers("readers", &key.readers) {
                    error(&at("readers"), message);
                }
            }
            (true, Some(user)) => {
                if user.trim().is_empty()
                    || user.len() > API_KEY_USER_MAX_BYTES
                    || user.chars().any(char::is_control)
                {
                    error(
                        &at("user"),
                        format!(
                            "must contain 1–{API_KEY_USER_MAX_BYTES} bytes without control characters"
                        ),
                    );
                }
                if key.requests_per_minute.is_some() {
                    error(
                        &at("requests_per_minute"),
                        "applies only to a key with readers: a user key shares that user's allowance".into(),
                    );
                }
            }
            _ => error(
                &at("readers"),
                "set exactly one of readers (non-empty) and user".into(),
            ),
        }
        if key.requests_per_minute == Some(0) {
            error(&at("requests_per_minute"), "must be at least 1".into());
        }
    }
}

/// An expired key is a valid way to retire one, so it only warns.
pub(super) fn warnings(config: &Config, issues: &mut Vec<Issue>) {
    let now = crate::clock::now();
    for (index, key) in config.identity.api_keys.iter().enumerate() {
        if key.expires.is_some_and(|at| at <= now) {
            issues.push(Issue::warning(
                format!("identity.api_keys[{index}].expires"),
                "already passed: the reader refuses this key",
            ));
        }
    }
}
