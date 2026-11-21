//! `[[identity.api_keys]]`: operator-issued bearer secrets for HTTP MCP.
//!
//! A key is a static secret with grants the operator states in the file, like
//! the stdio subject's. The file holds an environment reference, never the
//! secret; the writer publishes only a keyed hash of it.

use super::*;

/// Every API-key secret starts with this; any other bearer is an OIDC access token.
pub const API_KEY_PREFIX: &str = "fyrr_key_";
/// Fewest characters after the prefix: 43 characters of `[A-Za-z0-9_-]` carry
/// at least 256 bits when generated randomly.
const API_KEY_MIN_RANDOM: usize = 43;

/// Whether `text` is a well-formed API-key secret: the prefix and at least 43
/// characters from `[A-Za-z0-9_-]`. The shape does not prove the secret is
/// random; generating it from a random source does.
pub fn is_api_key_secret(text: &str) -> bool {
    text.strip_prefix(API_KEY_PREFIX).is_some_and(|random| {
        random.len() >= API_KEY_MIN_RANDOM
            && random
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    })
}

/// One API key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApiKeyConfig {
    /// Unique ID of the key, as source IDs are. It names the key in this file
    /// only; the published artifacts hold its hash.
    #[schemars(regex(pattern = SOURCE_ID_PATTERN))]
    pub name: String,
    /// The bearer secret, `{ env = "VAR" }`: `fyrr_key_` followed by at least
    /// 43 characters from `[A-Za-z0-9_-]`. Only the writer reads the variable.
    pub key: SecretRef,
    /// Reader tokens (`usr:<id>`, `grp:<id>`) the key acts with, as
    /// `identity.stdio_readers` states them for the stdio subject.
    pub readers: Vec<String>,
}

impl LoadedConfig {
    /// Checks of the secrets' values, which need the environment. A variable
    /// that is unset is reported by [`LoadedConfig::missing_secrets`], not
    /// here. Messages carry paths only, never a value. A malformed secret is
    /// no secret to compare, so it takes no part in the duplicate check.
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
                        "must be {API_KEY_PREFIX} followed by at least {API_KEY_MIN_RANDOM} characters from [A-Za-z0-9_-]; generate it with `echo \"{API_KEY_PREFIX}$(openssl rand -hex 32)\"`"
                    ),
                ));
            } else if let Some((other, _)) = seen.iter().find(|(_, other)| *other == secret) {
                issues.push(Issue::error(
                    path,
                    format!("holds the same secret as identity.api_keys[{other}].key"),
                ));
            } else {
                seen.push((index, secret));
            }
        }
        issues
    }
}

pub(super) fn validate(config: &Config, error: &mut impl FnMut(&str, String)) {
    let mut names = BTreeSet::new();
    for (index, key) in config.identity.api_keys.iter().enumerate() {
        let at = |field: &str| format!("identity.api_keys[{index}].{field}");
        if !SOURCE_ID.is_match(&key.name) {
            error(
                &at("name"),
                format!("does not match the pattern {SOURCE_ID_PATTERN}"),
            );
        } else if !names.insert(key.name.as_str()) {
            error(&at("name"), "duplicate name".into());
        }
        if key.readers.is_empty() {
            error(&at("readers"), "must name at least one reader".into());
        } else if let Err(message) = check_readers("readers", &key.readers) {
            error(&at("readers"), message);
        }
    }
}

impl Config {
    /// `reader.http` is configured and nothing can authenticate its callers:
    /// neither `identity.oidc` nor an API key. The HTTP reader refuses to
    /// start then; the writer, which publishes identity for the same file, has
    /// no such need, so removing the last key stays a valid configuration.
    pub fn http_authentication_missing(&self) -> bool {
        self.reader.http.is_some()
            && self.identity.oidc.is_none()
            && self.identity.api_keys.is_empty()
    }
}
