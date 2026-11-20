//! The identity artifact: one published, versioned
//! snapshot per namespace that maps each canonical user to reader tokens. The
//! reader only looks callers up in it, never calls a provider at query time.
//! Principal keys and tokens are hashed; the artifact is a token dictionary
//! with compact per-user index lists, published as full snapshots.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::limits::{check_digest, check_id};
use crate::version::{Contract, SchemaVersion};

/// The identity artifact contract version written by this build.
pub const IDENTITY_CONTRACT_VERSION: SchemaVersion = SchemaVersion::new(1, 2);

/// One identity artifact generation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct IdentityArtifact {
    pub schema_version: SchemaVersion,
    pub namespace: String,
    pub generation: u64,
    pub built_at: DateTime<Utc>,
    /// How principal keys and tokens were hashed.
    pub hashing: HashingScheme,
    /// Hashed reader tokens (`tok:<64 lowercase hex>`, the HMAC-SHA256 of the
    /// raw token); principals refer to them by index.
    pub tokens: Vec<String>,
    pub principals: Vec<Principal>,
    /// The immutable inputs this generation was built from.
    pub inputs: Vec<IdentityInput>,
    /// Hashed issuer/subject bindings; never taken from unverified token claims.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subjects: Vec<SubjectBinding>,
}

/// Keyed hashing of principal keys and tokens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HashingScheme {
    /// `hmac-sha256`.
    pub algorithm: String,
    /// Identifier of the namespace hashing key (never the key itself).
    pub key_id: String,
}

/// A canonical user and the indexes of its tokens in `tokens`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Principal {
    /// Hashed canonical user key (`key:<64 lowercase hex>`, the HMAC-SHA256 of
    /// e.g. the corporate e-mail).
    pub key: String,
    pub tokens: Vec<u32>,
    /// Group memberships, as indexes into the same token dictionary.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<u32>,
}

/// A hashed subject mapped to a principal: an issuer-scoped OIDC subject, or
/// an API-key secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SubjectBinding {
    pub subject: String,
    pub principal: String,
    /// From this instant on the binding admits nobody. Absent, it does not
    /// expire. Readers older than contract 1.2 ignore it, but they never derive
    /// the subject of an API key either.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires: Option<DateTime<Utc>>,
}

/// One input snapshot of the build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct IdentityInput {
    /// `source` (a connector principal snapshot) or `directory`.
    pub kind: IdentityInputKind,
    /// Source ID, or the directory provider name.
    pub id: String,
    /// Object key of the immutable snapshot, relative to the namespace root.
    pub object: String,
    pub digest: String,
    pub read_at: DateTime<Utc>,
}

/// The origin of an identity input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum IdentityInputKind {
    Source,
    Directory,
}

impl IdentityArtifact {
    /// The principal a subject is bound to at `now`; an expired binding admits nobody.
    pub fn principal_for_subject(&self, subject: &str, now: DateTime<Utc>) -> Option<&Principal> {
        let binding = self.subjects.iter().find(|b| b.subject == subject)?;
        if binding.expires.is_some_and(|expires| expires <= now) {
            return None;
        }
        self.principals.iter().find(|p| p.key == binding.principal)
    }
}

impl Contract for IdentityArtifact {
    const NAME: &'static str = "identity-artifact";
    const VERSION: SchemaVersion = IDENTITY_CONTRACT_VERSION;

    fn validate(&self) -> Result<(), String> {
        check_id("namespace", &self.namespace)?;
        check_id("hashing.key_id", &self.hashing.key_id)?;
        if self.hashing.algorithm != "hmac-sha256" {
            return Err("hashing.algorithm must be hmac-sha256".into());
        }
        let mut tokens = BTreeSet::new();
        for (index, token) in self.tokens.iter().enumerate() {
            if !is_hashed(token, TOKEN_PREFIX) {
                return Err(format!(
                    "tokens[{index}] is not a hashed token ({TOKEN_PREFIX}<64 hex>)"
                ));
            }
            if !tokens.insert(token) {
                return Err("tokens has duplicates".into());
            }
        }
        let mut keys = BTreeSet::new();
        for (index, principal) in self.principals.iter().enumerate() {
            if !is_hashed(&principal.key, KEY_PREFIX) {
                return Err(format!(
                    "principals[{index}].key is not a hashed key ({KEY_PREFIX}<64 hex>)"
                ));
            }
            if !keys.insert(&principal.key) {
                return Err(format!("principals[{index}]: duplicate key"));
            }
            if principal
                .tokens
                .iter()
                .any(|token| *token as usize >= self.tokens.len())
            {
                return Err(format!("principals[{index}]: token index out of range"));
            }
        }
        let mut subjects = BTreeSet::new();
        for binding in &self.subjects {
            if !is_hashed(&binding.subject, KEY_PREFIX)
                || !keys.contains(&binding.principal)
                || !subjects.insert(&binding.subject)
            {
                return Err("subject binding invalid".into());
            }
        }
        for principal in &self.principals {
            let groups: BTreeSet<_> = principal.groups.iter().collect();
            let tokens: BTreeSet<_> = principal.tokens.iter().collect();
            if groups.len() != principal.groups.len()
                || tokens.len() != principal.tokens.len()
                || !groups.is_subset(&tokens)
            {
                return Err("principal group membership invalid".into());
            }
        }
        for input in &self.inputs {
            check_digest("inputs[].digest", &input.digest)?;
        }
        Ok(())
    }
}

/// Prefix of a hashed reader token.
pub const TOKEN_PREFIX: &str = "tok:";
/// Prefix of a hashed principal key.
pub const KEY_PREFIX: &str = "key:";

/// `<prefix><64 lowercase hex>`: the artifact never holds raw identities or
/// tokens, so `usr:…`, `grp:…` or an e-mail is refused.
pub(crate) fn is_hashed(value: &str, prefix: &str) -> bool {
    crate::limits::prefixed_hex(value, prefix, 64)
}
