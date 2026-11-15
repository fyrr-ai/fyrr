//! Hashed directory inputs. Raw provider resources remain at the writer boundary.
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

use crate::identity::{HashingScheme, KEY_PREFIX, TOKEN_PREFIX, is_hashed};
use crate::{Contract, SchemaVersion};

/// Full canonical input, independently versioned from the serving artifact.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DirectorySnapshot {
    pub schema_version: SchemaVersion,
    pub namespace: String,
    pub provider: String,
    pub built_at: DateTime<Utc>,
    pub hashing: HashingScheme,
    #[schemars(length(max = 100_000))]
    pub users: Vec<DirectoryUser>,
    /// Provider and operator groups use the same membership expansion.
    #[schemars(length(max = 100_000))]
    pub groups: Vec<DirectoryGroup>,
    #[serde(default)]
    #[schemars(length(max = 100_000))]
    pub links: Vec<AccountLink>,
    /// Encrypted provisioning resources, published atomically with this snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<DirectoryStatePointer>,
}

/// Immutable encrypted resource state needed by the SCIM writer, never the reader.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DirectoryStatePointer {
    pub object: String,
    pub digest: String,
}

/// Canonical principal, its normalized e-mail aliases and issuer-scoped subjects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DirectoryUser {
    pub key: String,
    /// Hashed canonical user reader token.
    pub token: String,
    pub active: bool,
    #[serde(default)]
    #[schemars(length(max = 64))]
    pub aliases: Vec<String>,
    #[serde(default)]
    #[schemars(length(max = 64))]
    pub subjects: Vec<String>,
}

/// A hashed group token and direct canonical-user/group members.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DirectoryGroup {
    pub token: String,
    #[schemars(length(max = 100_000))]
    pub members: Vec<String>,
}

/// Explicit source account link; ambiguous links never grant access.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AccountLink {
    pub source: String,
    /// HMAC of the source's stable user ID, not its e-mail.
    pub user: String,
    pub principal: String,
}

impl Contract for DirectorySnapshot {
    const NAME: &'static str = "directory-snapshot";
    const VERSION: SchemaVersion = SchemaVersion::new(1, 1);
    fn validate(&self) -> Result<(), String> {
        crate::limits::check_id("namespace", &self.namespace)?;
        crate::limits::check_id("provider", &self.provider)?;
        crate::limits::check_id("hashing.key_id", &self.hashing.key_id)?;
        if self.hashing.algorithm != "hmac-sha256"
            || self.users.len() > 100_000
            || self.groups.len() > 100_000
            || self.links.len() > 100_000
            || self.groups.iter().map(|g| g.members.len()).sum::<usize>() > 1_000_000
            || self
                .users
                .iter()
                .map(|u| u.aliases.len() + u.subjects.len())
                .sum::<usize>()
                > 1_000_000
        {
            return Err("directory bounds or hashing invalid".into());
        }
        if let Some(state) = &self.state {
            if !state.object.starts_with("directory/state-") || state.object.contains("..") {
                return Err("directory state path invalid".into());
            }
            crate::limits::check_digest("state.digest", &state.digest)?;
        }
        let mut users = BTreeSet::new();
        let mut tokens = BTreeSet::new();
        for u in &self.users {
            if !is_hashed(&u.token, TOKEN_PREFIX)
                || !tokens.insert(&u.token)
                || !is_hashed(&u.key, KEY_PREFIX)
                || !users.insert(&u.key)
                || u.aliases.len() > 64
                || u.subjects.len() > 64
                || u.aliases
                    .iter()
                    .chain(&u.subjects)
                    .any(|v| !is_hashed(v, KEY_PREFIX))
            {
                return Err("directory user invalid".into());
            }
        }
        let mut groups = BTreeSet::new();
        for g in &self.groups {
            if !is_hashed(&g.token, TOKEN_PREFIX)
                || !tokens.insert(&g.token)
                || !groups.insert(&g.token)
                || g.members.len() > 100_000
                || g.members
                    .iter()
                    .any(|m| !is_hashed(m, KEY_PREFIX) && !is_hashed(m, TOKEN_PREFIX))
            {
                return Err("directory group invalid".into());
            }
        }
        for l in &self.links {
            crate::limits::check_id("links.source", &l.source)?;
            if !is_hashed(&l.user, KEY_PREFIX) || !is_hashed(&l.principal, KEY_PREFIX) {
                return Err("directory link invalid".into());
            }
        }
        Ok(())
    }
}
