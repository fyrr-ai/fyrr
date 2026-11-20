//! API keys in the identity artifact: a subject binding per key, and for a key
//! with `readers` its own principal.
//!
//! Only keyed hashes are published. The subject is the HMAC of the secret in
//! its own MAC domain, the principal of a `readers` key the HMAC of its name in
//! another; see `fyrr_reader::storage::api_key_subject`.
use crate::membership::{UserTokens, operator_grants};
use crate::{Result, WriterError};
use fyrr_core::config::{Config, Environment, ProcessEnvironment, is_api_key_secret};
use fyrr_core::directory::DirectorySnapshot;
use fyrr_core::identity::{IdentityArtifact, SubjectBinding};
use fyrr_reader::storage::{api_key_principal, api_key_subject, hash_principal};
use std::collections::BTreeSet;

/// The hash of a configured key's secret, or `None` when the variable is unset
/// or holds something that is no API-key secret.
fn subject(
    config: &Config,
    env: &dyn Environment,
    key: &fyrr_core::config::ApiKeyConfig,
) -> Result<Option<String>> {
    let Ok(secret) = key.key.resolve(env) else {
        return Ok(None);
    };
    if !is_api_key_secret(secret.expose()) {
        return Ok(None);
    }
    Ok(Some(api_key_subject(config, secret.expose())?))
}

/// Bindings for every configured key, and the grants of the `readers` keys.
///
/// A key whose user is absent or inactive gets no binding: the reader refuses
/// it, and `writer status` says so. A secret that is missing, malformed or
/// shared is a deployment error and fails the build, because publishing the
/// other keys without it would hide the mistake.
pub(crate) fn bindings(
    config: &Config,
    env: &dyn Environment,
    directory: Option<&DirectorySnapshot>,
    grants: &mut UserTokens,
) -> Result<Vec<SubjectBinding>> {
    let mut subjects = BTreeSet::new();
    let mut names = BTreeSet::new();
    let mut bound = Vec::new();
    for key in &config.identity.api_keys {
        let secret = key
            .key
            .resolve(env)
            .map_err(|_| WriterError::CredentialUnavailable)?;
        if !is_api_key_secret(secret.expose()) || !names.insert(key.name.as_str()) {
            return Err(WriterError::ConfigurationInvalid);
        }
        let subject = api_key_subject(config, secret.expose())?;
        if !subjects.insert(subject.clone()) {
            return Err(WriterError::ConfigurationInvalid);
        }
        let principal = if let Some(user) = &key.user {
            let principal = hash_principal(config, "key", &user.trim().to_lowercase())?;
            let active = directory.is_some_and(|snapshot| {
                snapshot
                    .users
                    .iter()
                    .any(|user| user.active && user.key == principal)
            });
            if !active || !grants.contains_key(&principal) {
                continue;
            }
            principal
        } else {
            let principal = api_key_principal(config, &key.name)?;
            grants
                .entry(principal.clone())
                .or_default()
                .extend(operator_grants(config, &key.readers)?);
            principal
        };
        bound.push(SubjectBinding {
            subject,
            principal,
            expires: key.expires,
        });
    }
    Ok(bound)
}

/// Value-free `writer status` warnings about configured keys the published
/// identity does not admit.
pub(crate) fn status_warnings(
    config: &Config,
    identity: &IdentityArtifact,
) -> Result<Vec<&'static str>> {
    let now = fyrr_core::clock::now();
    let mut warnings = BTreeSet::new();
    for key in &config.identity.api_keys {
        let Some(subject) = subject(config, &ProcessEnvironment, key)? else {
            warnings.insert("api_key_secret_unusable");
            continue;
        };
        match identity.subjects.iter().find(|b| b.subject == subject) {
            None => warnings.insert("api_key_unbound"),
            Some(binding) if binding.expires.is_some_and(|at| at <= now) => {
                warnings.insert("api_key_expired")
            }
            Some(_) => false,
        };
    }
    Ok(warnings.into_iter().collect())
}
