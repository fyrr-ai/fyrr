//! Operator-pinned RS256 trust. Authentication errors expose only bounded codes.
use crate::error::*;
use fyrr_core::config::{Config, OidcConfig, is_api_key_secret};
use jsonwebtoken::{
    Algorithm, DecodingKey, Validation, decode, decode_header,
    jwk::{AlgorithmParameters, JwkSet, KeyAlgorithm, KeyOperations, PublicKeyUse},
};
use serde::Deserialize;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

#[derive(Clone)]
pub(crate) struct AuthenticatedSubject {
    pub key: String,
    pub expires: i64,
    pub identity: Option<fyrr_core::publication::GenerationPointer>,
    /// The subject is the hash of an API-key secret, and this is its
    /// deployment-wide hash, which the published keys name. The secret proves
    /// nothing until those keys list it and the published identity binds the
    /// subject, so admission looks it up even for requests that skip
    /// admission for OIDC subjects.
    pub api_key: Option<String>,
}
/// RFC 7519 §4.1.3: `aud` is one string or an array of strings.
#[derive(Deserialize)]
#[serde(untagged)]
enum Audience {
    One(String),
    Many(Vec<String>),
}
impl Audience {
    fn contains(&self, audience: &str) -> bool {
        match self {
            Self::One(value) => value == audience,
            Self::Many(values) => values.iter().any(|value| value == audience),
        }
    }
}
#[derive(Deserialize)]
struct Claims {
    iss: String,
    aud: Audience,
    sub: String,
    exp: i64,
    nbf: Option<i64>,
    scope: String,
    typ: Option<String>,
}
struct Trust {
    keys: Option<JwkSet>,
    fetched: Option<Instant>,
    attempted: Option<Instant>,
}
/// The subject of an API-key bearer, if its shape is right; the shape is checked
/// before anything is hashed. The subject says nothing about whether the key
/// exists: admission looks it up in the identity, so an unknown and a removed
/// key fail the same way.
pub(crate) fn api_key(config: &Config, token: &str) -> Result<AuthenticatedSubject> {
    if !is_api_key_secret(token) {
        return Err(Unauthorized);
    }
    Ok(AuthenticatedSubject {
        key: crate::storage::api_key_subject(config, token)?,
        expires: i64::MAX,
        identity: None,
        api_key: Some(crate::storage::api_key_deployment_hash(config, token)?),
    })
}
#[derive(Clone)]
pub(crate) struct Authenticator {
    config: Config,
    oidc: OidcConfig,
    client: reqwest::Client,
    trust: Arc<Mutex<Trust>>,
    refresh: Arc<Mutex<()>>,
}
impl Authenticator {
    /// Readiness needs fresh pinned trust; uptime alone is insufficient.
    pub(crate) async fn ready(&self) -> bool {
        let trust = self.trust.lock().await;
        let fresh = trust
            .fetched
            .is_some_and(|t| t.elapsed() < self.oidc.jwks_validity.as_duration());
        drop(trust);
        if !fresh {
            let _ = self.refresh().await;
        }
        self.trust
            .lock()
            .await
            .fetched
            .is_some_and(|t| t.elapsed() < self.oidc.jwks_validity.as_duration())
    }
    async fn refresh(&self) -> Result<()> {
        // Unknown keys cannot queue refreshes or block verification of current keys.
        let Ok(_refresh) = self.refresh.try_lock() else {
            return Ok(());
        };
        let mut trust = self.trust.lock().await;
        if trust
            .attempted
            .is_some_and(|t| t.elapsed() < self.oidc.jwks_refresh_interval.as_duration())
        {
            return Ok(());
        }
        trust.attempted = Some(Instant::now());
        drop(trust);
        let keys = self.fetch_keys().await?;
        let mut trust = self.trust.lock().await;
        trust.keys = Some(keys);
        trust.fetched = Some(Instant::now());
        Ok(())
    }
    pub(crate) fn new(config: &Config) -> Result<Self> {
        let oidc = config.identity.oidc.clone().ok_or(ConfigurationInvalid)?;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(3))
            .connect_timeout(Duration::from_secs(1))
            .build()
            .map_err(|_| ConfigurationInvalid)?;
        Ok(Self {
            refresh: Arc::new(Mutex::new(())),
            config: config.clone(),
            oidc,
            client,
            trust: Arc::new(Mutex::new(Trust {
                keys: None,
                fetched: None,
                attempted: None,
            })),
        })
    }
    pub(crate) async fn authenticate(&self, token: &str) -> Result<AuthenticatedSubject> {
        if token.is_empty() || token.len() > 16 * 1024 {
            return Err(Unauthorized);
        }
        let header = decode_header(token).map_err(|_| Unauthorized)?;
        let typ = if self.oidc.keycloak { "JWT" } else { "at+jwt" };
        if header.alg != Algorithm::RS256 || header.typ.as_deref() != Some(typ) {
            return Err(Unauthorized);
        }
        let kid = header
            .kid
            .as_deref()
            .filter(|k| !k.is_empty() && k.len() <= 128 && k.bytes().all(|b| b.is_ascii_graphic()))
            .ok_or(Unauthorized)?;
        let trust = self.trust.lock().await;
        let fresh = trust
            .fetched
            .is_some_and(|t| t.elapsed() < self.oidc.jwks_validity.as_duration());
        let known = trust.keys.as_ref().is_some_and(|k| {
            k.keys
                .iter()
                .any(|k| k.common.key_id.as_deref() == Some(kid))
        });
        drop(trust);
        if !fresh || !known {
            let _ = self.refresh().await;
        }
        let trust = self.trust.lock().await;
        if !trust
            .fetched
            .is_some_and(|t| t.elapsed() < self.oidc.jwks_validity.as_duration())
        {
            return Err(AuthorizationUnavailable);
        }
        let keys = trust.keys.clone().ok_or(AuthorizationUnavailable)?;
        drop(trust);
        let mut matches = keys
            .keys
            .iter()
            .filter(|k| k.common.key_id.as_deref() == Some(kid));
        let key = matches.next().ok_or(Unauthorized)?;
        if matches.next().is_some()
            || !matches!(key.algorithm, AlgorithmParameters::RSA(_))
            || key
                .common
                .key_algorithm
                .is_some_and(|a| a != KeyAlgorithm::RS256)
            || key
                .common
                .public_key_use
                .as_ref()
                .is_some_and(|u| *u != PublicKeyUse::Signature)
            || key
                .common
                .key_operations
                .as_ref()
                .is_some_and(|ops| !ops.contains(&KeyOperations::Verify))
        {
            return Err(Unauthorized);
        }
        if let AlgorithmParameters::RSA(rsa) = &key.algorithm
            && (rsa.n.len() < 342 || rsa.n.len() > 683 || rsa.e != "AQAB")
        {
            return Err(Unauthorized);
        }
        let key = DecodingKey::from_jwk(key).map_err(|_| Unauthorized)?;
        let mut validation = Validation::new(Algorithm::RS256);
        // The times are checked below on the clock every other expiry of the
        // reader uses; the library would read the system clock.
        validation.validate_exp = false;
        validation.validate_nbf = false;
        validation.set_issuer(&[self.oidc.issuer.as_str()]);
        validation.set_audience(&[self.oidc.audience.as_str()]);
        validation.set_required_spec_claims(&["exp", "sub", "iss", "aud"]);
        let claims = decode::<Claims>(token, &key, &validation)
            .map_err(|_| Unauthorized)?
            .claims;
        if !times_valid(claims.exp, claims.nbf, fyrr_core::clock::now().timestamp()) {
            return Err(Unauthorized);
        }
        if claims.iss != self.oidc.issuer
            || !claims.aud.contains(&self.oidc.audience)
            || claims.sub.is_empty()
            || claims.sub.len() > 512
            || !claims
                .scope
                .split_whitespace()
                .any(|s| s == self.oidc.scope)
            || (self.oidc.keycloak && claims.typ.as_deref() != Some("Bearer"))
        {
            return Err(Unauthorized);
        }
        let key = crate::storage::hash_principal(
            &self.config,
            "key",
            &format!(
                "oidc:{}:{}{}",
                self.oidc.issuer.len(),
                self.oidc.issuer,
                claims.sub
            ),
        )?;
        Ok(AuthenticatedSubject {
            key,
            expires: claims.exp,
            identity: None,
            api_key: None,
        })
    }
    async fn fetch_keys(&self) -> Result<JwkSet> {
        let response = self
            .client
            .get(&self.oidc.jwks_url)
            .send()
            .await
            .map_err(|_| AuthorizationUnavailable)?;
        if !response.status().is_success() {
            return Err(AuthorizationUnavailable);
        }
        let bytes = crate::provider::bounded_body(response, 256 * 1024)
            .await
            .map_err(|_| AuthorizationUnavailable)?;
        let keys: JwkSet = serde_json::from_slice(&bytes).map_err(|_| AuthorizationUnavailable)?;
        if keys.keys.is_empty() || keys.keys.len() > 16 {
            return Err(AuthorizationUnavailable);
        }
        let mut ids = std::collections::BTreeSet::new();
        for key in &keys.keys {
            let id = key
                .common
                .key_id
                .as_deref()
                .ok_or(AuthorizationUnavailable)?;
            if id.is_empty() || id.len() > 128 || !ids.insert(id) {
                return Err(AuthorizationUnavailable);
            }
        }
        Ok(keys)
    }
}

/// A token is expired when `exp <= now` (RFC 7519): the one rule for the
/// verification of a token and for every later check of its subject.
pub(crate) fn unexpired(exp: i64, now: i64) -> bool {
    exp > now
}

/// The token times at `now`, without leeway: valid before `exp`, and from
/// `nbf` on.
fn times_valid(exp: i64, nbf: Option<i64>, now: i64) -> bool {
    unexpired(exp, now) && nbf.is_none_or(|nbf| nbf <= now)
}

#[cfg(test)]
mod time_tests {
    #[test]
    fn token_times_have_no_leeway() {
        let now = 1_000;
        assert!(!super::times_valid(now, None, now));
        assert!(super::times_valid(now + 1, None, now));
        assert!(!super::times_valid(now - 1, None, now));
        assert!(super::times_valid(now + 600, Some(now), now));
        assert!(!super::times_valid(now + 600, Some(now + 1), now));
    }
}
