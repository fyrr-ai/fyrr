//! Bearer authentication: operator-pinned RS256 trust for OIDC access tokens and
//! hashed API keys. Authentication errors expose only bounded codes.
use crate::error::*;
use fyrr_core::config::{API_KEY_PREFIX, Config, OidcConfig, is_api_key_secret};
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
    /// The subject is the hash of an API-key secret. The secret proves nothing
    /// until the published identity binds it, so admission looks it up even for
    /// requests that skip it for OIDC subjects.
    pub api_key: bool,
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
    scope: String,
    typ: Option<String>,
}
struct Trust {
    keys: Option<JwkSet>,
    fetched: Option<Instant>,
    attempted: Option<Instant>,
}
/// Verifies OIDC access tokens against the pinned key set.
#[derive(Clone)]
struct OidcVerifier {
    config: Config,
    oidc: OidcConfig,
    client: reqwest::Client,
    trust: Arc<Mutex<Trust>>,
    refresh: Arc<Mutex<()>>,
}
/// Both bearer kinds: an OIDC verifier when `identity.oidc` is configured, and
/// the API-key path for every token that starts with `fyrr_key_`.
#[derive(Clone)]
pub(crate) struct Authenticator {
    config: Config,
    oidc: Option<OidcVerifier>,
}
impl Authenticator {
    pub(crate) fn new(config: &Config) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
            oidc: config
                .identity
                .oidc
                .is_some()
                .then(|| OidcVerifier::new(config))
                .transpose()?,
        })
    }
    /// Readiness needs fresh pinned trust when OIDC is configured; API keys
    /// have none to fetch.
    pub(crate) async fn ready(&self) -> bool {
        match &self.oidc {
            Some(oidc) => oidc.ready().await,
            None => true,
        }
    }
    pub(crate) async fn authenticate(&self, token: &str) -> Result<AuthenticatedSubject> {
        if token.starts_with(API_KEY_PREFIX) {
            return self.api_key(token);
        }
        match &self.oidc {
            Some(oidc) => oidc.authenticate(token).await,
            None => Err(Unauthorized),
        }
    }
    /// The shape is checked before anything is hashed. The subject says nothing
    /// about whether the key exists: admission looks it up in the identity, so
    /// an unknown, revoked and expired key fail the same way.
    fn api_key(&self, token: &str) -> Result<AuthenticatedSubject> {
        if !is_api_key_secret(token) {
            return Err(Unauthorized);
        }
        Ok(AuthenticatedSubject {
            key: crate::storage::api_key_subject(&self.config, token)?,
            // Admission replaces this with the binding's own expiry, so that
            // every later expiry check of the request sees it.
            expires: i64::MAX,
            identity: None,
            api_key: true,
        })
    }
}
impl OidcVerifier {
    /// Readiness needs fresh pinned trust; uptime alone is insufficient.
    async fn ready(&self) -> bool {
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
    fn new(config: &Config) -> Result<Self> {
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
    async fn authenticate(&self, token: &str) -> Result<AuthenticatedSubject> {
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
        validation.leeway = 0;
        validation.validate_nbf = true;
        validation.set_issuer(&[self.oidc.issuer.as_str()]);
        validation.set_audience(&[self.oidc.audience.as_str()]);
        validation.set_required_spec_claims(&["exp", "sub", "iss", "aud"]);
        let claims = decode::<Claims>(token, &key, &validation)
            .map_err(|_| Unauthorized)?
            .claims;
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
            api_key: false,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn authenticator() -> Authenticator {
        let config = fyrr_core::config::parse(concat!(
            "[namespace]\nname='acme'\n[storage]\nuri='s3://auth'\n",
            "[embedding]\nendpoint='https://embed.invalid'\n",
        ))
        .unwrap()
        .config;
        Authenticator::new(&config).unwrap()
    }

    #[tokio::test]
    async fn without_oidc_only_a_well_formed_key_gets_past_authentication() {
        let auth = authenticator();
        assert!(auth.ready().await, "API keys have no trust to fetch");
        // Not an API key, so it is an OIDC token, and there is no OIDC.
        for token in ["eyJhbGciOiJSUzI1NiJ9.e30.c2ln", "fyrr_token_x", ""] {
            assert!(matches!(auth.authenticate(token).await, Err(Unauthorized)));
        }
        // An API-key bearer of the wrong shape is refused before anything is hashed:
        // this configuration has no hashing key, which a hash would report differently.
        let random = |n: usize| {
            format!(
                "{API_KEY_PREFIX}{}",
                "aB3_Zk9Qm7".repeat(13)[..n].to_owned()
            )
        };
        for token in [
            random(0),
            random(42),
            random(129),
            format!("{}!", random(43)),
            format!("{} ", random(43)),
            // 43 characters with fewer than ten distinct ones
            format!("{API_KEY_PREFIX}{}", "a".repeat(43)),
            format!("{API_KEY_PREFIX}{}", "ab".repeat(30)),
        ] {
            assert!(matches!(auth.authenticate(&token).await, Err(Unauthorized)));
        }
        assert!(matches!(
            auth.authenticate(&random(43)).await,
            Err(AuthorizationUnavailable)
        ));
    }
}
