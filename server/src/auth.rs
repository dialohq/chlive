//! Who a connection is (the `[auth]` section): nobody (`none`, the default: connections are
//! anonymous), whoever its `hello` says (`declared`, for dev and tests), or the subject of a
//! credential it presents (`jwt`): a JWT signed by the issuer, verified with the issuer's public
//! keys only, so that chlive can check credentials but never make them.
//!
//! The keys are a JWKS, from a file, a URL, or both (their union): read at start (which fails
//! without them), again every `jwks_refresh`, and when a credential names a key id that isn't
//! among them (at most every `jwks_cooldown`), so a rotation needs no restart. A source that
//! fails to load keeps its previous keys.
//!
//! What a client hears of a failure is one of four codes with a fixed message
//! ([`AuthError`]): the signature is checked before any claim, and nothing of the credential
//! or of what the server expected is said.

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::jwk::{AlgorithmParameters, Jwk};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::protocol::{AUTH_BAD_SIGNATURE, AUTH_EXPIRED, AUTH_WRONG_AUDIENCE};

/// `[auth]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AuthConfig {
    pub mode: Mode,
    /// jwt: the credentials' `iss`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issuer: Option<String>,
    /// jwt: what their `aud` must hold.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audience: Option<String>,
    /// jwt: the issuer's public keys (a JWKS), over HTTP(S) ...
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jwks_url: Option<String>,
    /// ... and/or in a file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jwks_file: Option<PathBuf>,
    /// How often the keys are read again.
    #[serde(with = "humantime_serde")]
    pub jwks_refresh: Duration,
    /// A credential with a key id the server doesn't have reads them again, at most this often.
    #[serde(with = "humantime_serde")]
    pub jwks_cooldown: Duration,
    /// The signing algorithms taken; only public-key ones (HS* would need the issuer's secret).
    pub algorithms: Vec<String>,
    /// The claim holding the subject (`{subject:String}` in grants), taken as an opaque string.
    pub subject_claim: String,
    /// Clock skew allowed on `exp` and `nbf`.
    #[serde(with = "humantime_serde")]
    pub leeway: Duration,
    /// jwt: a connection without a `hello` this long after connecting is closed.
    #[serde(with = "humantime_serde")]
    pub hello_timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Connections are anonymous; a `hello` is refused.
    #[default]
    None,
    /// `hello {user}` is trusted: for dev and tests only.
    Declared,
    /// `hello {token}`: a JWT, verified.
    Jwt,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            mode: Mode::None,
            issuer: None,
            audience: None,
            jwks_url: None,
            jwks_file: None,
            jwks_refresh: Duration::from_secs(300),
            jwks_cooldown: Duration::from_secs(30),
            algorithms: [
                "RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "EdDSA",
            ]
            .map(String::from)
            .to_vec(),
            subject_claim: "sub".into(),
            leeway: Duration::from_secs(5),
            hello_timeout: Duration::from_secs(10),
        }
    }
}

impl AuthConfig {
    /// What can be checked without the keys.
    pub fn check(&self) -> Result<()> {
        self.algorithms()?;
        if self.subject_claim.is_empty() {
            bail!("auth.subject_claim: empty");
        }
        if self.mode != Mode::Jwt {
            return Ok(());
        }
        if self.issuer.as_deref().unwrap_or_default().is_empty() {
            bail!("auth.issuer: mode = \"jwt\" needs the credentials' issuer");
        }
        if self.audience.as_deref().unwrap_or_default().is_empty() {
            bail!("auth.audience: mode = \"jwt\" needs the audience credentials are for");
        }
        if self.jwks_url.is_none() && self.jwks_file.is_none() {
            bail!("auth: mode = \"jwt\" needs the issuer's keys: jwks_url, jwks_file or both");
        }
        if let Some(url) = &self.jwks_url {
            if !(url.starts_with("https://") || url.starts_with("http://")) {
                bail!("auth.jwks_url: {url:?} isn't an http(s) URL");
            }
        }
        Ok(())
    }

    fn algorithms(&self) -> Result<Vec<Algorithm>> {
        if self.algorithms.is_empty() {
            bail!("auth.algorithms: empty");
        }
        self.algorithms
            .iter()
            .map(|a| {
                let alg = Algorithm::from_str(a)
                    .map_err(|_| anyhow::anyhow!("auth.algorithms: unknown algorithm {a:?}"))?;
                if matches!(alg, Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512) {
                    bail!(
                        "auth.algorithms: {a} is a shared secret, which would let chlive make \
                         credentials too; it only verifies them (public-key algorithms only)"
                    );
                }
                Ok(alg)
            })
            .collect()
    }
}

/// How the server tells who a connection is.
pub enum Auth {
    None,
    Declared,
    Jwt(Arc<Verifier>),
}

impl std::fmt::Debug for Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Auth::None => "None",
            Auth::Declared => "Declared",
            Auth::Jwt(_) => "Jwt",
        })
    }
}

/// The same mode (and, verifying, the same verifier).
impl PartialEq for Auth {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Auth::None, Auth::None) | (Auth::Declared, Auth::Declared) => true,
            (Auth::Jwt(a), Auth::Jwt(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

impl Auth {
    /// For `config` (its keys loaded, and kept fresh in the background: needs a runtime).
    pub async fn start(config: &AuthConfig) -> Result<Self> {
        config.check()?;
        Ok(match config.mode {
            Mode::None => Auth::None,
            Mode::Declared => Auth::Declared,
            Mode::Jwt => Auth::Jwt(Verifier::start(config).await?),
        })
    }
}

/// A verified credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verified {
    pub subject: String,
    /// Unix milliseconds.
    pub expires_at: i64,
}

/// Why a credential isn't taken: one of the protocol's codes, each with a fixed message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthError {
    /// Malformed, signed by no key the server has, or with an algorithm it doesn't take.
    BadSignature,
    /// Not for this server: its issuer or audience differs, or a claim the server needs (the
    /// subject, the expiry) is missing.
    WrongAudience,
    /// Past its `exp` (or before its `nbf`).
    Expired,
}

impl AuthError {
    pub fn code(self) -> &'static str {
        match self {
            AuthError::BadSignature => AUTH_BAD_SIGNATURE,
            AuthError::WrongAudience => AUTH_WRONG_AUDIENCE,
            AuthError::Expired => AUTH_EXPIRED,
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            AuthError::BadSignature => "the credential's signature can't be verified",
            AuthError::WrongAudience => "the credential isn't for this server",
            AuthError::Expired => "the credential has expired",
        }
    }

    fn from_kind(kind: &ErrorKind) -> Self {
        match kind {
            ErrorKind::ExpiredSignature | ErrorKind::ImmatureSignature => AuthError::Expired,
            ErrorKind::InvalidIssuer
            | ErrorKind::InvalidAudience
            | ErrorKind::InvalidSubject
            | ErrorKind::MissingRequiredClaim(_)
            | ErrorKind::InvalidClaimFormat(_) => AuthError::WrongAudience,
            // malformed, the signature, the key, the algorithm
            _ => AuthError::BadSignature,
        }
    }

    /// Whether another key might verify it (the signature failed, not the claims).
    fn signature(kind: &ErrorKind) -> bool {
        Self::from_kind(kind) == AuthError::BadSignature
    }
}

/// One of the issuer's keys.
#[derive(Clone)]
struct Key {
    kid: Option<String>,
    /// The algorithm the JWK says it's for, if it does.
    alg: Option<Algorithm>,
    key: DecodingKey,
}

/// The keys from each source; a source that fails to reload keeps its own.
#[derive(Clone, Default)]
struct Keys {
    file: Arc<[Key]>,
    url: Arc<[Key]>,
}

impl Keys {
    fn all(&self) -> impl Iterator<Item = &Key> {
        self.file.iter().chain(self.url.iter())
    }
}

/// Verifies credentials with the issuer's public keys.
pub struct Verifier {
    validation: Validation,
    subject_claim: String,
    /// Kept apart from `validation.leeway` (seconds) for the deadline.
    leeway: Duration,
    pub hello_timeout: Duration,
    jwks_url: Option<String>,
    jwks_file: Option<PathBuf>,
    cooldown: Duration,
    http: reqwest::Client,
    keys: RwLock<Keys>,
    /// When the keys were last read (one reload at a time).
    loaded_at: tokio::sync::Mutex<Instant>,
}

impl Verifier {
    /// Load the keys (failing without any), then reload them every `jwks_refresh`.
    pub async fn start(config: &AuthConfig) -> Result<Arc<Self>> {
        config.check()?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.algorithms = config.algorithms()?;
        validation.set_issuer(&[config.issuer.as_deref().unwrap_or_default()]);
        validation.set_audience(&[config.audience.as_deref().unwrap_or_default()]);
        validation.set_required_spec_claims(&["exp", "iss", "aud"]);
        validation.validate_nbf = true;
        validation.leeway = config.leeway.as_secs();
        let verifier = Arc::new(Self {
            validation,
            subject_claim: config.subject_claim.clone(),
            leeway: config.leeway,
            hello_timeout: config.hello_timeout,
            jwks_url: config.jwks_url.clone(),
            jwks_file: config.jwks_file.clone(),
            cooldown: config.jwks_cooldown,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()?,
            keys: RwLock::default(),
            loaded_at: tokio::sync::Mutex::new(Instant::now()),
        });
        let mut keys = Keys::default();
        if let Some(file) = &verifier.jwks_file {
            keys.file = verifier
                .read_file(file)
                .with_context(|| format!("auth.jwks_file: {}", file.display()))?;
        }
        if let Some(url) = &verifier.jwks_url {
            keys.url = verifier
                .fetch(url)
                .await
                .with_context(|| format!("auth.jwks_url: {url}"))?;
        }
        if keys.all().next().is_none() {
            bail!("auth: the issuer's JWKS has no keys");
        }
        info!(
            file = keys.file.len(),
            url = keys.url.len(),
            "auth: keys loaded"
        );
        *verifier.keys.write().unwrap() = keys;

        let weak = Arc::downgrade(&verifier);
        let refresh = config.jwks_refresh;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(refresh).await;
                let Some(v) = weak.upgrade() else { return };
                let mut loaded_at = v.loaded_at.lock().await;
                v.reload().await;
                *loaded_at = Instant::now();
            }
        });
        Ok(verifier)
    }

    /// The credential's subject and expiry, or why it isn't taken.
    pub async fn verify(&self, token: &str) -> Result<Verified, AuthError> {
        let header = jsonwebtoken::decode_header(token).map_err(|_| AuthError::BadSignature)?;
        if !self.validation.algorithms.contains(&header.alg) {
            return Err(AuthError::BadSignature);
        }
        // a key id the server doesn't have: the issuer may have rotated
        if let Some(kid) = &header.kid {
            if !self.has(kid) {
                self.reload_soon().await;
            }
        }
        let keys = self.keys.read().unwrap().clone();
        let candidates = keys.all().filter(|k| {
            header
                .kid
                .as_ref()
                .is_none_or(|kid| k.kid.as_ref() == Some(kid))
                && k.alg.is_none_or(|a| a == header.alg)
        });
        // jsonwebtoken wants every algorithm of a validation in the key's family: this one's
        let mut validation = self.validation.clone();
        validation.algorithms = vec![header.alg];
        let mut verdict = Err(AuthError::BadSignature);
        for k in candidates {
            match jsonwebtoken::decode::<serde_json::Map<String, serde_json::Value>>(
                token,
                &k.key,
                &validation,
            ) {
                Ok(data) => {
                    verdict = self.verified(&data.claims);
                    break;
                }
                // another key (without a kid, any of the algorithm's) may verify it
                Err(e) if AuthError::signature(e.kind()) => continue,
                Err(e) => {
                    verdict = Err(AuthError::from_kind(e.kind()));
                    break;
                }
            }
        }
        verdict
    }

    /// When a connection with this credential must have renewed it.
    pub fn deadline(&self, v: &Verified) -> Instant {
        let left = v.expires_at - now_ms();
        Instant::now() + Duration::from_millis(left.max(0) as u64) + self.leeway
    }

    fn verified(
        &self,
        claims: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<Verified, AuthError> {
        let subject = claims
            .get(&self.subject_claim)
            .and_then(|s| s.as_str())
            .filter(|s| !s.is_empty())
            .ok_or(AuthError::WrongAudience)?;
        let exp = claims
            .get("exp")
            .and_then(|e| e.as_f64())
            .ok_or(AuthError::WrongAudience)?;
        Ok(Verified {
            subject: subject.to_string(),
            expires_at: (exp * 1000.0) as i64,
        })
    }

    fn has(&self, kid: &str) -> bool {
        self.keys
            .read()
            .unwrap()
            .all()
            .any(|k| k.kid.as_deref() == Some(kid))
    }

    /// Read the keys again, unless they were within the cooldown (or another reload is on).
    async fn reload_soon(&self) {
        let Ok(mut loaded_at) = self.loaded_at.try_lock() else {
            // one is on: wait for it
            let _ = self.loaded_at.lock().await;
            return;
        };
        if loaded_at.elapsed() < self.cooldown {
            return;
        }
        self.reload().await;
        *loaded_at = Instant::now();
    }

    /// Each source again; one that fails keeps its keys.
    async fn reload(&self) {
        let mut keys = self.keys.read().unwrap().clone();
        if let Some(file) = &self.jwks_file {
            match self.read_file(file) {
                Ok(k) => keys.file = k,
                Err(e) => {
                    warn!(file = %file.display(), error = %format!("{e:#}"), "auth: reading the keys failed; keeping the ones loaded")
                }
            }
        }
        if let Some(url) = &self.jwks_url {
            match self.fetch(url).await {
                Ok(k) => keys.url = k,
                Err(e) => {
                    warn!(url, error = %format!("{e:#}"), "auth: fetching the keys failed; keeping the ones loaded")
                }
            }
        }
        *self.keys.write().unwrap() = keys;
    }

    fn read_file(&self, file: &PathBuf) -> Result<Arc<[Key]>> {
        let text = std::fs::read_to_string(file)?;
        parse_jwks(&text)
    }

    async fn fetch(&self, url: &str) -> Result<Arc<[Key]>> {
        let text = self
            .http
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        parse_jwks(&text)
    }
}

/// A JWKS's keys: public keys only (a shared secret, `"kty": "oct"`, is refused); keys of a
/// kind the server can't use are skipped.
fn parse_jwks(text: &str) -> Result<Arc<[Key]>> {
    #[derive(Deserialize)]
    struct Set {
        keys: Vec<serde_json::Value>,
    }
    let set: Set = serde_json::from_str(text).context("not a JWKS ({\"keys\": [...]})")?;
    let mut keys = Vec::new();
    for value in set.keys {
        if value.get("kty").and_then(|k| k.as_str()) == Some("oct") {
            bail!("a shared-secret key (\"kty\": \"oct\"): chlive only takes the issuer's public keys");
        }
        let jwk: Jwk = match serde_json::from_value(value) {
            Ok(j) => j,
            Err(e) => {
                warn!(error = %e, "auth: skipping a key that isn't a JWK chlive can use");
                continue;
            }
        };
        if matches!(jwk.algorithm, AlgorithmParameters::OctetKey(_)) {
            bail!("a shared-secret key: chlive only takes the issuer's public keys");
        }
        let alg = jwk
            .common
            .key_algorithm
            .and_then(|a| Algorithm::from_str(&a.to_string()).ok());
        let key = match DecodingKey::from_jwk(&jwk) {
            Ok(k) => k,
            Err(e) => {
                warn!(kid = jwk.common.key_id, error = %e, "auth: skipping a key chlive can't use");
                continue;
            }
        };
        keys.push(Key {
            kid: jwk.common.key_id.clone(),
            alg,
            key,
        });
    }
    Ok(keys.into())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jwt_needs_an_issuer_an_audience_and_keys() {
        let jwt = |f: fn(&mut AuthConfig)| {
            let mut c = AuthConfig {
                mode: Mode::Jwt,
                issuer: Some("https://auth.example".into()),
                audience: Some("chlive".into()),
                jwks_file: Some("jwks.json".into()),
                ..Default::default()
            };
            f(&mut c);
            c.check().map_err(|e| e.to_string())
        };
        assert!(jwt(|_| {}).is_ok());
        assert!(jwt(|c| c.issuer = None)
            .unwrap_err()
            .contains("auth.issuer"));
        assert!(jwt(|c| c.audience = None)
            .unwrap_err()
            .contains("auth.audience"));
        assert!(jwt(|c| c.jwks_file = None)
            .unwrap_err()
            .contains("jwks_url, jwks_file or both"));
        assert!(jwt(|c| c.jwks_url = Some("ftp://x".into()))
            .unwrap_err()
            .contains("http(s)"));
        assert!(jwt(|c| c.algorithms = vec!["ES256".into(), "HS256".into()])
            .unwrap_err()
            .contains("shared secret"));
        assert!(jwt(|c| c.algorithms = vec!["XX1".into()])
            .unwrap_err()
            .contains("unknown algorithm"));
        // the other modes need none of it
        assert!(AuthConfig::default().check().is_ok());
    }

    #[test]
    fn shared_secrets_are_refused_and_unusable_keys_skipped() {
        let Err(e) = parse_jwks(r#"{"keys": [{"kty": "oct", "k": "c2VjcmV0"}]}"#) else {
            panic!("a shared secret taken")
        };
        assert!(e.to_string().contains("shared-secret"), "{e}");
        let keys = parse_jwks(
            r#"{"keys": [
                {"kty": "OKP", "crv": "Ed25519", "kid": "k1", "alg": "EdDSA",
                 "x": "11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo"},
                {"kty": "nope"}
            ]}"#,
        )
        .unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].kid.as_deref(), Some("k1"));
        assert_eq!(keys[0].alg, Some(Algorithm::EdDSA));
        assert!(parse_jwks("[]").is_err());
    }

    #[test]
    fn failures_map_to_four_codes() {
        use ErrorKind as K;
        for (kind, code) in [
            (K::InvalidSignature, AUTH_BAD_SIGNATURE),
            (K::InvalidToken, AUTH_BAD_SIGNATURE),
            (K::InvalidAlgorithm, AUTH_BAD_SIGNATURE),
            (K::InvalidKeyFormat, AUTH_BAD_SIGNATURE),
            (K::InvalidIssuer, AUTH_WRONG_AUDIENCE),
            (K::InvalidAudience, AUTH_WRONG_AUDIENCE),
            (K::MissingRequiredClaim("exp".into()), AUTH_WRONG_AUDIENCE),
            (K::ExpiredSignature, AUTH_EXPIRED),
            (K::ImmatureSignature, AUTH_EXPIRED),
        ] {
            assert_eq!(AuthError::from_kind(&kind).code(), code, "{kind:?}");
        }
    }
}
