//! Who a connection is (the `[auth]` section): nobody (`none`, the default: connections are
//! anonymous), whoever its `hello` says (`declared`, for dev and tests), or the subject of a
//! credential it presents (`jwt`): a JWT signed by one of the issuers (`[[auth.issuers]]`),
//! verified with that issuer's public keys only, so that chlive can check credentials but never
//! make them.
//!
//! A credential's `iss` picks its issuer, and only that issuer's keys may verify it: a key of
//! one issuer never validates another's credential. Each issuer has its audiences (any one of
//! them in `aud` is enough), its algorithms, the claim its subject is in (with a prefix, so that
//! issuers' subjects can't collide), and claims that must have exact values.
//!
//! Each issuer's keys are a JWKS, from a file, a URL, or both (their union): read at start
//! (which fails without them), again every `jwks_refresh`, and when a credential names a key id
//! that isn't among them (at most every `jwks_cooldown`), so a rotation needs no restart. A
//! source that fails to load keeps its previous keys.
//!
//! What a client hears of a failure is one of four codes with a fixed message
//! ([`AuthError`]): past the choice of issuer, the signature is checked before any claim, and
//! nothing of the credential or of what the server expected is said.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::jwk::{AlgorithmParameters, Jwk};
use jsonwebtoken::{Algorithm, DecodingKey, Header, Validation};
use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::protocol::{AUTH_BAD_SIGNATURE, AUTH_EXPIRED, AUTH_WRONG_AUDIENCE};

/// `[auth]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawAuthConfig")]
pub struct AuthConfig {
    pub mode: Mode,
    /// jwt: who signs credentials, each with its own keys and claims.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub issuers: Vec<IssuerConfig>,
    /// How often the keys are read again.
    #[serde(with = "humantime_serde")]
    pub jwks_refresh: Duration,
    /// A credential with a key id its issuer's keys don't have reads them again, at most this
    /// often.
    #[serde(with = "humantime_serde")]
    pub jwks_cooldown: Duration,
    /// Clock skew allowed on `exp` and `nbf`.
    #[serde(with = "humantime_serde")]
    pub leeway: Duration,
    /// jwt: a connection without a `hello` this long after connecting is closed.
    #[serde(with = "humantime_serde")]
    pub hello_timeout: Duration,
}

/// `[[auth.issuers]]`: one issuer of credentials.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IssuerConfig {
    /// The credentials' `iss`, exactly: it picks the issuer, so each is listed once.
    pub issuer: String,
    /// What their `aud` must hold one of.
    pub audiences: Vec<String>,
    /// The issuer's public keys (a JWKS), over HTTP(S) ...
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_url: Option<String>,
    /// ... and/or in a file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_file: Option<PathBuf>,
    /// The signing algorithms taken; only public-key ones (HS* would need the issuer's secret).
    #[serde(default = "default_algorithms")]
    pub algorithms: Vec<String>,
    /// The claim holding the subject (`{subject:String}` in grants), a string taken as is ...
    #[serde(default = "default_subject_claim")]
    pub subject_claim: String,
    /// ... after this prefix (e.g. `user:`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub subject_prefix: String,
    /// Claims that must have exactly these values (e.g. `email_verified = true`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub required_claims: BTreeMap<String, serde_json::Value>,
}

fn default_algorithms() -> Vec<String> {
    [
        "RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "EdDSA",
    ]
    .map(String::from)
    .to_vec()
}

fn default_subject_claim() -> String {
    "sub".into()
}

impl Default for IssuerConfig {
    /// No issuer yet: the defaults of the rest.
    fn default() -> Self {
        Self {
            issuer: String::new(),
            audiences: Vec::new(),
            jwks_url: None,
            jwks_file: None,
            algorithms: default_algorithms(),
            subject_claim: default_subject_claim(),
            subject_prefix: String::new(),
            required_claims: BTreeMap::new(),
        }
    }
}

/// `[auth]` as written, with the single issuer's keys of before `[[auth.issuers]]` (refused,
/// saying where they went).
#[derive(Deserialize)]
#[serde(deny_unknown_fields, default)]
struct RawAuthConfig {
    mode: Mode,
    issuers: Vec<IssuerConfig>,
    #[serde(with = "humantime_serde")]
    jwks_refresh: Duration,
    #[serde(with = "humantime_serde")]
    jwks_cooldown: Duration,
    #[serde(with = "humantime_serde")]
    leeway: Duration,
    #[serde(with = "humantime_serde")]
    hello_timeout: Duration,
    issuer: Option<IgnoredAny>,
    audience: Option<IgnoredAny>,
    jwks_url: Option<IgnoredAny>,
    jwks_file: Option<IgnoredAny>,
    algorithms: Option<IgnoredAny>,
    subject_claim: Option<IgnoredAny>,
}

impl Default for RawAuthConfig {
    fn default() -> Self {
        let d = AuthConfig::default();
        Self {
            mode: d.mode,
            issuers: d.issuers,
            jwks_refresh: d.jwks_refresh,
            jwks_cooldown: d.jwks_cooldown,
            leeway: d.leeway,
            hello_timeout: d.hello_timeout,
            issuer: None,
            audience: None,
            jwks_url: None,
            jwks_file: None,
            algorithms: None,
            subject_claim: None,
        }
    }
}

impl TryFrom<RawAuthConfig> for AuthConfig {
    type Error = String;

    fn try_from(r: RawAuthConfig) -> Result<Self, String> {
        let moved = [
            ("issuer", r.issuer.is_some()),
            ("audience", r.audience.is_some()),
            ("jwks_url", r.jwks_url.is_some()),
            ("jwks_file", r.jwks_file.is_some()),
            ("algorithms", r.algorithms.is_some()),
            ("subject_claim", r.subject_claim.is_some()),
        ];
        if let Some((key, _)) = moved.iter().find(|(_, set)| *set) {
            return Err(format!(
                "auth.{key} moved: each issuer is its own [[auth.issuers]], with issuer, \
                 audiences (a list), jwks_url and/or jwks_file, algorithms and subject_claim"
            ));
        }
        Ok(Self {
            mode: r.mode,
            issuers: r.issuers,
            jwks_refresh: r.jwks_refresh,
            jwks_cooldown: r.jwks_cooldown,
            leeway: r.leeway,
            hello_timeout: r.hello_timeout,
        })
    }
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
            issuers: Vec::new(),
            jwks_refresh: Duration::from_secs(300),
            jwks_cooldown: Duration::from_secs(30),
            leeway: Duration::from_secs(5),
            hello_timeout: Duration::from_secs(10),
        }
    }
}

impl AuthConfig {
    /// What can be checked without the keys.
    pub fn check(&self) -> Result<()> {
        let mut seen = BTreeSet::new();
        for (i, issuer) in self.issuers.iter().enumerate() {
            let at = format!("auth.issuers.{i}");
            issuer.check(&at)?;
            if !seen.insert(&issuer.issuer) {
                bail!(
                    "{at}.issuer: {:?} is listed twice (a credential's iss picks its issuer)",
                    issuer.issuer
                );
            }
        }
        if self.mode == Mode::Jwt && self.issuers.is_empty() {
            bail!(
                "auth.issuers: mode = \"jwt\" needs at least one [[auth.issuers]]: who signs \
                 the credentials, and with which keys"
            );
        }
        Ok(())
    }
}

impl IssuerConfig {
    fn check(&self, at: &str) -> Result<()> {
        if self.issuer.is_empty() {
            bail!("{at}.issuer: empty (the credentials' iss)");
        }
        if self.audiences.is_empty() || self.audiences.iter().any(String::is_empty) {
            bail!("{at}.audiences: the audiences credentials are for, at least one (none empty)");
        }
        if self.jwks_url.is_none() && self.jwks_file.is_none() {
            bail!("{at}: the issuer's keys are needed: jwks_url, jwks_file or both");
        }
        if let Some(url) = &self.jwks_url {
            if !(url.starts_with("https://") || url.starts_with("http://")) {
                bail!("{at}.jwks_url: {url:?} isn't an http(s) URL");
            }
        }
        self.algorithms(at)?;
        if self.subject_claim.is_empty() {
            bail!("{at}.subject_claim: empty");
        }
        if self.required_claims.keys().any(String::is_empty) {
            bail!("{at}.required_claims: a claim without a name");
        }
        Ok(())
    }

    fn algorithms(&self, at: &str) -> Result<Vec<Algorithm>> {
        if self.algorithms.is_empty() {
            bail!("{at}.algorithms: empty");
        }
        self.algorithms
            .iter()
            .map(|a| {
                let alg = Algorithm::from_str(a)
                    .map_err(|_| anyhow::anyhow!("{at}.algorithms: unknown algorithm {a:?}"))?;
                if matches!(alg, Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512) {
                    bail!(
                        "{at}.algorithms: {a} is a shared secret, which would let chlive make \
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
    /// Malformed, signed by no key of its issuer, or with an algorithm it doesn't take.
    BadSignature,
    /// Not for this server: its issuer is none of the server's, its audience differs, a claim
    /// the server needs (the subject, the expiry) is missing, or a required claim's value
    /// differs.
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

/// Verifies credentials, each with its issuer's public keys.
pub struct Verifier {
    /// By `iss`.
    issuers: BTreeMap<String, Issuer>,
    /// Kept apart from the validations' leeway (seconds) for the deadline.
    leeway: Duration,
    pub hello_timeout: Duration,
    cooldown: Duration,
    http: reqwest::Client,
}

/// One issuer: how its credentials are validated and whose they are, and its keys.
struct Issuer {
    iss: String,
    validation: Validation,
    subject_claim: String,
    subject_prefix: String,
    required_claims: BTreeMap<String, serde_json::Value>,
    jwks_url: Option<String>,
    jwks_file: Option<PathBuf>,
    keys: RwLock<Keys>,
    /// When its keys were last read; held while they're read again.
    loaded_at: tokio::sync::Mutex<Instant>,
}

impl Verifier {
    /// Load every issuer's keys (failing without any), then reload them every `jwks_refresh`.
    pub async fn start(config: &AuthConfig) -> Result<Arc<Self>> {
        config.check()?;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;
        let mut issuers = BTreeMap::new();
        for (i, c) in config.issuers.iter().enumerate() {
            let at = format!("auth.issuers.{i}");
            let issuer = Issuer::new(c, config.leeway, &at)?;
            issuer.load(&http, &at).await?;
            issuers.insert(c.issuer.clone(), issuer);
        }
        let verifier = Arc::new(Self {
            issuers,
            leeway: config.leeway,
            hello_timeout: config.hello_timeout,
            cooldown: config.jwks_cooldown,
            http,
        });

        let weak = Arc::downgrade(&verifier);
        let refresh = config.jwks_refresh;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(refresh).await;
                let Some(v) = weak.upgrade() else { return };
                for issuer in v.issuers.values() {
                    let mut loaded_at = issuer.loaded_at.lock().await;
                    issuer.reload(&v.http).await;
                    *loaded_at = Instant::now();
                }
            }
        });
        Ok(verifier)
    }

    /// The credential's subject and expiry, or why it isn't taken.
    pub async fn verify(&self, token: &str) -> Result<Verified, AuthError> {
        let header = jsonwebtoken::decode_header(token).map_err(|_| AuthError::BadSignature)?;
        // the issuer it claims, unverified: only to choose whose keys may verify it
        let claimed: serde_json::Map<String, serde_json::Value> =
            jsonwebtoken::dangerous::insecure_decode_claims(token)
                .map_err(|_| AuthError::BadSignature)?;
        let issuer = claimed
            .get("iss")
            .and_then(|iss| iss.as_str())
            .and_then(|iss| self.issuers.get(iss))
            .ok_or(AuthError::WrongAudience)?;
        issuer.verify(token, &header, self).await
    }

    /// When a connection with this credential must have renewed it.
    pub fn deadline(&self, v: &Verified) -> Instant {
        let left = v.expires_at - now_ms();
        Instant::now() + Duration::from_millis(left.max(0) as u64) + self.leeway
    }
}

impl Issuer {
    fn new(c: &IssuerConfig, leeway: Duration, at: &str) -> Result<Self> {
        let mut validation = Validation::new(Algorithm::RS256);
        validation.algorithms = c.algorithms(at)?;
        validation.set_issuer(&[&c.issuer]);
        validation.set_audience(&c.audiences);
        validation.set_required_spec_claims(&["exp", "iss", "aud"]);
        validation.validate_nbf = true;
        validation.leeway = leeway.as_secs();
        Ok(Self {
            iss: c.issuer.clone(),
            validation,
            subject_claim: c.subject_claim.clone(),
            subject_prefix: c.subject_prefix.clone(),
            required_claims: c.required_claims.clone(),
            jwks_url: c.jwks_url.clone(),
            jwks_file: c.jwks_file.clone(),
            keys: RwLock::default(),
            loaded_at: tokio::sync::Mutex::new(Instant::now()),
        })
    }

    /// Its keys, the first time: each source must load, and together have a key.
    async fn load(&self, http: &reqwest::Client, at: &str) -> Result<()> {
        let mut keys = Keys::default();
        if let Some(file) = &self.jwks_file {
            keys.file =
                read_file(file).with_context(|| format!("{at}.jwks_file: {}", file.display()))?;
        }
        if let Some(url) = &self.jwks_url {
            keys.url = fetch(http, url)
                .await
                .with_context(|| format!("{at}.jwks_url: {url}"))?;
        }
        if keys.all().next().is_none() {
            bail!("{at}: the issuer's JWKS has no keys");
        }
        info!(
            issuer = self.iss,
            file = keys.file.len(),
            url = keys.url.len(),
            "auth: keys loaded"
        );
        *self.keys.write().unwrap() = keys;
        Ok(())
    }

    /// `token`, which claims to be this issuer's, verified with this issuer's keys only.
    async fn verify(
        &self,
        token: &str,
        header: &Header,
        verifier: &Verifier,
    ) -> Result<Verified, AuthError> {
        if !self.validation.algorithms.contains(&header.alg) {
            return Err(AuthError::BadSignature);
        }
        // a key id the server doesn't have: the issuer may have rotated
        if let Some(kid) = &header.kid {
            if !self.has(kid) {
                self.reload_soon(&verifier.http, verifier.cooldown).await;
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

    /// Whose a credential with these (verified) claims is, if it has what this issuer requires.
    fn verified(
        &self,
        claims: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<Verified, AuthError> {
        let subject = claims
            .get(&self.subject_claim)
            .and_then(|s| s.as_str())
            .filter(|s| !s.is_empty())
            .ok_or(AuthError::WrongAudience)?;
        if self
            .required_claims
            .iter()
            .any(|(claim, value)| claims.get(claim) != Some(value))
        {
            return Err(AuthError::WrongAudience);
        }
        let exp = claims
            .get("exp")
            .and_then(|e| e.as_f64())
            .ok_or(AuthError::WrongAudience)?;
        Ok(Verified {
            subject: format!("{}{subject}", self.subject_prefix),
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
    async fn reload_soon(&self, http: &reqwest::Client, cooldown: Duration) {
        let Ok(mut loaded_at) = self.loaded_at.try_lock() else {
            // one is on: wait for it
            let _ = self.loaded_at.lock().await;
            return;
        };
        if loaded_at.elapsed() < cooldown {
            return;
        }
        self.reload(http).await;
        *loaded_at = Instant::now();
    }

    /// Each source again; one that fails keeps its keys.
    async fn reload(&self, http: &reqwest::Client) {
        let mut keys = self.keys.read().unwrap().clone();
        if let Some(file) = &self.jwks_file {
            match read_file(file) {
                Ok(k) => keys.file = k,
                Err(e) => {
                    warn!(file = %file.display(), error = %format!("{e:#}"), "auth: reading the keys failed; keeping the ones loaded")
                }
            }
        }
        if let Some(url) = &self.jwks_url {
            match fetch(http, url).await {
                Ok(k) => keys.url = k,
                Err(e) => {
                    warn!(url, error = %format!("{e:#}"), "auth: fetching the keys failed; keeping the ones loaded")
                }
            }
        }
        *self.keys.write().unwrap() = keys;
    }
}

fn read_file(file: &PathBuf) -> Result<Arc<[Key]>> {
    let text = std::fs::read_to_string(file)?;
    parse_jwks(&text)
}

async fn fetch(http: &reqwest::Client, url: &str) -> Result<Arc<[Key]>> {
    let text = http
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    parse_jwks(&text)
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
    fn jwt_needs_issuers_each_with_audiences_and_keys() {
        let issuer = |iss: &str| IssuerConfig {
            issuer: iss.into(),
            audiences: vec!["chlive".into()],
            jwks_file: Some("jwks.json".into()),
            ..Default::default()
        };
        let jwt = |f: fn(&mut AuthConfig)| {
            let mut c = AuthConfig {
                mode: Mode::Jwt,
                issuers: vec![issuer("https://a.example"), issuer("https://b.example")],
                ..Default::default()
            };
            f(&mut c);
            c.check().map_err(|e| e.to_string())
        };
        assert!(jwt(|_| {}).is_ok());
        let refused = |f: fn(&mut AuthConfig), expected: &str| {
            let e = jwt(f).unwrap_err();
            assert!(e.contains(expected), "{e}");
        };
        refused(|c| c.issuers.clear(), "at least one [[auth.issuers]]");
        refused(
            |c| c.issuers[1].issuer.clear(),
            "auth.issuers.1.issuer: empty",
        );
        refused(
            |c| c.issuers[1].issuer = "https://a.example".into(),
            "auth.issuers.1.issuer: \"https://a.example\" is listed twice",
        );
        refused(
            |c| c.issuers[0].audiences.clear(),
            "auth.issuers.0.audiences",
        );
        refused(
            |c| c.issuers[0].audiences.push(String::new()),
            "auth.issuers.0.audiences",
        );
        refused(
            |c| c.issuers[0].jwks_file = None,
            "auth.issuers.0: the issuer's keys are needed: jwks_url, jwks_file or both",
        );
        refused(
            |c| c.issuers[0].jwks_url = Some("ftp://x".into()),
            "auth.issuers.0.jwks_url: \"ftp://x\" isn't an http(s) URL",
        );
        refused(
            |c| c.issuers[1].algorithms = vec!["ES256".into(), "HS256".into()],
            "auth.issuers.1.algorithms: HS256 is a shared secret",
        );
        refused(
            |c| c.issuers[0].algorithms = vec!["XX1".into()],
            "unknown algorithm",
        );
        refused(
            |c| c.issuers[0].subject_claim.clear(),
            "auth.issuers.0.subject_claim: empty",
        );
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
