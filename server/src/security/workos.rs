//! Permissions in WorkOS (FGA), polled: WorkOS tells nothing when a role assignment changes, so
//! every `poll` the provider asks again for each watched (grant, subject), and right away for a
//! new one.
//!
//! A subject is a WorkOS user's `external_id`, a member of the configured organization (found
//! by its `external_id` too). A grant is a permission slug, asked one of two ways:
//! - for values ([`GrantKind::Values`]): the `external_id`s of the organization's resources
//!   (its direct children) the user holds the permission on;
//! - as a check ([`GrantKind::Check`]): whether the user holds it on the organization itself
//!   (their role in the organization); yes is `[permission]`, no is `[]`.
//!
//! A subject WorkOS doesn't know (no such user, or not a member) is granted nothing. When a
//! request fails, the last answer stands (one never answered stays loading).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use futures::{stream, StreamExt};
use reqwest::{StatusCode, Url};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{watch, Notify};
use tracing::{info, warn};

use super::{GrantKind, Granted, PermissionProvider, Watchers};

/// `[security.providers.<name>]` with `kind = "workos"`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkOsConfig {
    /// The organization's `external_id`.
    pub organization: String,
    /// The environment variable holding the API key (never the key itself).
    #[serde(default = "default_api_key_env")]
    pub api_key_env: String,
    /// How often the watched subjects are asked again.
    #[serde(default = "default_poll", with = "humantime_serde")]
    pub poll: Duration,
    /// The API's base URL.
    #[serde(default = "default_url")]
    pub url: String,
}

fn default_api_key_env() -> String {
    "WORKOS_API_KEY".into()
}

fn default_poll() -> Duration {
    Duration::from_secs(5)
}

fn default_url() -> String {
    "https://api.workos.com".into()
}

impl WorkOsConfig {
    pub fn check(&self) -> Result<()> {
        if self.organization.is_empty() {
            bail!("organization: the organization's external_id");
        }
        if self.poll.is_zero() {
            bail!("poll: more than zero");
        }
        Url::parse(&self.url).context("url")?;
        Ok(())
    }
}

/// A grant: a permission slug, and how it's asked.
#[derive(Debug, Clone, PartialEq)]
pub struct Grant {
    pub permission: String,
    pub kind: GrantKind,
}

impl Grant {
    pub fn new(permission: &str, kind: GrantKind) -> Result<Self> {
        let permission = permission.trim();
        if permission.is_empty() || permission.contains(char::is_whitespace) {
            bail!("a permission slug (like `feed:read`), not {permission:?}");
        }
        Ok(Self {
            permission: permission.to_string(),
            kind,
        })
    }
}

pub struct WorkOsProvider {
    watchers: Arc<Mutex<Watchers>>,
    wake: Arc<Notify>,
}

impl PermissionProvider for WorkOsProvider {
    fn watch(&self, grant: usize, subject: &str) -> watch::Receiver<Granted> {
        let rx = self.watchers.lock().unwrap().watch(grant, subject, || None);
        self.wake.notify_one();
        rx
    }
}

impl WorkOsProvider {
    /// Poll `grants` in the background, with the API key from `api_key_env`.
    pub fn start(name: &str, config: &WorkOsConfig, grants: Vec<Grant>) -> Result<Arc<Self>> {
        let key = std::env::var(&config.api_key_env).with_context(|| {
            format!(
                "the WorkOS API key: set {} (provider {name})",
                config.api_key_env
            )
        })?;
        Self::start_with_key(name, config, key, grants)
    }

    pub fn start_with_key(
        name: &str,
        config: &WorkOsConfig,
        key: String,
        grants: Vec<Grant>,
    ) -> Result<Arc<Self>> {
        config.check()?;
        let api = Api {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()?,
            base: Url::parse(&config.url)?,
            key,
        };
        let provider = Arc::new(Self {
            watchers: Arc::default(),
            wake: Arc::default(),
        });
        let poller = Poller {
            name: name.to_string(),
            config: config.clone(),
            api,
            grants,
            watchers: provider.watchers.clone(),
            wake: provider.wake.clone(),
        };
        tokio::spawn(poller.run());
        Ok(provider)
    }
}

struct Poller {
    name: String,
    config: WorkOsConfig,
    api: Api,
    grants: Vec<Grant>,
    watchers: Arc<Mutex<Watchers>>,
    wake: Arc<Notify>,
}

impl Poller {
    async fn run(self) {
        let name = &self.name;
        let mut backoff = Duration::from_secs(1);
        let org = loop {
            match self.api.organization(&self.config.organization).await {
                Ok(org) => break org,
                Err(e) => {
                    warn!(provider = %name, error = %format!("{e:#}"), secs = backoff.as_secs(), "permissions: finding the WorkOS organization failed; trying again");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
        };
        info!(provider = %name, organization = %org.id, "permissions: polling WorkOS");
        let (this, org) = (&self, &org);
        // subject → its organization membership
        let mut members: HashMap<String, String> = HashMap::new();
        // the pairs answered at least once
        let mut answered: HashSet<(usize, String)> = HashSet::new();
        let mut all = true;
        loop {
            let pairs = {
                let mut w = self.watchers.lock().unwrap();
                w.prune();
                w.watched()
            };
            let watched: HashSet<_> = pairs.iter().cloned().collect();
            answered.retain(|p| watched.contains(p));
            members.retain(|s, _| watched.iter().any(|(_, w)| w == s));
            let pairs: Vec<_> = pairs
                .into_iter()
                .filter(|p| all || !answered.contains(p))
                .collect();

            // who they are: the subjects not known yet
            let mut subjects: Vec<&String> = pairs.iter().map(|(_, s)| s).collect();
            subjects.sort();
            subjects.dedup();
            let unknown: Vec<String> = subjects
                .into_iter()
                .filter(|s| !members.contains_key(*s))
                .cloned()
                .collect();
            let found: Vec<_> = stream::iter(unknown)
                .map(|s: String| async move {
                    let membership = this.api.membership(&org.id, &s).await;
                    (s, membership)
                })
                .buffer_unordered(16)
                .collect()
                .await;
            let mut strangers = HashSet::new();
            for (subject, membership) in found {
                match membership {
                    Ok(Some(om)) => {
                        members.insert(subject, om);
                    }
                    Ok(None) => {
                        strangers.insert(subject);
                    }
                    Err(e) => {
                        warn!(provider = %name, %subject, error = %format!("{e:#}"), "permissions: finding the WorkOS user failed")
                    }
                }
            }

            // what they're granted
            let asked: Vec<_> = stream::iter(pairs)
                .map(|(grant, subject)| {
                    let (members, strangers) = (&members, &strangers);
                    async move {
                        let answer = if strangers.contains(&subject) {
                            Ok(Some(Vec::new()))
                        } else if let Some(om) = members.get(&subject) {
                            this.ask(org, om, &this.grants[grant]).await
                        } else {
                            Err(anyhow::anyhow!("not found yet"))
                        };
                        (grant, subject, answer)
                    }
                })
                .buffer_unordered(16)
                .collect()
                .await;
            for (grant, subject, answer) in asked {
                match answer {
                    Ok(Some(mut values)) => {
                        values.sort();
                        values.dedup();
                        self.watchers.lock().unwrap().publish(
                            grant,
                            &subject,
                            Some(Arc::new(values)),
                        );
                        answered.insert((grant, subject));
                    }
                    // the membership is gone: found again next time
                    Ok(None) => {
                        members.remove(&subject);
                    }
                    Err(e) => {
                        if !strangers.contains(&subject) && members.contains_key(&subject) {
                            warn!(provider = %name, %subject, permission = %self.grants[grant].permission, error = %format!("{e:#}"), "permissions: asking WorkOS failed; keeping the last answer");
                        }
                    }
                }
            }

            // a new subject is asked at once; everyone again every `poll`
            all = tokio::select! {
                _ = tokio::time::sleep(self.config.poll) => true,
                _ = self.wake.notified() => false,
            };
        }
    }

    /// What membership `om` is granted by `grant`; none: the membership is gone.
    async fn ask(&self, org: &Org, om: &str, grant: &Grant) -> Result<Option<Vec<String>>> {
        match grant.kind {
            GrantKind::Values => self.api.resources(org, om, &grant.permission).await,
            GrantKind::Check => Ok(self
                .api
                .check(org, om, &grant.permission)
                .await?
                .map(|yes| yes.then(|| grant.permission.clone()).into_iter().collect())),
        }
    }
}

/// The organization: its id, and its resource's (the parent of the others).
struct Org {
    id: String,
    resource: String,
}

struct Api {
    http: reqwest::Client,
    base: Url,
    key: String,
}

#[derive(Deserialize)]
struct List<T> {
    data: Vec<T>,
    #[serde(default)]
    list_metadata: Option<ListMetadata>,
}

#[derive(Deserialize)]
struct ListMetadata {
    after: Option<String>,
}

impl Api {
    fn url(&self, path: &[&str], query: &[(&str, &str)]) -> Url {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .expect("an http(s) base URL")
            .pop_if_empty()
            .extend(path);
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query);
        }
        url
    }

    /// The response's body; none: 404.
    async fn send<T: DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<Option<T>> {
        let response = request.bearer_auth(&self.key).send().await?;
        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!("{status}: {}", body.chars().take(300).collect::<String>());
        }
        Ok(Some(response.json().await?))
    }

    async fn get<T: DeserializeOwned>(
        &self,
        path: &[&str],
        query: &[(&str, &str)],
    ) -> Result<Option<T>> {
        self.send(self.http.get(self.url(path, query))).await
    }

    async fn organization(&self, external_id: &str) -> Result<Org> {
        let org: Option<Value> = self
            .get(&["organizations", "external_id", external_id], &[])
            .await?;
        let org = org.with_context(|| format!("no organization with external_id {external_id}"))?;
        let id = org["id"]
            .as_str()
            .context("an organization without an id")?
            .to_string();
        let list: Option<List<Value>> = self
            .get(
                &["authorization", "resources"],
                &[
                    ("organization_id", &id),
                    ("resource_type_slug", "organization"),
                ],
            )
            .await?;
        let resource = list
            .and_then(|l| {
                l.data
                    .first()
                    .and_then(|r| r["id"].as_str().map(str::to_string))
            })
            .with_context(|| format!("organization {id} has no authorization resource"))?;
        Ok(Org { id, resource })
    }

    /// The membership in `org` of the user whose `external_id` is `subject`; none: no such
    /// user, or not a member.
    async fn membership(&self, org: &str, subject: &str) -> Result<Option<String>> {
        let user: Option<Value> = self
            .get(&["user_management", "users", "external_id", subject], &[])
            .await?;
        let Some(user) = user.as_ref().and_then(|u| u["id"].as_str()) else {
            return Ok(None);
        };
        let list: Option<List<Value>> = self
            .get(
                &["user_management", "organization_memberships"],
                &[
                    ("user_id", user),
                    ("organization_id", org),
                    ("limit", "100"),
                ],
            )
            .await?;
        Ok(list.and_then(|l| {
            l.data
                .iter()
                .find(|m| m["status"] == "active")
                .and_then(|m| m["id"].as_str().map(str::to_string))
        }))
    }

    /// The `external_id`s of the organization's resources `om` holds `permission` on; none:
    /// the membership is gone.
    async fn resources(
        &self,
        org: &Org,
        om: &str,
        permission: &str,
    ) -> Result<Option<Vec<String>>> {
        let mut out = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let mut query = vec![
                ("permission_slug", permission),
                ("parent_resource_id", org.resource.as_str()),
                ("limit", "100"),
            ];
            if let Some(a) = &after {
                query.push(("after", a));
            }
            let path = ["authorization", "organization_memberships", om, "resources"];
            let Some(page): Option<List<Value>> = self.get(&path, &query).await? else {
                return Ok(None);
            };
            out.extend(
                page.data
                    .iter()
                    .filter_map(|r| r["external_id"].as_str().map(str::to_string)),
            );
            match page.list_metadata.and_then(|m| m.after) {
                Some(a) if !page.data.is_empty() => after = Some(a),
                _ => return Ok(Some(out)),
            }
        }
    }

    /// Whether `om` holds `permission` on the organization; none: the membership is gone.
    async fn check(&self, org: &Org, om: &str, permission: &str) -> Result<Option<bool>> {
        let url = self.url(
            &["authorization", "organization_memberships", om, "check"],
            &[],
        );
        let body = json!({ "permission_slug": permission, "resource_id": org.resource });
        let answer: Option<Value> = self.send(self.http.post(url).json(&body)).await?;
        answer
            .map(|a| {
                a["authorized"]
                    .as_bool()
                    .context("a check without `authorized`")
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::{Path, Query, State};
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use std::collections::{BTreeMap, BTreeSet};

    const ORG: &str = "org_1";
    const ORG_RESOURCE: &str = "authz_resource_org";

    /// WorkOS, as much as the provider asks of it.
    #[derive(Default)]
    struct Mock {
        /// external_id → user id
        users: BTreeMap<String, String>,
        /// user id → membership
        members: BTreeMap<String, String>,
        /// (membership, permission) → resources' external_ids
        resources: BTreeMap<(String, String), Vec<String>>,
        /// (membership, permission) on the organization
        checks: BTreeSet<(String, String)>,
        failing: bool,
        requests: usize,
    }

    type Shared = Arc<Mutex<Mock>>;

    fn authorized(headers: &HeaderMap) -> bool {
        headers.get("authorization").and_then(|v| v.to_str().ok()) == Some("Bearer sk_test")
    }

    async fn serve(mock: Shared) -> String {
        async fn org(Path(ext): Path<String>) -> Result<Json<Value>, StatusCode> {
            (ext == "acme")
                .then(|| Json(json!({ "id": ORG })))
                .ok_or(StatusCode::NOT_FOUND)
        }
        async fn org_resource(Query(q): Query<HashMap<String, String>>) -> Json<Value> {
            assert_eq!(q["organization_id"], ORG);
            assert_eq!(q["resource_type_slug"], "organization");
            Json(json!({ "data": [{ "id": ORG_RESOURCE }], "list_metadata": { "after": null } }))
        }
        async fn user(
            State(m): State<Shared>,
            headers: HeaderMap,
            Path(ext): Path<String>,
        ) -> Result<Json<Value>, StatusCode> {
            let mut m = m.lock().unwrap();
            m.requests += 1;
            if !authorized(&headers) {
                return Err(StatusCode::UNAUTHORIZED);
            }
            let id = m.users.get(&ext).ok_or(StatusCode::NOT_FOUND)?;
            Ok(Json(json!({ "id": id, "external_id": ext })))
        }
        async fn memberships(
            State(m): State<Shared>,
            Query(q): Query<HashMap<String, String>>,
        ) -> Json<Value> {
            let m = m.lock().unwrap();
            assert_eq!(q["organization_id"], ORG);
            let data: Vec<_> = m
                .members
                .get(&q["user_id"])
                .map(|om| json!({ "id": om, "status": "active" }))
                .into_iter()
                .collect();
            Json(json!({ "data": data, "list_metadata": { "after": null } }))
        }
        // two to a page, the cursor an index
        async fn resources(
            State(m): State<Shared>,
            Path(om): Path<String>,
            Query(q): Query<HashMap<String, String>>,
        ) -> Result<Json<Value>, StatusCode> {
            let mut m = m.lock().unwrap();
            m.requests += 1;
            if m.failing {
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
            assert_eq!(q["parent_resource_id"], ORG_RESOURCE);
            if !m.members.values().any(|o| *o == om) {
                return Err(StatusCode::NOT_FOUND);
            }
            let all = m
                .resources
                .get(&(om, q["permission_slug"].clone()))
                .cloned()
                .unwrap_or_default();
            let from: usize = q.get("after").map_or(0, |a| a.parse().unwrap());
            let page: Vec<_> = all
                .iter()
                .skip(from)
                .take(2)
                .map(|e| json!({ "external_id": e }))
                .collect();
            let after = (from + 2 < all.len()).then(|| (from + 2).to_string());
            Ok(Json(
                json!({ "data": page, "list_metadata": { "after": after } }),
            ))
        }
        async fn check(
            State(m): State<Shared>,
            Path(om): Path<String>,
            Json(body): Json<Value>,
        ) -> Json<Value> {
            let mut m = m.lock().unwrap();
            m.requests += 1;
            assert_eq!(body["resource_id"], ORG_RESOURCE);
            let permission = body["permission_slug"].as_str().unwrap().to_string();
            Json(json!({ "authorized": m.checks.contains(&(om, permission)) }))
        }
        let app = Router::new()
            .route("/organizations/external_id/{ext}", get(org))
            .route("/authorization/resources", get(org_resource))
            .route("/user_management/users/external_id/{ext}", get(user))
            .route(
                "/user_management/organization_memberships",
                get(memberships),
            )
            .route(
                "/authorization/organization_memberships/{om}/resources",
                get(resources),
            )
            .route(
                "/authorization/organization_memberships/{om}/check",
                post(check),
            )
            .with_state(mock);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        url
    }

    fn mock() -> Shared {
        let mut m = Mock::default();
        for (ext, id, om) in [("alice", "u_a", "om_a"), ("bob", "u_b", "om_b")] {
            m.users.insert(ext.into(), id.into());
            m.members.insert(id.into(), om.into());
        }
        // carol is a user, but not a member
        m.users.insert("carol".into(), "u_c".into());
        let feeds = ["alice", "bob", "carol", "dave", "erin"].map(String::from);
        m.resources
            .insert(("om_a".into(), "feed:read".into()), feeds.to_vec());
        m.resources
            .insert(("om_b".into(), "feed:read".into()), vec!["bob".into()]);
        m.checks.insert(("om_a".into(), "messages:read".into()));
        Arc::new(Mutex::new(m))
    }

    async fn start(mock: &Shared, poll: Duration) -> Arc<WorkOsProvider> {
        let config = WorkOsConfig {
            organization: "acme".into(),
            api_key_env: default_api_key_env(),
            poll,
            url: serve(mock.clone()).await,
        };
        let grants = vec![
            Grant::new("feed:read", GrantKind::Values).unwrap(),
            Grant::new("messages:read", GrantKind::Check).unwrap(),
        ];
        WorkOsProvider::start_with_key("workos", &config, "sk_test".into(), grants).unwrap()
    }

    async fn loaded(rx: &mut watch::Receiver<Granted>) -> Vec<String> {
        let got = tokio::time::timeout(Duration::from_secs(5), rx.wait_for(Option::is_some))
            .await
            .expect("an answer")
            .unwrap();
        got.as_ref().unwrap().as_ref().clone()
    }

    async fn changes_to(rx: &watch::Receiver<Granted>, want: &[&str]) {
        let mut rx = rx.clone();
        tokio::time::timeout(
            Duration::from_secs(5),
            rx.wait_for(|g| g.as_deref().is_some_and(|v| v.iter().eq(want.iter()))),
        )
        .await
        .expect("the change")
        .unwrap();
    }

    #[tokio::test]
    async fn values_checks_and_strangers() {
        let mock = mock();
        let p = start(&mock, Duration::from_secs(60)).await;
        // a new subject is asked at once, not at the next poll; every page is read
        let mut alice = p.watch(0, "alice");
        assert_eq!(
            loaded(&mut alice).await,
            ["alice", "bob", "carol", "dave", "erin"]
        );
        let mut bob = p.watch(0, "bob");
        assert_eq!(loaded(&mut bob).await, ["bob"]);
        // a check: yes is the permission, no is nothing
        assert_eq!(loaded(&mut p.watch(1, "alice")).await, ["messages:read"]);
        assert!(loaded(&mut p.watch(1, "bob")).await.is_empty());
        // no member, no user: nothing
        assert!(loaded(&mut p.watch(0, "carol")).await.is_empty());
        assert!(loaded(&mut p.watch(1, "zed")).await.is_empty());
    }

    #[tokio::test]
    async fn changes_are_polled_and_failures_keep_the_last_answer() {
        let mock = mock();
        let p = start(&mock, Duration::from_millis(50)).await;
        let mut bob = p.watch(0, "bob");
        let mut check = p.watch(1, "bob");
        assert_eq!(loaded(&mut bob).await, ["bob"]);
        assert!(loaded(&mut check).await.is_empty());

        // granted in WorkOS: told at the next poll
        {
            let mut m = mock.lock().unwrap();
            m.resources.insert(
                ("om_b".into(), "feed:read".into()),
                vec!["alice".into(), "bob".into()],
            );
            m.checks.insert(("om_b".into(), "messages:read".into()));
        }
        changes_to(&bob, &["alice", "bob"]).await;
        changes_to(&check, &["messages:read"]).await;

        // WorkOS failing: what was told stands
        mock.lock().unwrap().failing = true;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(bob.borrow().as_deref().unwrap(), &["alice", "bob"]);

        // revoked (and working again): told
        {
            let mut m = mock.lock().unwrap();
            m.failing = false;
            m.checks.clear();
        }
        changes_to(&check, &[]).await;

        // nobody watching anymore: not asked anymore
        drop((bob, check));
        tokio::time::sleep(Duration::from_millis(200)).await;
        let before = mock.lock().unwrap().requests;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(mock.lock().unwrap().requests, before);
    }

    #[test]
    fn config() {
        let c: WorkOsConfig = toml::from_str("organization = \"acme\"\npoll = \"2s\"").unwrap();
        assert_eq!(c.api_key_env, "WORKOS_API_KEY");
        assert_eq!(c.poll, Duration::from_secs(2));
        assert_eq!(c.url, "https://api.workos.com");
        c.check().unwrap();
        assert!(Grant::new("feed read", GrantKind::Values).is_err());
        assert!(Grant::new("", GrantKind::Check).is_err());
    }
}
