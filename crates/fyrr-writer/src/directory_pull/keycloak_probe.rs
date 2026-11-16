//! Measurement-only candidate; production directory pulls remain serial.
use super::*;
use futures::{StreamExt, TryStreamExt};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Instant;

#[derive(Default)]
struct Requests {
    calls: AtomicUsize,
    active: AtomicUsize,
    peak: AtomicUsize,
}
struct Request<'a>(&'a Requests);
impl Requests {
    fn start(&self) -> Request<'_> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.peak.fetch_max(
            self.active.fetch_add(1, Ordering::SeqCst) + 1,
            Ordering::SeqCst,
        );
        Request(self)
    }
}
impl Drop for Request<'_> {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

// Match the production per-user protocol. These fixture memberships fit one
// page, so each observed pages() call is exactly one HTTP request.
async fn memberships(
    mut api: KeycloakRead,
    user: String,
    observed_count: Option<usize>,
    requests: Arc<Requests>,
) -> Result<(String, BTreeSet<String>)> {
    let count = match observed_count {
        Some(count) => count,
        None => {
            let _request = requests.start();
            api.count(&["users", &user, "groups", "count"]).await?
        }
    };
    let groups = {
        let _request = requests.start();
        api.pages(&["users", &user, "groups"]).await?
    };
    let ids: BTreeSet<_> = groups
        .iter()
        .map(|group| text(group, "id").map(str::to_owned))
        .collect::<Result<_>>()?;
    let after = {
        let _request = requests.start();
        api.count(&["users", &user, "groups", "count"]).await?
    };
    if ids.len() != count || groups.len() != count || after != count {
        return Err(WriterError::DirectoryPullIncomplete);
    }
    Ok((user, ids))
}

fn api(client: &reqwest::Client, base: &str, token: &str) -> KeycloakRead {
    KeycloakRead {
        client: client.clone(),
        base: base.parse().unwrap(),
        token: token.into(),
    }
}

#[allow(clippy::print_stdout)]
#[tokio::test]
#[ignore = "owned real Keycloak: serial versus bounded membership read experiment"]
async fn keycloak_membership_concurrency_preserves_complete_reads() {
    let endpoint = std::env::var("FYRR_TEST_KEYCLOAK_ENDPOINT").unwrap();
    let username = std::env::var("FYRR_TEST_KEYCLOAK_USERNAME").unwrap();
    let password = std::env::var("FYRR_TEST_KEYCLOAK_PASSWORD").unwrap();
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .unwrap();
    let authorization: Value = client
        .post(format!(
            "{endpoint}/realms/master/protocol/openid-connect/token"
        ))
        .form(&[
            ("grant_type", "password"),
            ("client_id", "admin-cli"),
            ("username", username.as_str()),
            ("password", password.as_str()),
        ])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let token = authorization["access_token"].as_str().unwrap();
    let realm = format!(
        "io-probe-{}-{}",
        std::process::id(),
        fyrr_core::clock::now().timestamp_micros()
    );
    let response = client
        .post(format!("{endpoint}/admin/realms"))
        .bearer_auth(token)
        .json(&serde_json::json!({"realm":realm,"enabled":true,"sslRequired":"none"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201);
    let base = format!("{endpoint}/admin/realms/{realm}");
    let mut groups = Vec::new();
    for n in 0..2 {
        let response = client
            .post(format!("{base}/groups"))
            .bearer_auth(token)
            .json(&serde_json::json!({"name":format!("group-{n}")}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 201);
        groups.push(
            response.headers()[reqwest::header::LOCATION]
                .to_str()
                .unwrap()
                .rsplit('/')
                .next()
                .unwrap()
                .to_owned(),
        );
    }
    let mut users = Vec::new();
    for n in 0..24 {
        let response=client.post(format!("{base}/users")).bearer_auth(token).json(&serde_json::json!({"username":format!("user-{n}"),"enabled":true,"emailVerified":true,"email":format!("user-{n}@example.test")})).send().await.unwrap();
        assert_eq!(response.status(), 201);
        let user = response.headers()[reqwest::header::LOCATION]
            .to_str()
            .unwrap()
            .rsplit('/')
            .next()
            .unwrap()
            .to_owned();
        for group in groups.iter().take(if n % 2 == 0 { 2 } else { 1 }) {
            let response = client
                .put(format!("{base}/users/{user}/groups/{group}"))
                .bearer_auth(token)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 204);
        }
        users.push(user);
    }
    let mut directory = api(&client, &base, token);
    assert_eq!(directory.count(&["users", "count"]).await.unwrap(), 24);
    assert_eq!(directory.count(&["groups", "count"]).await.unwrap(), 2);
    let known_groups: BTreeSet<_> = groups.iter().cloned().collect();
    let mut baseline = None;
    // Repeat the matched pair to expose warm-order effects, never assert speed.
    for concurrency in [1, 8, 1, 8] {
        let uptime = std::process::Command::new("uptime").output().unwrap();
        let requests = Arc::new(Requests::default());
        let started = Instant::now();
        let results = futures::stream::iter(users.iter().cloned())
            .map(|user| memberships(api(&client, &base, token), user, None, requests.clone()))
            .buffer_unordered(concurrency)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let elapsed = started.elapsed();
        let values: BTreeMap<_, _> = results.into_iter().collect();
        assert_eq!(values.len(), 24);
        assert!(
            values
                .values()
                .flatten()
                .all(|id| known_groups.contains(id))
        );
        assert_eq!(values.values().map(BTreeSet::len).sum::<usize>(), 36);
        assert!(values.values().map(BTreeSet::len).sum::<usize>() <= 1_000_000);
        assert_eq!(directory.count(&["users", "count"]).await.unwrap(), 24);
        assert_eq!(directory.count(&["groups", "count"]).await.unwrap(), 2);
        if let Some(expected) = &baseline {
            assert_eq!(&values, expected);
        } else {
            baseline = Some(values);
        }
        assert_eq!(requests.calls.load(Ordering::SeqCst), 72);
        assert_eq!(requests.active.load(Ordering::SeqCst), 0);
        assert!(requests.peak.load(Ordering::SeqCst) <= concurrency);
        println!(
            "keycloak-membership-probe: concurrency={concurrency}, elapsed_us={}, requests={}, peak={}, uptime={}",
            elapsed.as_micros(),
            requests.calls.load(Ordering::SeqCst),
            requests.peak.load(Ordering::SeqCst),
            String::from_utf8_lossy(&uptime.stdout).trim()
        );
    }
    // A real membership change between the first count and list must fail.
    let user = &users[1];
    let count = directory
        .count(&["users", user, "groups", "count"])
        .await
        .unwrap();
    let response = client
        .put(format!("{base}/users/{user}/groups/{}", groups[1]))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 204);
    let result = memberships(
        api(&client, &base, token),
        user.clone(),
        Some(count),
        Arc::new(Requests::default()),
    )
    .await;
    assert!(matches!(result, Err(WriterError::DirectoryPullIncomplete)));
    let response = client
        .delete(&base)
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 204);
}
