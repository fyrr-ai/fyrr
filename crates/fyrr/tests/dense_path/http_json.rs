//! The JSON query and fetch routes of the real reader executable: the gates of `/mcp`, plain JSON out.
use super::http_mcp::{Issuer, Reader, fixture, fixture_with_documents};
use super::*;

async fn post(reader: &Reader, token: &str, path: &str, body: &Value) -> reqwest::Response {
    reader
        .client
        .post(format!("{}{path}", reader.base))
        .bearer_auth(token)
        .json(body)
        .send()
        .await
        .unwrap()
}

/// A refusal carries only its code in `error`; the headers come back.
async fn refusal(
    response: reqwest::Response,
    status: u16,
    code: &str,
) -> reqwest::header::HeaderMap {
    assert_eq!(response.status(), status);
    let headers = response.headers().clone();
    let body: Value = response.json().await.unwrap();
    assert_eq!(body, json!({ "error": code }));
    headers
}

#[tokio::test]
#[ignore = "real HTTP reader executable, pinned JWKS and local storage; scripts/ci"]
async fn identity_http_json_routes_share_the_mcp_gates_on_storage() {
    let (_, issuer_root) = setup();
    let issuer = Issuer::new(&issuer_root).await;
    let (worker, _, root, _) = fixture(&issuer).await;
    let namespace = fyrr_core::config::load(&worker)
        .unwrap()
        .config
        .namespace
        .name;
    let reader = Reader::start(&worker, &root, &issuer, 12, 1000).await;
    let tokens: Vec<_> = (0..4)
        .map(|i| issuer.token(&format!("subject-{i}"), json!({}), json!({})))
        .collect();
    let query = format!("/v1/namespaces/{namespace}/query");
    let fetch = format!("/v1/namespaces/{namespace}/fetch");
    let search = json!({"query":"fixture query","sources":["incremental"],"limit":5});

    // Authentication comes first: a wrong namespace does not show to a stranger.
    for path in [
        &query,
        "/v1/namespaces/other/query",
        "/v1/namespaces//query",
        "/v1/namespaces//fetch",
    ] {
        let response = reader
            .client
            .post(format!("{}{path}", reader.base))
            .json(&search)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 401, "{path}");
        assert!(response.headers().contains_key("www-authenticate"));
    }
    let unmapped = issuer.token("unmapped", json!({}), json!({}));
    let unavailable = refusal(
        post(&reader, &unmapped, &query, &search).await,
        503,
        "authorization_unavailable",
    )
    .await;
    assert_eq!(unavailable["retry-after"], "1");
    let foreign = reader
        .client
        .post(format!("{}{query}", reader.base))
        .header("Host", "untrusted.invalid")
        .bearer_auth(&tokens[0])
        .json(&search)
        .send()
        .await
        .unwrap();
    refusal(foreign, 403, "host_mismatch").await;
    assert_eq!(
        reader
            .client
            .get(format!("{}{query}", reader.base))
            .bearer_auth(&tokens[0])
            .send()
            .await
            .unwrap()
            .status(),
        405
    );

    // An empty name is no namespace. A namespace that does not exist is
    // refused as the one a caller is not bound in, whatever the route.
    refusal(
        post(&reader, &tokens[0], "/v1/namespaces//query", &search).await,
        404,
        "invalid_argument",
    )
    .await;
    for (path, body) in [
        ("/v1/namespaces/other/query", &search),
        ("/v1/namespaces/other/fetch", &json!({"cursor":"x"})),
    ] {
        let missing = refusal(
            post(&reader, &tokens[0], path, body).await,
            503,
            "authorization_unavailable",
        )
        .await;
        assert_eq!(missing["retry-after"], unavailable["retry-after"], "{path}");
    }

    // The same permission filter as MCP: each caller reads its own document
    // and the output is the tool's own structured result.
    for (i, token) in tokens.iter().enumerate() {
        let response = post(&reader, token, &query, &search).await;
        assert_eq!(response.status(), 200);
        let body: Value = response.json().await.unwrap();
        let mcp = reader.call(token, 1, "search", search.clone()).await;
        assert_eq!(body["hits"], mcp["structuredContent"]["hits"]);
        assert_eq!(
            body["metadata"]["publication_id"],
            mcp["structuredContent"]["metadata"]["publication_id"]
        );
        let hits = body["hits"].as_array().unwrap();
        assert_eq!(hits.len(), 1, "caller {i} reads one document");
        assert_eq!(hits[0]["root_id"], format!("{namespace}-http-{i}"));
    }
    let own = json!({"source_id":"incremental","root_id":format!("{namespace}-http-1")});
    let fetched: Value = post(&reader, &tokens[1], &fetch, &own)
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(
        fetched["documents"][0]["root_id"],
        format!("{namespace}-http-1")
    );
    let denied = post(&reader, &tokens[0], &fetch, &own).await;
    assert_eq!(denied.status(), 200, "a denied document is not an error");
    assert_eq!(
        denied.json::<Value>().await.unwrap()["documents"],
        json!([]),
        "a denied document is an empty result"
    );

    // Tool errors keep their code, each at its status.
    for body in [
        json!({"query":"x","limit":76}),
        json!({"query":"x","unknown":true}),
        json!([]),
    ] {
        refusal(
            post(&reader, &tokens[0], &query, &body).await,
            400,
            "invalid_argument",
        )
        .await;
    }
    let malformed = reader
        .client
        .post(format!("{}{query}", reader.base))
        .bearer_auth(&tokens[0])
        .body("{")
        .send()
        .await
        .unwrap();
    refusal(malformed, 400, "invalid_argument").await;
    refusal(
        post(&reader, &tokens[1], &fetch, &json!({"cursor":"untrusted"})).await,
        409,
        "stale_cursor",
    )
    .await;

    // Over the per-user limit: 429 with a delay; other users are unaffected.
    let mut limited = None;
    for _ in 0..16 {
        let response = post(&reader, &tokens[3], &query, &search).await;
        if response.status() == 429 {
            limited = Some(response);
            break;
        }
        assert_eq!(response.status(), 200);
    }
    let limited = refusal(
        limited.expect("the user limit applies"),
        429,
        "rate_limited",
    )
    .await;
    assert_eq!(limited["retry-after"], "60");
    assert_eq!(
        post(&reader, &tokens[2], &query, &search).await.status(),
        200
    );
}

#[tokio::test]
#[ignore = "real HTTP reader executable, pinned JWKS and local storage; scripts/ci"]
async fn identity_http_metrics_listener_counts_requests_without_identity_on_storage() {
    let (_, issuer_root) = setup();
    let issuer = Issuer::new(&issuer_root).await;
    let (worker, _, root, _) = fixture(&issuer).await;
    let namespace = fyrr_core::config::load(&worker)
        .unwrap()
        .config
        .namespace
        .name;
    let reserve = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let metrics = reserve.local_addr().unwrap();
    drop(reserve);
    let reader = Reader::start_appended(&worker, &root, |address, base| {
        format!("\n[identity.oidc]\nissuer='{0}'\naudience='fyrr-http-test'\njwks_url='{0}/jwks'\njwks_validity='1s'\njwks_refresh_interval='100ms'\n[reader.http]\nlisten='{address}'\npublic_url='{base}/mcp'\nmetrics_listen='{metrics}'\n", issuer.url)
    })
    .await;
    let token = issuer.token("subject-0", json!({}), json!({}));
    let query = format!("/v1/namespaces/{namespace}/query");
    let search = json!({"query":"fixture query","sources":["incremental"],"limit":5});
    assert_eq!(post(&reader, &token, &query, &search).await.status(), 200);
    let anonymous = reader
        .client
        .post(format!("{}{query}", reader.base))
        .json(&search)
        .send()
        .await
        .unwrap();
    assert_eq!(anonymous.status(), 401);

    // The public listener serves no metrics; the extra one needs no credential.
    let public = reader
        .client
        .get(format!("{}/metrics", reader.base))
        .send()
        .await
        .unwrap();
    assert_ne!(public.status(), 200);
    let scrape = reader
        .client
        .get(format!("http://{metrics}/metrics"))
        .send()
        .await
        .unwrap();
    assert_eq!(scrape.status(), 200);
    assert!(
        scrape.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/plain; version=0.0.4")
    );
    let body = scrape.text().await.unwrap();
    for line in [
        "fyrr_reader_http_request_duration_seconds_count{route=\"search\",code=\"200\"} 1",
        "fyrr_reader_http_request_duration_seconds_count{route=\"search\",code=\"401\"} 1",
        "fyrr_reader_open_namespaces 1",
        "# TYPE fyrr_reader_queue_wait_seconds histogram",
        "# TYPE fyrr_reader_object_store_requests_total counter",
        "# TYPE fyrr_reader_object_store_bytes_total counter",
        "# TYPE fyrr_reader_mirror_bytes gauge",
    ] {
        assert!(body.contains(line), "{line}\n{body}");
    }
    // Nothing that names a namespace, a caller, a query or a document.
    for private in [
        namespace.as_str(),
        "subject-0",
        "fixture query",
        "fixture document",
        "http-0",
    ] {
        assert!(!body.contains(private), "{private}\n{body}");
    }
}

#[tokio::test]
#[ignore = "real HTTP reader executable, pinned JWKS and local storage; scripts/ci"]
async fn identity_http_json_query_returns_every_record_and_caps_the_hits_per_author_on_storage() {
    let (_, issuer_root) = setup();
    let issuer = Issuer::new(&issuer_root).await;
    // `multi` has three records; `hidden` is not readable by the caller.
    let documents = [
        (
            "multi",
            vec!["alpha one", "alpha two", "alpha three"],
            "Ada",
            "usr:u0",
        ),
        ("ada", vec!["alpha four"], "Ada", "usr:u0"),
        ("lin", vec!["alpha five"], "Lin", "usr:u0"),
        ("hidden", vec!["alpha six"], "Ada", "usr:u1"),
    ]
    .map(|(id, texts, author, reader)| {
        let mut document = super::incremental::doc(id, "r1", "");
        document["texts"] = json!(texts);
        document["author"] = json!(author);
        document["readers"] = json!([reader]);
        document
    });
    let (worker, _, root, _) =
        fixture_with_documents(&issuer.url, Backend::selected(), documents.to_vec()).await;
    let namespace = fyrr_core::config::load(&worker)
        .unwrap()
        .config
        .namespace
        .name;
    let reader = Reader::start(&worker, &root, &issuer, 1000, 1000).await;
    let token = issuer.token("subject-0", json!({}), json!({}));
    let query = format!("/v1/namespaces/{namespace}/query");
    let ask = async |members: Value| -> Vec<(String, String)> {
        let mut body = json!({"query": "alpha", "sources": ["incremental"], "limit": 20,
                              "output": "metadata"});
        for (name, member) in members.as_object().unwrap() {
            body[name] = member.clone();
        }
        let response = post(&reader, &token, &query, &body).await;
        assert_eq!(response.status(), 200, "{body}");
        let body: Value = response.json().await.unwrap();
        body["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| {
                (
                    hit["root_id"].as_str().unwrap().to_owned(),
                    hit["record_id"].as_str().unwrap().to_owned(),
                )
            })
            .collect()
    };
    let roots = |hits: &[(String, String)]| {
        let mut roots: Vec<_> = hits.iter().map(|hit| hit.0.as_str()).collect();
        roots.sort_unstable();
        roots.into_iter().map(str::to_owned).collect::<Vec<_>>()
    };

    // One hit per document: the best record of `multi`. Saying so changes nothing.
    let grouped = ask(json!({})).await;
    assert_eq!(roots(&grouped), ["ada", "lin", "multi"]);
    assert_eq!(ask(json!({"group_documents": true})).await, grouped);
    // Every record is a hit, and `hidden` is never one.
    let every = ask(json!({"group_documents": false})).await;
    assert_eq!(roots(&every), ["ada", "lin", "multi", "multi", "multi"]);
    assert_eq!(
        every
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        5,
        "{every:?}"
    );
    for hit in &grouped {
        assert!(every.contains(hit), "the best record is among them");
    }
    assert_eq!(
        ask(json!({"group_documents": false, "limit": 2}))
            .await
            .len(),
        2
    );

    // At most one hit per author: one of the two Ada documents, and Lin.
    let by_author = |max: u32| json!([{"attribute": "author", "max": max}]);
    let one = ask(json!({"caps": by_author(1)})).await;
    assert_eq!(one.len(), 2, "{one:?}");
    assert!(roots(&one).contains(&"lin".to_owned()));
    // Capped records: two of the four records of Ada, and Lin.
    let two = ask(json!({"group_documents": false, "caps": by_author(2)})).await;
    assert_eq!(two.len(), 3, "{two:?}");
    assert!(roots(&two).contains(&"lin".to_owned()));
    // A cap that nothing reaches changes nothing.
    assert_eq!(ask(json!({"caps": by_author(3)})).await, grouped);
    assert_eq!(
        ask(json!({"group_documents": false, "caps": by_author(4)})).await,
        every
    );
}
