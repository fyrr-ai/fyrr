//! The JSON query and fetch routes of the real reader executable: the gates of `/mcp`, plain JSON out.
use super::http_mcp::{Issuer, Reader, fixture};
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
    for path in [&query, "/v1/namespaces/other/query"] {
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

    // Only the configured namespace exists.
    refusal(
        post(&reader, &tokens[0], "/v1/namespaces/other/query", &search).await,
        404,
        "invalid_argument",
    )
    .await;
    refusal(
        post(
            &reader,
            &tokens[0],
            "/v1/namespaces/other/fetch",
            &json!({"cursor":"x"}),
        )
        .await,
        404,
        "invalid_argument",
    )
    .await;

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
    let denied: Value = post(&reader, &tokens[0], &fetch, &own)
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(
        denied["documents"].as_array().map_or(0, Vec::len),
        0,
        "a denied document is an empty result, not an error"
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
