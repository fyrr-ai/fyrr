use super::*;
#[tokio::test]
async fn response_frame_keeps_claim_after_body_drop() {
    use hyper::body::Body;
    let claims = Arc::new(crate::serve::ResponseClaims::default());
    let weak = Arc::downgrade(&claims);
    let mut body = AccountedBody {
        body: axum::body::Body::from("bounded response"),
        _claims: claims,
    };
    let frame = futures::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx))
        .await
        .unwrap()
        .unwrap();
    let bytes = frame.into_data().unwrap();
    drop(body);
    assert!(weak.upgrade().is_some());
    assert_eq!(bytes.as_ref(), b"bounded response");
    drop(bytes);
    assert!(weak.upgrade().is_none());
}
