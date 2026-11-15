use super::*;
#[test]
fn list_cursor_maximum_identifiers_and_windows_fit_and_roundtrip() {
    let documents: Vec<_> = (0..5)
        .map(|index| FetchDocument {
            source_id: format!("{}{}", "\u{1}".repeat(511), index),
            root_id: "\\".repeat(512),
            window: Some(FetchWindow {
                record_id: "\"".repeat(512),
                revision: "\u{1}".repeat(256),
                start: u32::MAX,
                end: Some(u32::MAX),
                context: None,
            }),
        })
        .collect();
    let legacy = CursorWire {
        namespace: "n".repeat(63).into(),
        subject: "s".repeat(71).into(),
        content_version: u64::MAX,
        expires: i64::MAX,
        uri: None,
        source: None,
        root: None,
        after: Cow::Owned(("s".repeat(63), "\"".repeat(512), "\\".repeat(512))),
        offset: usize::MAX,
        documents: Some(Cow::Owned(documents.clone())),
        document_index: 4,
    };
    let cursor = FetchCursor::try_from(legacy.clone()).unwrap();
    let bytes = serde_json::to_vec(&cursor).unwrap();
    assert_eq!(bytes, serde_json::to_vec(&legacy).unwrap());
    assert!(
        bytes.len() + 65 <= fyrr_core::limits::mcp::MAX_CURSOR_BYTES,
        "{}",
        bytes.len() + 65
    );
    let decoded: FetchCursor = serde_json::from_slice(&bytes).unwrap();
    let FetchCursor::Documents(decoded) = decoded else {
        panic!("document cursor")
    };
    assert_eq!(decoded.documents, documents);
    assert_eq!(decoded.position.after, *legacy.after);
}

#[test]
fn cursor_wire_bytes_preserve_lookup_and_document_envelopes() {
    const LEGACY_DOCUMENT: &str = concat!(
        "{\"namespace\":\"n\",\"subject\":\"s\",\"content_version\":7,\"expires\"",
        ":9,\"uri\":null,\"source\":null,\"root\":null,\"after\":[\"a\",\"b\",\"c\"",
        "],\"offset\":2,\"documents\":\"AQMAc3JjBAByb290AQMAcmVjRwBzaGEyNT",
        "Y6MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMD",
        "AwMDAwMDAwMDAwMDAwMDAwMDAwMAUAAAAAAwAAAA\"}",
    );

    let document = FetchDocument {
        source_id: "src".into(),
        root_id: "root".into(),
        window: Some(FetchWindow {
            record_id: "rec".into(),
            revision: format!("sha256:{}", "0".repeat(64)),
            start: 5,
            end: None,
            context: Some(3),
        }),
    };
    let base = CursorWire {
        namespace: "n".into(),
        subject: "s".into(),
        content_version: 7,
        expires: 9,
        uri: None,
        source: None,
        root: None,
        after: Cow::Owned(("a".into(), "b".into(), "c".into())),
        offset: 2,
        documents: Some(Cow::Owned(vec![document])),
        document_index: 0,
    };

    assert_eq!(serde_json::to_string(&base).unwrap(), LEGACY_DOCUMENT);
    let mut uri = base.clone();
    uri.documents = None;
    uri.uri = Some("https://example.test/\"x".into());
    let mut root = base.clone();
    root.documents = None;
    root.source = Some("src".into());
    root.root = Some("root".into());
    let mut continuation = base.clone();
    continuation
        .documents
        .as_mut()
        .unwrap()
        .to_mut()
        .push(FetchDocument {
            source_id: "other".into(),
            root_id: "other".into(),
            window: None,
        });
    continuation.document_index = 1;
    for wire in [base, uri, root, continuation] {
        let before = serde_json::to_vec(&wire).unwrap();
        let cursor = FetchCursor::try_from(wire).unwrap();
        assert_eq!(serde_json::to_vec(&cursor).unwrap(), before);
        let decoded: FetchCursor = serde_json::from_slice(&before).unwrap();
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), before);
    }
}
