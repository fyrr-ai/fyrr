//! The independent reads and index opens of a reader start run together, in
//! the time of one round, and report the failure a sequential start reports.
use super::*;
use crate::storage::test_store::latency::LatencyStore;
use object_store::ObjectStoreExt;
use std::time::Duration;

const LATENCY: Duration = Duration::from_secs(1);

struct Publication {
    store: Arc<object_store::memory::InMemory>,
    head: ServingHead,
    manifest: ContentManifest,
}
async fn publication() -> Publication {
    let store = Arc::new(object_store::memory::InMemory::new());
    let manifest_text =
        include_str!("../../../../../fixtures/contracts/content-manifest/manifest.json");
    let policy = include_str!("../../../../../fixtures/contracts/policy-artifact/valid.json");
    let identity =
        include_str!("../../../../../fixtures/contracts/identity-artifact/artifact.json");
    let mut head: ServingHead = serde_json::from_str(include_str!(
        "../../../../../fixtures/contracts/serving-head/head.json"
    ))
    .unwrap();
    head.policy.digest = digest(policy.as_bytes());
    head.identity.digest = digest(identity.as_bytes());
    let pointer = head.content.clone().unwrap();
    for (object, text) in [
        (pointer.manifest.as_str(), manifest_text),
        (head.policy.object.as_str(), policy),
        (head.identity.object.as_str(), identity),
    ] {
        store
            .put(&Path::from(format!("ns/{object}")), text.to_owned().into())
            .await
            .unwrap();
    }
    Publication {
        store,
        head,
        manifest: serde_json::from_str(manifest_text).unwrap(),
    }
}

type Reads = (
    Result<(ContentManifest, Arc<PolicyArtifact>, Arc<IdentityArtifact>)>,
    LatencyStore,
    Duration,
);
async fn read(
    publication: &Publication,
    store: Arc<object_store::memory::InMemory>,
    namespace: &str,
    embedding: &fyrr_core::envelope::EmbeddingIdentity,
) -> Reads {
    let store = LatencyStore::new(store, LATENCY);
    let pointer = publication.head.content.as_ref().unwrap();
    let caches = (ArtifactCache::default(), ArtifactCache::default());
    let start = tokio::time::Instant::now();
    let result = PreparedReader::read_publication(
        &store,
        &Path::from("ns"),
        &publication.head,
        pointer,
        namespace,
        embedding,
        (&caches.0, &caches.1),
    )
    .await;
    (result, store.clone(), start.elapsed())
}

#[tokio::test(start_paused = true)]
async fn manifest_policy_and_identity_are_read_in_one_round() {
    let p = publication().await;
    let (result, store, elapsed) = read(
        &p,
        p.store.clone(),
        &p.manifest.namespace,
        &p.manifest.embedding,
    )
    .await;
    let (manifest, _, _) = result.unwrap();
    assert_eq!(manifest, p.manifest);
    assert_eq!(store.requests(), 3);
    assert_eq!(store.peak_in_flight(), 3);
    assert_eq!(
        store.rounds(elapsed),
        1,
        "three sequential reads take 3: {elapsed:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn publication_read_failures_keep_their_sequential_order() {
    let p = publication().await;
    let pointer = p.head.content.clone().unwrap();
    let namespace = p.manifest.namespace.clone();
    let embedding = p.manifest.embedding.clone();
    let without = |store: Arc<object_store::memory::InMemory>, paths: Vec<String>| async move {
        for path in paths {
            store
                .delete(&Path::from(format!("ns/{path}")))
                .await
                .unwrap();
        }
        store
    };
    // Everything is missing: the manifest read fails first.
    let store = without(
        p.store.clone(),
        vec![
            pointer.manifest.clone(),
            p.head.policy.object.clone(),
            p.head.identity.object.clone(),
        ],
    )
    .await;
    assert_eq!(
        read(&p, store, &namespace, &embedding).await.0.unwrap_err(),
        StorageUnavailable
    );
    // A foreign manifest is judged before the missing policy.
    let p = publication().await;
    let store = without(p.store.clone(), vec![p.head.policy.object.clone()]).await;
    assert_eq!(
        read(&p, store.clone(), "other", &embedding)
            .await
            .0
            .unwrap_err(),
        PublicationInvalid
    );
    let mut foreign = embedding.clone();
    foreign.fingerprint = digest(b"another embedding");
    assert_eq!(
        read(&p, store.clone(), &namespace, &foreign)
            .await
            .0
            .unwrap_err(),
        EmbeddingProfileMismatch
    );
    // With a valid manifest, a missing policy or identity is not authorized.
    assert_eq!(
        read(&p, store, &namespace, &embedding).await.0.unwrap_err(),
        AuthorizationUnavailable
    );
    let p = publication().await;
    let store = without(p.store.clone(), vec![p.head.identity.object.clone()]).await;
    assert_eq!(
        read(&p, store, &namespace, &embedding).await.0.unwrap_err(),
        AuthorizationUnavailable
    );
}

fn index(name: &str, fragments: &[u32]) -> lance_table::format::IndexMetadata {
    use lance_table::format::{IndexMetadata, pb};
    static SERIAL: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(1);
    let mut uuid = vec![0u8; 16];
    uuid[0] = SERIAL.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let mut index = IndexMetadata::try_from(pb::IndexMetadata {
        uuid: Some(pb::Uuid { uuid }),
        name: name.to_owned(),
        fields: vec![0],
        ..Default::default()
    })
    .unwrap();
    let bitmap = index.fragment_bitmap.insert(Default::default());
    for fragment in fragments {
        bitmap.insert(*fragment);
    }
    index
}

/// `segments` indexes of every kind, all covering fragments 0 and 1.
fn complete_indexes(segments: usize) -> Vec<lance_table::format::IndexMetadata> {
    INDEX_KINDS
        .iter()
        .flat_map(|(_, name, _)| (0..segments).map(|_| index(name, &[0, 1])))
        .collect()
}

/// Opens the index with one round of latency; `fail` names the kinds whose open fails
/// and `wrong` the kinds that open as another index type.
async fn verify(
    indexes: &[lance_table::format::IndexMetadata],
    fts_compatible: bool,
    fail: &[&str],
    wrong: &[&str],
) -> (Result<(), IndexVerificationError>, LatencyStore, Duration) {
    let backing = Arc::new(object_store::memory::InMemory::new());
    backing.put(&Path::from("index"), "i".into()).await.unwrap();
    let store = LatencyStore::new(backing, LATENCY);
    let by_column: BTreeMap<_, _> = INDEX_KINDS
        .iter()
        .map(|(column, name, kind)| (*column, (*name, *kind)))
        .collect();
    let start = tokio::time::Instant::now();
    let result = verify_loaded_indices(indexes, &[0, 1], fts_compatible, |column, _| {
        let store = store.clone();
        let (name, kind) = by_column[column];
        let outcome = if fail.contains(&name) {
            Err(IndexVerificationError::StorageUnavailable)
        } else if wrong.contains(&name) {
            Ok(IndexType::Vector)
        } else {
            Ok(kind)
        };
        async move {
            store.head(&Path::from("index")).await.unwrap();
            outcome
        }
    })
    .await;
    (result, store.clone(), start.elapsed())
}

#[tokio::test(start_paused = true)]
async fn index_opens_overlap_up_to_their_bound() {
    for (segments, rounds) in [(1, 1), (2, 1), (5, 3)] {
        let indexes = complete_indexes(segments);
        let (result, store, elapsed) = verify(&indexes, true, &[], &[]).await;
        result.unwrap();
        assert_eq!(store.requests(), 8 * segments);
        assert_eq!(
            store.peak_in_flight(),
            (8 * segments).min(INDEX_OPEN_CONCURRENCY)
        );
        assert_eq!(
            store.rounds(elapsed),
            rounds,
            "{segments} segments: {elapsed:?}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn index_verification_reports_the_error_a_sequential_pass_reports() {
    use IndexVerificationError::*;
    let indexes = complete_indexes(2);
    let [readers, fts, uri, source_type] = [
        "readers_idx",
        Layout::FTS_INDEX,
        "source_uri_idx",
        "source_type_idx",
    ];
    // A later open failure never outranks an earlier kind mismatch.
    assert_eq!(
        verify(&indexes, true, &[source_type], &[uri]).await.0,
        Err(IncompatibleIndex)
    );
    // The earliest failing index wins whichever open finishes first.
    assert_eq!(
        verify(&indexes, true, &[readers], &[readers, uri]).await.0,
        Err(StorageUnavailable)
    );
    assert_eq!(
        verify(&indexes, true, &[uri], &[fts]).await.0,
        Err(IncompatibleIndex)
    );
    // FTS incompatibility is judged in its kind's turn, after the readers kind.
    assert_eq!(
        verify(&indexes, false, &[], &[]).await.0,
        Err(IncompatibleIndex)
    );
    assert_eq!(
        verify(&indexes, false, &[readers], &[]).await.0,
        Err(StorageUnavailable)
    );
    assert_eq!(
        verify(&indexes, false, &[], &[readers]).await.0,
        Err(ReadersIndexMissing)
    );
    // Coverage is judged after the opens of its own kind, before later kinds.
    let mut gap = complete_indexes(1);
    gap.retain(|i| i.name != "readers_idx");
    assert_eq!(
        verify(&gap, true, &[uri], &[]).await.0,
        Err(ReadersIndexMissing)
    );
    let mut partial = complete_indexes(1);
    partial.retain(|i| i.name != "project_idx");
    partial.push(index("project_idx", &[0]));
    assert_eq!(
        verify(&partial, true, &[], &[]).await.0,
        Err(IncompatibleIndex)
    );
}
