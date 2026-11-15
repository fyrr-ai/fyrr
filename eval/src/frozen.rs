//! Read-only adapters for retained, profile-bound frozen vectors.
use std::collections::BTreeMap;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::{Error, Result};

use clap::Args;
use fyrr_core::config::EmbeddingConfig;
use fyrr_core::envelope::EmbeddingIdentity;
use rusqlite::{Connection, OpenFlags, OptionalExtension};

use fyrr_reader::embedding::{self, Role, digest, identity, validate};
use sha2::{Digest, Sha256};

const INVALID: Error = Error::InputInvalid;

#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub(crate) struct FrozenInput {
    /// Profile-bound JSON query vectors keyed by the original frozen text keys.
    #[arg(long)]
    frozen_queries: Option<PathBuf>,
    /// Immutable paid SQLite index, opened read-only.
    #[arg(long, requires = "frozen_vectors")]
    frozen_index: Option<PathBuf>,
    /// Packed little-endian f32 file bound to the immutable index.
    #[arg(long, requires = "frozen_index")]
    frozen_vectors: Option<PathBuf>,
}

struct Indexed {
    index: Mutex<Connection>,
    vectors: File,
    role: Option<Role>,
}

pub(crate) struct FrozenVectors {
    config: EmbeddingConfig,
    queries: BTreeMap<String, Vec<f32>>,
    indexed: Option<Indexed>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Queries {
    embedding: EmbeddingIdentity,
    vectors: BTreeMap<String, Vec<f32>>,
}

impl FrozenInput {
    pub(crate) fn configured(&self) -> bool {
        self.frozen_queries.is_some() || self.frozen_index.is_some()
    }

    pub(crate) fn open(&self, config: &EmbeddingConfig) -> Result<Arc<FrozenVectors>> {
        embedding::check_profile(config).map_err(|_| INVALID)?;
        let queries = if let Some(path) = &self.frozen_queries {
            let input: Queries = serde_json::from_reader(File::open(path).map_err(|_| INVALID)?)
                .map_err(|_| INVALID)?;
            if input.embedding != identity(config) {
                return Err(INVALID);
            }
            for (key, vector) in &input.vectors {
                fyrr_core::limits::check_digest("frozen key", key).map_err(|_| INVALID)?;
                validate(vector, config.dimensions).map_err(|_| INVALID)?;
            }
            input.vectors
        } else {
            BTreeMap::new()
        };
        let indexed = self.open_index(config)?;
        Ok(Arc::new(FrozenVectors {
            config: config.clone(),
            queries,
            indexed,
        }))
    }
    fn open_index(&self, config: &EmbeddingConfig) -> Result<Option<Indexed>> {
        Ok(match (&self.frozen_index, &self.frozen_vectors) {
            (Some(path), Some(vectors)) => {
                let path = std::fs::canonicalize(path).map_err(|_| INVALID)?;
                let escaped = path
                    .to_str()
                    .ok_or(INVALID)?
                    .replace('%', "%25")
                    .replace('?', "%3f")
                    .replace('#', "%23");
                let index = Connection::open_with_flags(
                    format!("file:{escaped}?immutable=1&mode=ro"),
                    OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
                )
                .map_err(|_| INVALID)?;
                let declared = |table: &str| -> Result<bool> {
                    index
                        .query_row(
                            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND \
                                name=?1)",
                            [table],
                            |r| r.get(0),
                        )
                        .map_err(|_| INVALID)
                };
                let role = if declared("embedding_profile")? {
                    let profiles = index
                        .prepare("SELECT identity, role FROM embedding_profile LIMIT 2")
                        .map_err(|_| INVALID)?
                        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
                        .map_err(|_| INVALID)?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|_| INVALID)?;
                    let [(profile, role)] = profiles.as_slice() else {
                        return Err(INVALID);
                    };
                    if serde_json::from_str::<EmbeddingIdentity>(profile).map_err(|_| INVALID)?
                        != identity(config)
                    {
                        return Err(INVALID);
                    }
                    Some(match role.as_str() {
                        "query" => Role::Query,
                        "document" => Role::Document,
                        _ => return Err(INVALID),
                    })
                } else {
                    if identity(config).settings.is_some() {
                        return Err(INVALID);
                    }
                    None
                };
                let (count, minimum, maximum): (i64, Option<i64>, Option<i64>) = index
                    .query_row(
                        "SELECT COUNT(*),MIN(vector_index),MAX(vector_index) FROM embeddings",
                        [],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                    )
                    .map_err(|_| INVALID)?;
                let (rows, expected) = if declared("embedding_vectors")? {
                    let bindings = index
                        .prepare("SELECT dimensions,rows,sha256 FROM embedding_vectors LIMIT 2")
                        .map_err(|_| INVALID)?
                        .query_map([], |r| {
                            Ok((
                                r.get::<_, u32>(0)?,
                                r.get::<_, i64>(1)?,
                                r.get::<_, String>(2)?,
                            ))
                        })
                        .map_err(|_| INVALID)?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|_| INVALID)?;
                    let [(width, rows, hash)] = bindings.as_slice() else {
                        return Err(INVALID);
                    };
                    if *width != config.dimensions {
                        return Err(INVALID);
                    }
                    (u64::try_from(*rows).map_err(|_| INVALID)?, hash.clone())
                } else {
                    // The original immutable pair predates file-binding metadata.
                    // Its known hashes are the binding; lengths alone prove nothing.
                    if config.dimensions != 1024
                        || hash(File::open(path).map_err(|_| INVALID)?)?
                            != "sha256:13a9f70ad6b5086674b00d0e5ec0f2b5647d12fd9deb2fcc716aead\
                                df4895ee5"
                    {
                        return Err(INVALID);
                    }
                    let rows = maximum
                        .and_then(|n| u64::try_from(n).ok())
                        .and_then(|n| n.checked_add(1))
                        .ok_or(INVALID)?;
                    (
                        rows,
                        "sha256:602b5282d707056a01353588e4a00cec1bcd1d80aa63e8583f26cc1b5352d8de"
                            .into(),
                    )
                };
                let vectors = File::open(vectors).map_err(|_| INVALID)?;
                if (count == 0 && rows != 0)
                    || minimum.is_some_and(|n| n < 0)
                    || maximum.is_some_and(|n| n < 0 || n as u64 >= rows)
                    || rows
                        .checked_mul(u64::from(config.dimensions))
                        .and_then(|n| n.checked_mul(4))
                        != Some(vectors.metadata().map_err(|_| INVALID)?.len())
                    || hash(vectors.try_clone().map_err(|_| INVALID)?)? != expected
                {
                    return Err(INVALID);
                }
                Some(Indexed {
                    index: Mutex::new(index),
                    vectors,
                    role,
                })
            }
            (None, None) => None,
            _ => return Err(INVALID),
        })
    }
}

fn hash(mut file: File) -> Result<String> {
    let mut hash = Sha256::new();
    let mut buffer = vec![0; 64 * 1024];
    loop {
        let count = std::io::Read::read(&mut file, &mut buffer).map_err(|_| INVALID)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(format!("sha256:{}", hex::encode(hash.finalize())))
}

impl FrozenVectors {
    /// The vector of provider input `text` (prefixes already applied).
    pub(crate) fn transformed(&self, text: &str, role: Role) -> Result<Vec<f32>> {
        let key = if identity(&self.config).settings.is_none() {
            digest(text.as_bytes())
        } else {
            embedding::transformed_key(&self.config, text, role)
        };
        if let Some(vector) = self.queries.get(&key) {
            return Ok(vector.clone());
        }
        let indexed = self.indexed.as_ref().ok_or(INVALID)?;
        if indexed.role.is_some_and(|cached| cached != role) {
            return Err(INVALID);
        }
        let position: i64 = indexed
            .index
            .lock()
            .map_err(|_| INVALID)?
            .query_row(
                "SELECT vector_index FROM embeddings WHERE text_sha256=?1",
                [&digest(text.as_bytes())[7..]],
                |r| r.get(0),
            )
            .optional()
            .map_err(|_| INVALID)?
            .ok_or(INVALID)?;
        let offset = u64::try_from(position)
            .map_err(|_| INVALID)?
            .checked_mul(u64::from(self.config.dimensions) * 4)
            .ok_or(INVALID)?;
        let mut bytes = vec![0; self.config.dimensions as usize * 4];
        indexed
            .vectors
            .read_exact_at(&mut bytes, offset)
            .map_err(|_| INVALID)?;
        let vector: Vec<_> = bytes
            .chunks_exact(4)
            .map(|b| f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
            .collect();
        validate(&vector, self.config.dimensions).map_err(|_| INVALID)?;
        Ok(vector)
    }
}
