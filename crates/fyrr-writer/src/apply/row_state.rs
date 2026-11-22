use super::*;

/// Row-local revision/tombstone metadata in metadata_json._fyrr; its visibility is the Lance pin.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentState {
    pub revision: Option<String>,
    pub revision_order: Option<i64>,
    pub deleted: bool,
    /// Content physically absent while revision and current permission state remain authoritative.
    pub content_purged: bool,
    /// A same-kind log position proves a later observed recreation; listings have none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delete_sequence: Option<u64>,
    pub publication_sequence: u64,
    /// On a staged row: the change's permission statement. None: the change
    /// makes none (a provider with a permission stream), so it narrows
    /// nothing and its rows take the applied state.
    pub permission: Option<PermissionState>,
}
/// Separate permission revision inside DocumentState, including for a not-yet-ingested root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionState {
    pub revision: Option<String>,
    pub readers: Vec<String>,
    pub containers: Vec<String>,
}

/// Row-local key in metadata_json: the hashed readers a section states for
/// itself. A row without it takes its document's readers.
pub(crate) const OWN_READERS: &str = "_fyrr_own_readers";

/// `rows`, one-row batches of the built-in columns, as one batch followed by the
/// attribute columns of the staged rows (`taken`) that they were made from, in
/// the order of the staged rows' columns, which is the table's: the fragment
/// written from the batch names its columns by position.
pub(crate) fn with_typed(
    rows: &[RecordBatch],
    taken: &[u32],
    staged: &RecordBatch,
    attributes: &fyrr_core::attributes::AttributeSchema,
) -> Result<RecordBatch> {
    let all = arrow_select::concat::concat_batches(&rows[0].schema(), rows)
        .map_err(|_| WriterError::ChangedRowsInvalid)?;
    let indices = arrow_array::UInt32Array::from(taken.to_vec());
    let mut fields = all.schema().fields().iter().cloned().collect::<Vec<_>>();
    let mut arrays = all.columns().to_vec();
    let built_in = fields.len();
    let staged_schema = staged.schema();
    for (field, column) in staged_schema
        .fields()
        .iter()
        .zip(staged.columns())
        .skip(built_in)
    {
        let Some(kind) = attributes.get(field.name()) else {
            continue;
        };
        arrays.push(
            arrow_select::take::take(column.as_ref(), &indices, None)
                .map_err(|_| WriterError::ChangedRowsInvalid)?,
        );
        fields.push(Arc::new(arrow_schema::Field::new(
            field.name(),
            kind.arrow_type(),
            true,
        )));
    }
    // Every attribute of the settings is a column of the staged rows.
    if fields.len() - built_in != attributes.len() {
        return Err(WriterError::ChangedRowsInvalid);
    }
    RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), arrays)
        .map_err(|_| WriterError::ChangedRowsInvalid)
}

pub(crate) fn own_readers(batch: &RecordBatch, row: usize) -> Result<Option<Vec<String>>> {
    let metadata: serde_json::Value =
        serde_json::from_str(strings(batch, "metadata_json")?.value(row))
            .map_err(|_| WriterError::ChangedRowsInvalid)?;
    metadata
        .get(OWN_READERS)
        .map(|own| {
            serde_json::from_value(own.clone()).map_err(|_| WriterError::DocumentStateInvalid)
        })
        .transpose()
}

/// Set or remove the row's own readers (a one-row batch).
pub(crate) fn with_own_readers(batch: &RecordBatch, own: Option<&[String]>) -> Result<RecordBatch> {
    let mut metadata: serde_json::Value =
        serde_json::from_str(strings(batch, "metadata_json")?.value(0))
            .map_err(|_| WriterError::ChangedRowsInvalid)?;
    let fields = metadata
        .as_object_mut()
        .ok_or(WriterError::ChangedRowsInvalid)?;
    match own {
        Some(own) => fields.insert(OWN_READERS.into(), serde_json::json!(own)),
        None => fields.remove(OWN_READERS),
    };
    replace(
        batch,
        "metadata_json",
        Arc::new(StringArray::from(vec![metadata.to_string()])),
    )
}

/// The readers of a row: its document's, or its own section readers within
/// them. The only derivation: content, permissions and restore all use it.
pub(crate) fn effective_readers(own: Option<&[String]>, document: &[String]) -> Vec<String> {
    match own {
        None => document.to_vec(),
        Some(own) => own
            .iter()
            .filter(|token| document.contains(token))
            .cloned()
            .collect(),
    }
}

pub fn reader_token(config: &Config, source: &str, token: &str) -> Result<String> {
    let sync = config
        .sources
        .iter()
        .find(|s| s.id == source)
        .is_some_and(|s| s.permissions == fyrr_core::config::PermissionsMode::Sync);
    fyrr_reader::storage::hash_principal(
        config,
        "tok",
        &if sync {
            format!("{source}:{token}")
        } else {
            token.into()
        },
    )
    .map_err(WriterError::from)
}
pub fn permission_readers(
    config: &Config,
    source: &str,
    raw: &[String],
    containers: &[String],
) -> Result<Vec<String>> {
    let mut readers = raw
        .iter()
        .map(|t| reader_token(config, source, t))
        .collect::<Result<Vec<_>, _>>()?;
    for container in containers {
        readers.push(container_token(config, source, container)?);
    }
    readers.sort();
    readers.dedup();
    Ok(readers)
}
pub fn container_token(config: &Config, source: &str, id: &str) -> Result<String> {
    fyrr_reader::storage::hash_principal(config, "tok", &format!("container:{source}:{id}"))
        .map_err(WriterError::from)
}

pub fn state_source(source: &str) -> String {
    format!("__fyrr_state_{}", &digest(source.as_bytes())[7..39])
}
pub fn keys_filter(source: &str, roots: &[String]) -> String {
    format!(
        "source_id IN ({},{}) AND root_id IN ({})",
        sql(source),
        sql(&state_source(source)),
        roots.iter().map(|r| sql(r)).collect::<Vec<_>>().join(",")
    )
}
/// The content rows of the documents `roots`, without their tombstones.
pub fn rows_filter(source: &str, roots: &[String]) -> String {
    format!(
        "source_id = {} AND root_id IN ({})",
        sql(source),
        roots.iter().map(|r| sql(r)).collect::<Vec<_>>().join(",")
    )
}
pub fn strings<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|a| a.as_any().downcast_ref())
        .ok_or(WriterError::ChangedRowsInvalid)
}
pub fn tokens(batch: &RecordBatch, row: usize) -> Result<Vec<String>> {
    let a = batch
        .column_by_name("readers")
        .and_then(|a| a.as_any().downcast_ref::<ListArray>())
        .ok_or(WriterError::ChangedRowsInvalid)?;
    let values = a.value(row);
    let values = values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(WriterError::ChangedRowsInvalid)?;
    Ok(values
        .iter()
        .map(|s| s.unwrap_or_default().to_owned())
        .collect())
}
pub fn state(batch: &RecordBatch, row: usize) -> Result<DocumentState> {
    let metadata: serde_json::Value =
        serde_json::from_str(strings(batch, "metadata_json")?.value(row))
            .map_err(|_| WriterError::ChangedRowsInvalid)?;
    let value = metadata
        .get("_fyrr")
        .ok_or(WriterError::DocumentStateInvalid)?;
    serde_json::from_value(value.clone()).map_err(|_| WriterError::DocumentStateInvalid)
}
pub async fn states(
    dataset: &Dataset,
    source: &str,
    roots: &[String],
) -> Result<BTreeMap<String, DocumentState>> {
    let mut result = BTreeMap::new();
    for page in roots.chunks(256) {
        let batch = dataset
            .scan()
            .project(&["root_id", "source_revision", "metadata_json", "readers"])
            .map_err(|_| WriterError::ChangedRowsInvalid)?
            .filter(&keys_filter(source, page))
            .map_err(|_| WriterError::ChangedRowsInvalid)?
            .try_into_batch()
            .await
            .map_err(|_| WriterError::ChangedRowScanFailed)?;
        for row in 0..batch.num_rows() {
            let root = strings(&batch, "root_id")?.value(row).to_owned();
            let value = state(&batch, row)?;
            if result.get(&root).is_some_and(|previous| previous != &value) {
                return Err(WriterError::DocumentStateInvalid);
            }
            result.insert(root, value);
        }
    }
    Ok(result)
}
fn replace(batch: &RecordBatch, name: &str, array: ArrayRef) -> Result<RecordBatch> {
    let mut columns = batch.columns().to_vec();
    columns[batch
        .schema()
        .index_of(name)
        .map_err(|_| WriterError::ChangedRowsInvalid)?] = array;
    RecordBatch::try_new(batch.schema(), columns).map_err(|_| WriterError::ChangedRowsInvalid)
}
fn readers(values: &[String]) -> ArrayRef {
    Arc::new(crate::prepare::string_lists(std::iter::once(values)))
}
pub(crate) fn with_state(
    batch: &RecordBatch,
    state: &DocumentState,
    permissions: &[String],
) -> Result<RecordBatch> {
    let mut metadata: serde_json::Value =
        serde_json::from_str(strings(batch, "metadata_json")?.value(0))
            .map_err(|_| WriterError::ChangedRowsInvalid)?;
    metadata["_fyrr"] =
        serde_json::to_value(state).map_err(|_| WriterError::DocumentStateInvalid)?;
    let result = replace(
        batch,
        "metadata_json",
        Arc::new(StringArray::from(vec![metadata.to_string()])),
    )?;
    replace(&result, "readers", readers(permissions))
}
/// A one-row batch with the values of `patch` written over it: its revision,
/// the text with its vector, and the typed attributes the patch names. Every
/// other column stays as it is.
pub(crate) fn with_patch(
    batch: &RecordBatch,
    patch: &fyrr_core::envelope::DocumentPatch,
    revision: &str,
    attributes: &fyrr_core::attributes::AttributeSchema,
) -> Result<RecordBatch> {
    let mut batch = replace(
        batch,
        "source_revision",
        Arc::new(StringArray::from(vec![revision])),
    )?;
    if let Some(text) = &patch.text {
        let mut metadata: serde_json::Value =
            serde_json::from_str(strings(&batch, "metadata_json")?.value(0))
                .map_err(|_| WriterError::ChangedRowsInvalid)?;
        metadata["_fyrr_record_revision"] = fyrr_core::limits::record_revision(
            strings(&batch, "source_id")?.value(0),
            strings(&batch, "root_id")?.value(0),
            strings(&batch, "record_id")?.value(0),
            text,
        )
        .into();
        metadata["_fyrr_text_utf8_bytes"] = (text.len() as u64).into();
        batch = replace(
            &batch,
            "searchable_text",
            Arc::new(StringArray::from(vec![text.as_str()])),
        )?;
        if let Some(vector) = &patch.vector {
            batch = replace(
                &batch,
                "vector",
                Arc::new(
                    FixedSizeListArray::from_iter_primitive::<Float16Type, _, _>(
                        [Some(vector.iter().map(|v| Some(half::f16::from_f32(*v))))],
                        i32::try_from(vector.len()).map_err(|_| WriterError::ChangedRowsInvalid)?,
                    ),
                ),
            )?;
        }
        batch = replace(
            &batch,
            "metadata_json",
            Arc::new(StringArray::from(vec![metadata.to_string()])),
        )?;
    }
    let named: fyrr_core::attributes::AttributeSchema = attributes
        .iter()
        .filter(|(name, _)| patch.attributes.contains_key(*name))
        .map(|(name, kind)| (name.clone(), *kind))
        .collect();
    let mut columns =
        crate::prepare::AttributeColumns::new(&named, named.keys().cloned().collect());
    columns
        .push(&patch.attributes)
        .map_err(|_| WriterError::ChangedRowsInvalid)?;
    for (name, array) in columns.finish() {
        batch = replace(&batch, &name, array)?;
    }
    Ok(batch)
}
pub(crate) fn marker_row(
    dimensions: u32,
    source: &str,
    root: &str,
    state: &DocumentState,
) -> Result<RecordBatch> {
    let schema = crate::prepare::schema(dimensions);
    let mut vector = vec![half::f16::ZERO; dimensions as usize];
    vector[0] = half::f16::ONE;
    let vector = FixedSizeListArray::from_iter_primitive::<Float16Type, _, _>(
        [Some(vector.into_iter().map(Some))],
        dimensions as i32,
    );
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| match field.name().as_str() {
            "source_id" => Arc::new(StringArray::from(vec![state_source(source)])) as ArrayRef,
            "root_id" => Arc::new(StringArray::from(vec![root])),
            "record_id" => Arc::new(StringArray::from(vec![format!(
                "state-{}",
                &digest(root.as_bytes())[7..39]
            )])),
            "source_revision" => Arc::new(StringArray::from(vec![
                state.revision.clone().unwrap_or_default(),
            ])),
            "metadata_json" => Arc::new(StringArray::from(vec![
                serde_json::json!({"_fyrr":state}).to_string(),
            ])),
            "searchable_text" => Arc::new(StringArray::from(vec![""])),
            "readers" => readers(&[]),
            "vector" => Arc::new(vector.clone()),
            _ => arrow_array::new_null_array(field.data_type(), 1),
        })
        .collect();
    RecordBatch::try_new(schema, columns).map_err(|_| WriterError::ChangedRowsInvalid)
}
