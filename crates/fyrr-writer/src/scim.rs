//! SCIM 2.0 resources are encrypted inputs, atomically published under the namespace lease.
use crate::WriterError;
use fyrr_core::{
    Contract,
    config::Config,
    directory::{DirectoryGroup, DirectorySnapshot, DirectoryStatePointer, DirectoryUser},
};
use fyrr_reader::storage::{hash_principal, namespace_store};
use object_store::path::Path;
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;

pub(crate) const USER_SCHEMA: &str = "urn:ietf:params:scim:schemas:core:2.0:User";
pub(crate) const GROUP_SCHEMA: &str = "urn:ietf:params:scim:schemas:core:2.0:Group";
pub(crate) const IDENTITY_EXTENSION: &str =
    "urn:fyrr:params:scim:schemas:extension:identity:2.0:User";
const PATCH_SCHEMA: &str = "urn:ietf:params:scim:api:messages:2.0:PatchOp";

#[derive(Debug, Clone, Copy)]
pub(crate) struct ScimError {
    pub status: u16,
    pub kind: &'static str,
}
impl ScimError {
    pub(crate) fn new(status: u16, kind: &'static str) -> Self {
        Self { status, kind }
    }
}
type Result<T> = std::result::Result<T, ScimError>;
impl From<fyrr_reader::storage::StorageError> for ScimError {
    fn from(code: fyrr_reader::storage::StorageError) -> Self {
        WriterError::from(code).into()
    }
}
impl From<WriterError> for ScimError {
    fn from(_: WriterError) -> Self {
        Self::new(503, "temporarilyUnavailable")
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScimState {
    pub users: BTreeMap<String, Value>,
    pub groups: BTreeMap<String, Value>,
    #[serde(default)]
    pub replays: BTreeMap<String, String>,
}
impl std::fmt::Debug for ScimState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ScimState(<redacted>)")
    }
}
fn text<'a>(resource: &'a Value, field: &str) -> Result<&'a str> {
    resource
        .get(field)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty() && s.len() <= 4096)
        .ok_or(ScimError::new(400, "invalidValue"))
}
fn clean_name(value: &str) -> String {
    value.trim().to_lowercase()
}
fn email(resource: &Value) -> Result<String> {
    let emails = resource.get("emails").and_then(Value::as_array);
    let value = emails
        .and_then(|list| {
            list.iter()
                .find(|v| v["primary"].as_bool() == Some(true))
                .or_else(|| list.first())
        })
        .and_then(|v| v["value"].as_str())
        .or_else(|| resource["userName"].as_str())
        .ok_or(ScimError::new(400, "invalidValue"))?;
    if !value.contains('@') || value.len() > 4096 {
        return Err(ScimError::new(400, "invalidValue"));
    }
    Ok(clean_name(value))
}
fn canonical_field(value: &str) -> Option<&'static str> {
    [
        "userName",
        "active",
        "emails",
        "displayName",
        "externalId",
        "name",
        "members",
        IDENTITY_EXTENSION,
    ]
    .into_iter()
    .find(|v| v.eq_ignore_ascii_case(value))
}
fn validate(resource: &Value, users: bool) -> Result<()> {
    if !resource.is_object() {
        return Err(ScimError::new(400, "invalidSyntax"));
    }
    if users {
        let _ = text(resource, "userName")?;
        let _ = email(resource)?;
        if !resource["active"].is_boolean() {
            return Err(ScimError::new(400, "invalidValue"));
        }
        if let Some(emails) = resource.get("emails") {
            let emails = emails
                .as_array()
                .filter(|v| v.len() <= 64)
                .ok_or(ScimError::new(400, "invalidValue"))?;
            if emails.iter().any(|v| text(v, "value").is_err())
                || emails
                    .iter()
                    .filter(|v| v["primary"].as_bool() == Some(true))
                    .count()
                    > 1
            {
                return Err(ScimError::new(400, "invalidValue"));
            }
        }
        if let Some(extension) = resource.get(IDENTITY_EXTENSION) {
            let subjects = extension["subjects"]
                .as_array()
                .filter(|s| s.len() <= 64)
                .ok_or(ScimError::new(400, "invalidValue"))?;
            for subject in subjects {
                let _ = text(subject, "issuer")?;
                let _ = text(subject, "subject")?;
            }
        }
    } else {
        let _ = text(resource, "displayName")?;
        if let Some(members) = resource.get("members") {
            let members = members
                .as_array()
                .filter(|s| s.len() <= 100_000)
                .ok_or(ScimError::new(400, "invalidValue"))?;
            for member in members {
                let _ = text(member, "value")?;
                if member
                    .get("type")
                    .is_some_and(|v| !matches!(v.as_str(), Some("User" | "Group")))
                {
                    return Err(ScimError::new(400, "invalidValue"));
                }
            }
        }
    }
    if serde_json::to_vec(resource)
        .map_err(|_| ScimError::new(400, "invalidSyntax"))?
        .len()
        > 1024 * 1024
    {
        return Err(ScimError::new(413, "tooLarge"));
    }
    Ok(())
}
fn normalize(body: &Value, users: bool, id: &str) -> Result<Value> {
    let object = body
        .as_object()
        .ok_or(ScimError::new(400, "invalidSyntax"))?;
    if !body["schemas"].as_array().is_some_and(|v| {
        v.iter()
            .any(|s| s.as_str() == Some(if users { USER_SCHEMA } else { GROUP_SCHEMA }))
    }) {
        return Err(ScimError::new(400, "invalidValue"));
    }
    let mut value = json!({"id":id,"schemas":[if users{USER_SCHEMA}else{GROUP_SCHEMA}]});
    if users {
        value["active"] = json!(true);
    }
    for (key, item) in object {
        if matches!(key.as_str(), "schemas" | "id" | "meta" | "groups") {
            continue;
        }
        let field = canonical_field(key).ok_or(ScimError::new(400, "invalidValue"))?;
        if users && field == "members"
            || !users && !matches!(field, "members" | "displayName" | "externalId")
        {
            return Err(ScimError::new(400, "invalidValue"));
        }
        value[field] = item.clone();
    }
    if value.get(IDENTITY_EXTENSION).is_some() {
        value["schemas"]
            .as_array_mut()
            .unwrap_or_else(|| unreachable!("normalize installs the schemas array"))
            .push(json!(IDENTITY_EXTENSION));
    }
    validate(&value, users)?;
    Ok(canonical_json(&value))
}
// Object order is not part of SCIM semantics, including retry fingerprints and ETags.
fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), canonical_json(v)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(canonical_json).collect()),
        other => other.clone(),
    }
}
fn etag(resource: &Value) -> String {
    format!(
        "W/\"{}\"",
        &fyrr_reader::embedding::digest(
            &serde_json::to_vec(resource).unwrap_or_else(|_| unreachable!("JSON values serialize"))
        )[7..]
    )
}
pub(crate) fn response(resource: &Value, users: bool) -> Value {
    let mut value = resource.clone();
    value["meta"] = json!({"resourceType":if users{"User"}else{"Group"},"version":etag(resource),"location":format!("/scim/v2/{}/{}",if users{"Users"}else{"Groups"},resource["id"].as_str().unwrap_or_default())});
    value
}
fn patch(resource: &Value, body: &Value, users: bool) -> Result<Value> {
    if !body["schemas"]
        .as_array()
        .is_some_and(|s| s.iter().any(|v| v.as_str() == Some(PATCH_SCHEMA)))
    {
        return Err(ScimError::new(400, "invalidSyntax"));
    }
    let ops = body["Operations"]
        .as_array()
        .filter(|v| !v.is_empty() && v.len() <= 100)
        .ok_or(ScimError::new(400, "invalidSyntax"))?;
    let mut value = resource.clone();
    for operation in ops {
        let op = text(operation, "op")?.to_lowercase();
        if !matches!(op.as_str(), "add" | "replace" | "remove") {
            return Err(ScimError::new(400, "invalidSyntax"));
        }
        let Some(path) = operation.get("path").and_then(Value::as_str) else {
            if op == "remove" {
                return Err(ScimError::new(400, "noTarget"));
            }
            let fields = operation["value"]
                .as_object()
                .ok_or(ScimError::new(400, "invalidValue"))?;
            for (field, item) in fields {
                let field = canonical_field(field).ok_or(ScimError::new(400, "invalidPath"))?;
                apply(&mut value, field, &op, item)?;
            }
            continue;
        };
        if path.len() > 4096 {
            return Err(ScimError::new(400, "invalidPath"));
        }
        if let Some((field, tail)) = path.split_once('[') {
            let field = canonical_field(field)
                .filter(|f| matches!(*f, "members" | "emails"))
                .ok_or(ScimError::new(400, "invalidPath"))?;
            let (filter, suffix) = tail
                .split_once(']')
                .ok_or(ScimError::new(400, "invalidFilter"))?;
            let (filter_field, expected) = filter
                .split_once(" eq \"")
                .and_then(|(f, v)| v.strip_suffix('"').map(|v| (f, v)))
                .filter(|(f, _)| matches!(*f, "value" | "type"))
                .ok_or(ScimError::new(400, "invalidFilter"))?;
            let sub = if suffix.is_empty() {
                None
            } else {
                Some(
                    suffix
                        .strip_prefix('.')
                        .filter(|f| matches!(*f, "value" | "type" | "primary" | "display"))
                        .ok_or(ScimError::new(400, "invalidPath"))?,
                )
            };
            let array = value[field]
                .as_array_mut()
                .ok_or(ScimError::new(400, "noTarget"))?;
            let selected: Vec<_> = array
                .iter()
                .enumerate()
                .filter(|(_, v)| {
                    v[filter_field]
                        .as_str()
                        .is_some_and(|v| v.eq_ignore_ascii_case(expected))
                })
                .map(|(i, _)| i)
                .collect();
            if selected.is_empty() {
                return Err(ScimError::new(400, "noTarget"));
            }
            if let Some(sub) = sub {
                for i in selected {
                    apply(&mut array[i], sub, &op, &operation["value"])?;
                }
            } else if op == "remove" {
                for i in selected.into_iter().rev() {
                    array.remove(i);
                }
            } else {
                for i in selected {
                    array[i] = operation["value"].clone();
                }
            }
        } else if let Some((field, sub)) = path.split_once('.') {
            let field = canonical_field(field)
                .filter(|f| *f == "name")
                .ok_or(ScimError::new(400, "invalidPath"))?;
            let sub = [
                "formatted",
                "familyName",
                "givenName",
                "middleName",
                "honorificPrefix",
                "honorificSuffix",
            ]
            .into_iter()
            .find(|f| f.eq_ignore_ascii_case(sub))
            .ok_or(ScimError::new(400, "invalidPath"))?;
            if value.get(field).is_none() && op != "remove" {
                value[field] = json!({});
            }
            if !value[field].is_object() {
                return Err(ScimError::new(400, "noTarget"));
            }
            apply(&mut value[field], sub, &op, &operation["value"])?;
        } else {
            let field = canonical_field(path).ok_or(ScimError::new(400, "invalidPath"))?;
            apply(&mut value, field, &op, &operation["value"])?;
        }
    }
    // Revalidate the complete result once: a later failure discards every preceding operation.
    normalize(&value, users, text(resource, "id")?)
}
fn apply(resource: &mut Value, field: &str, op: &str, item: &Value) -> Result<()> {
    if op == "remove" {
        if resource
            .as_object_mut()
            .unwrap_or_else(|| unreachable!("validated SCIM resource object"))
            .remove(field)
            .is_none()
        {
            return Err(ScimError::new(400, "noTarget"));
        }
    } else if op == "add" && matches!(field, "members" | "emails") {
        let additions = item.as_array().ok_or(ScimError::new(400, "invalidValue"))?;
        let existing = resource
            .as_object_mut()
            .unwrap_or_else(|| unreachable!("validated SCIM resource object"))
            .entry(field)
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .ok_or(ScimError::new(400, "invalidValue"))?;
        for value in additions {
            if !existing.contains(value) {
                existing.push(value.clone());
            }
        }
    } else {
        if item.is_null() {
            return Err(ScimError::new(400, "invalidValue"));
        }
        resource[field] = item.clone();
    }
    Ok(())
}
impl ScimState {
    pub(crate) async fn load(
        config: &Config,
        snapshot: Option<&DirectorySnapshot>,
    ) -> Result<Self> {
        let Some(pointer) = snapshot.and_then(|s| s.state.as_ref()) else {
            return Ok(Self::default());
        };
        let (store, root) = namespace_store(config)?;
        let state: Self = crate::secrets::read_json(
            config,
            store.as_ref(),
            &Path::from(format!("{root}/{}", pointer.object)),
        )
        .await?;
        if fyrr_reader::embedding::digest(
            &serde_json::to_vec(&state)
                .map_err(|_| ScimError::new(503, "temporarilyUnavailable"))?,
        ) != pointer.digest
        {
            return Err(ScimError::new(503, "temporarilyUnavailable"));
        }
        for value in state.users.values() {
            validate(value, true)?;
        }
        for value in state.groups.values() {
            validate(value, false)?;
        }
        Ok(state)
    }
    fn snapshot(&self, config: &Config) -> Result<DirectorySnapshot> {
        let mut snapshot = crate::directory_pull::base(config, "scim");
        let mut keys = BTreeMap::new();
        for (id, user) in &self.users {
            let email = email(user)?;
            let corporate = crate::directory_pull::corporate(config, &email);
            let key = hash_principal(
                config,
                "key",
                &if corporate {
                    email.clone()
                } else {
                    format!("scim-unmapped:{id}")
                },
            )?;
            keys.insert(id.clone(), key.clone());
            let aliases = user["emails"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|v| v["value"].as_str())
                .filter(|e| corporate && crate::directory_pull::corporate(config, &clean_name(e)))
                .map(|e| hash_principal(config, "key", &clean_name(e)))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let subjects = user
                .get(IDENTITY_EXTENSION)
                .and_then(|e| e["subjects"].as_array())
                .into_iter()
                .flatten()
                .map(|s| {
                    if Some(text(s, "issuer")?) != config.identity.directory.issuer.as_deref() {
                        return Err(ScimError::new(400, "invalidValue"));
                    }
                    crate::directory::subject_key(config, text(s, "issuer")?, text(s, "subject")?)
                        .map_err(ScimError::from)
                })
                .collect::<Result<Vec<_>>>()?;
            snapshot.users.push(DirectoryUser {
                key,
                token: hash_principal(config, "tok", &format!("usr:{email}"))?,
                active: user["active"].as_bool() == Some(true) && corporate,
                aliases,
                subjects,
            });
        }
        for (id, group) in &self.groups {
            let mut members = Vec::new();
            for member in group["members"].as_array().into_iter().flatten() {
                let reference = text(member, "value")?;
                if let Some(key) = keys.get(reference) {
                    if member["type"].as_str() == Some("Group") {
                        return Err(ScimError::new(400, "invalidValue"));
                    }
                    members.push(key.clone());
                } else if self.groups.contains_key(reference) {
                    if member["type"].as_str() == Some("User") {
                        return Err(ScimError::new(400, "invalidValue"));
                    }
                    members.push(hash_principal(
                        config,
                        "tok",
                        &format!("grp:scim:{reference}"),
                    )?);
                } else {
                    return Err(ScimError::new(400, "invalidValue"));
                }
            }
            snapshot.groups.push(DirectoryGroup {
                token: hash_principal(config, "tok", &format!("grp:scim:{id}"))?,
                members,
            });
        }
        snapshot
            .validate()
            .map_err(|_| ScimError::new(409, "uniqueness"))?;
        Ok(snapshot)
    }
}
/// A single resource mutation, including its optimistic concurrency precondition.
pub(crate) struct Mutation {
    pub users: bool,
    pub method: String,
    pub id: Option<String>,
    pub version: Option<String>,
    pub idempotency: Option<String>,
    pub body: Value,
}
impl std::fmt::Debug for Mutation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Mutation(<redacted>)")
    }
}
pub(crate) async fn mutate(
    config: &Config,
    request: Mutation,
) -> Result<(u16, Value, Option<chrono::DateTime<chrono::Utc>>)> {
    crate::lease::LeaseGuard::with(config, |lease| async move {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let head = lease.head().await?;
            let revision = head
                .identity
                .generation
                .checked_add(1)
                .ok_or(ScimError::new(503, "temporarilyUnavailable"))?;
            let current = crate::directory_pull::current(config, &head).await?;
            if current.as_ref().is_some_and(|s| s.provider != "scim") {
                return Err(ScimError::new(409, "invalidValue"));
            }
            let mut state = ScimState::load(config, current.as_ref()).await?;
            let value = match apply_mutation(&mut state, &request, revision)? {
                MutationResult::Unchanged(body) => return Ok((200, body, None)),
                MutationResult::Changed(value) => value,
            };
            let mut snapshot = state.snapshot(config)?;
            if let Some(current) = &current {
                snapshot.links = current.links.clone();
            }
            let bytes =
                serde_json::to_vec(&state).map_err(|_| ScimError::new(400, "invalidSyntax"))?;
            let digest = fyrr_reader::embedding::digest(&bytes);
            let object = format!("directory/state-{}.json", &digest[7..]);
            let (store, root) = namespace_store(config)?;
            crate::secrets::put(
                config,
                store.as_ref(),
                &Path::from(format!("{root}/{object}")),
                &state,
            )
            .await?;
            snapshot.state = Some(DirectoryStatePointer { object, digest });
            crate::directory::publish_held(config, &snapshot, &lease, Some(snapshot.built_at))
                .await?;
            let body = value
                .as_ref()
                .map_or(Value::Null, |v| response(v, request.users));
            Ok((
                if request.method == "POST" {
                    201
                } else if request.method == "DELETE" {
                    204
                } else {
                    200
                },
                body,
                Some(snapshot.built_at),
            ))
        })
        .await
        .unwrap_or_else(|_| Err(ScimError::new(503, "temporarilyUnavailable")))
    })
    .await
}
pub(crate) async fn resources(config: &Config, users: bool) -> Result<Vec<Value>> {
    let head = fyrr_reader::storage::optional_head(config)
        .await?
        .ok_or(ScimError::new(503, "temporarilyUnavailable"))?;
    let snapshot = crate::directory_pull::current(config, &head).await?;
    let state = ScimState::load(config, snapshot.as_ref()).await?;
    Ok(if users { state.users } else { state.groups }
        .values()
        .map(|v| response(v, users))
        .collect())
}

/// Discovery describes the supported SCIM attributes, including issuer-scoped bindings.
pub(crate) fn schemas() -> Vec<Value> {
    fn attribute(name: &str, kind: &str, multi: bool, required: bool, mutability: &str) -> Value {
        json!({"name":name,"type":kind,"multiValued":multi,"description":"Provisioned directory attribute","required":required,"caseExact":false,"mutability":mutability,"returned":"default","uniqueness":"none"})
    }
    let text = |name| attribute(name, "string", false, false, "readWrite");
    let mut emails = attribute("emails", "complex", true, false, "readWrite");
    emails["subAttributes"] = json!([
        text("value"),
        text("type"),
        text("display"),
        attribute("primary", "boolean", false, false, "readWrite")
    ]);
    let mut name = attribute("name", "complex", false, false, "readWrite");
    name["subAttributes"] = json!([
        text("formatted"),
        text("familyName"),
        text("givenName"),
        text("middleName"),
        text("honorificPrefix"),
        text("honorificSuffix")
    ]);
    let mut members = attribute("members", "complex", true, false, "readWrite");
    members["subAttributes"] = json!([
        attribute("value", "string", false, true, "immutable"),
        text("type"),
        text("display")
    ]);
    let mut subjects = attribute("subjects", "complex", true, false, "readWrite");
    subjects["subAttributes"] = json!([
        attribute("issuer", "string", false, true, "readWrite"),
        attribute("subject", "string", false, true, "readWrite")
    ]);
    vec![
        json!({"schemas":["urn:ietf:params:scim:schemas:core:2.0:Schema"],"id":USER_SCHEMA,"name":"User","description":"Canonical directory user","attributes":[attribute("userName","string",false,true,"readWrite"),attribute("active","boolean",false,false,"readWrite"),emails,name,text("displayName"),text("externalId")]}),
        json!({"schemas":["urn:ietf:params:scim:schemas:core:2.0:Schema"],"id":GROUP_SCHEMA,"name":"Group","description":"Nested directory group","attributes":[attribute("displayName","string",false,true,"readWrite"),members,text("externalId")]}),
        json!({"schemas":["urn:ietf:params:scim:schemas:core:2.0:Schema"],"id":IDENTITY_EXTENSION,"name":"Issuer subjects","description":"Trusted issuer and subject bindings","attributes":[subjects]}),
    ]
}

enum MutationResult {
    Unchanged(Value),
    Changed(Option<Value>),
}
fn apply_mutation(
    state: &mut ScimState,
    request: &Mutation,
    revision: u64,
) -> Result<MutationResult> {
    if state.users.len() + state.groups.len() >= 100_000 && request.method == "POST" {
        return Err(ScimError::new(413, "tooLarge"));
    }
    let replay_body = if request.method == "POST" {
        normalize(&request.body, request.users, "request")?
    } else {
        Value::Null
    };
    let replay = fyrr_reader::embedding::digest(
        &serde_json::to_vec(&(
            request.users,
            canonical_json(&replay_body),
            &request.idempotency,
        ))
        .map_err(|_| ScimError::new(400, "invalidSyntax"))?,
    );
    if request.method == "POST" {
        if let Some(id) = state.replays.get(&replay) {
            let resources = if request.users {
                &state.users
            } else {
                &state.groups
            };
            return resources
                .get(id)
                .map(|v| MutationResult::Unchanged(response(v, request.users)))
                .ok_or(ScimError::new(409, "uniqueness"));
        }
        if state.replays.len() >= 100_000 {
            return Err(ScimError::new(503, "temporarilyUnavailable"));
        }
    }
    let resources = if request.users {
        &mut state.users
    } else {
        &mut state.groups
    };
    let id = if request.method == "POST" {
        let mut random = [0; 16];
        SystemRandom::new()
            .fill(&mut random)
            .map_err(|_| ScimError::new(503, "temporarilyUnavailable"))?;
        hex::encode(random)
    } else {
        request.id.clone().ok_or(ScimError::new(404, "notFound"))?
    };
    let old = resources.get(&id).cloned();
    if request.method != "POST" {
        let old = old.as_ref().ok_or(ScimError::new(404, "notFound"))?;
        if request.version.as_deref() != Some(etag(old).as_str()) {
            return Err(ScimError::new(412, "invalidVers"));
        }
    }
    let mut value = match request.method.as_str() {
        "POST" | "PUT" => Some(normalize(&request.body, request.users, &id)?),
        "PATCH" => Some(patch(
            old.as_ref()
                .unwrap_or_else(|| unreachable!("existing resource required above")),
            &request.body,
            request.users,
        )?),
        "DELETE" => None,
        _ => return Err(ScimError::new(405, "invalidSyntax")),
    };
    if request.users
        && value.as_ref().is_some_and(|v| {
            state.users.iter().any(|(other, u)| {
                *other != id
                    && clean_name(u["userName"].as_str().unwrap_or_default())
                        == clean_name(v["userName"].as_str().unwrap_or_default())
            })
        })
    {
        return Err(ScimError::new(409, "uniqueness"));
    }
    let previous = old
        .as_ref()
        .map(|v| normalize(v, request.users, &id))
        .transpose()?;
    if value == previous {
        return Ok(MutationResult::Unchanged(response(
            old.as_ref()
                .unwrap_or_else(|| unreachable!("existing resource required above")),
            request.users,
        )));
    }
    // A representation restored to earlier values must not restore its ETag:
    // an old conditional request could otherwise replay across an ABA change.
    // Reuse the leased, monotonic identity generation rather than another counter.
    if let Some(value) = &mut value {
        value["meta"] = json!({"version":revision});
    }

    let resources = if request.users {
        &mut state.users
    } else {
        &mut state.groups
    };
    if let Some(value) = &value {
        resources.insert(id.clone(), value.clone());
    } else {
        resources.remove(&id);
        for group in state.groups.values_mut() {
            if let Some(members) = group["members"].as_array_mut() {
                let prior = members.len();
                members.retain(|m| m["value"].as_str() != Some(&id));
                if members.len() != prior {
                    group["meta"] = json!({"version":revision});
                }
            }
        }
    }
    if request.method == "POST" {
        state.replays.insert(replay, id);
    }
    Ok(MutationResult::Changed(value))
}
