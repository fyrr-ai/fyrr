//! The public configuration schema, extended by installed connector providers
//! in the full build.

use schemars::generate::SchemaSettings;
use serde_json::Value;

/// The configuration schema without connector providers; the full build's
/// `fyrr config schema` joins the installed providers' schemas into it.
pub fn config_schema() -> Value {
    let mut value = SchemaSettings::draft2020_12()
        .into_generator()
        .into_root_schema_for::<crate::config::Config>()
        .to_value();
    value["x-fyrr-schema-version"] = crate::config::CONFIG_SCHEMA_VERSION.to_string().into();
    value
}
