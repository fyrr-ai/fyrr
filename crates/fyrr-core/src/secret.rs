//! Versioned authenticated-encryption envelope; plaintext lives only in the writer.
use crate::limits::lowercase_hex;
use crate::{Contract, SchemaVersion};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct EncryptedSecret {
    pub schema_version: SchemaVersion,
    pub algorithm: String,
    pub key_id: String,
    pub nonce: String,
    pub ciphertext: String,
}
impl std::fmt::Debug for EncryptedSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EncryptedSecret(<redacted>)")
    }
}
impl Contract for EncryptedSecret {
    const NAME: &'static str = "encrypted-secret";
    const VERSION: SchemaVersion = SchemaVersion::new(1, 0);
    fn validate(&self) -> Result<(), String> {
        if self.algorithm != "aes-256-gcm"
            || !lowercase_hex(&self.key_id, 64)
            || !lowercase_hex(&self.nonce, 24)
            || self.ciphertext.len() < 32
            || self.ciphertext.len() > 34 * 1024 * 1024
            || !self.ciphertext.len().is_multiple_of(2)
            || !lowercase_hex(&self.ciphertext, self.ciphertext.len())
        {
            return Err("encrypted envelope invalid".into());
        }
        Ok(())
    }
}
