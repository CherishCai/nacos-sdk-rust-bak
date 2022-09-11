pub mod client_config;
pub mod constants;
pub mod error;

pub trait ConfigService {
    /// Get config, return the content.
    fn get_config(self, data_id: String, group: String, timeout_ms: u32) -> error::Result<String>;

    /// Listen the config change.
    fn listen(self, data_id: String, group: String) -> std::sync::mpsc::Receiver<ConfigResponse>;
}

pub struct ConfigResponse {
    /// Namespace/Tenant
    namespace: String,
    /// DataId
    data_id: String,
    /// Group
    group: String,
    /// Content
    content: String,
}
