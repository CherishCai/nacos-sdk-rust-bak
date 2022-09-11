use crate::api::{client_config, error};

pub trait ConfigService {
    /// Get config, return the content.
    fn get_config(&self, data_id: String, group: String, timeout_ms: u32) -> error::Result<String>;

    /// Listen the config change.
    fn listen(
        &mut self,
        data_id: String,
        group: String,
    ) -> error::Result<std::sync::mpsc::Receiver<ConfigResponse>>;
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

impl ConfigResponse {
    pub fn get_namespace(&self) -> &String {
        &self.namespace
    }
    pub fn get_data_id(&self) -> &String {
        &self.data_id
    }
    pub fn get_group(&self) -> &String {
        &self.group
    }
    pub fn get_content(&self) -> &String {
        &self.content
    }
}

pub struct ConfigServiceBuilder {
    client_config: client_config::ClientConfig,
}

impl Default for ConfigServiceBuilder {
    fn default() -> Self {
        ConfigServiceBuilder {
            client_config: client_config::ClientConfig::new(),
        }
    }
}

impl ConfigServiceBuilder {
    pub fn new(client_config: client_config::ClientConfig) -> Self {
        ConfigServiceBuilder { client_config }
    }

    /// Builds a new [`ConfigService`].
    pub async fn build(self) -> impl ConfigService {
        let mut config_service = crate::config::NacosConfigService::new(self.client_config);
        config_service.start().await;
        config_service
    }
}

#[cfg(test)]
mod tests {
    use crate::api::config::ConfigService;
    use crate::api::config::ConfigServiceBuilder;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_api_config_service() {
        let mut config_service = ConfigServiceBuilder::default().build().await;
        let config =
            config_service.get_config("hongwen.properties".to_string(), "LOVE".to_string(), 3000);
        println!("get the config {}", config.expect("None"));
        let rx = config_service
            .listen("hongwen.properties".to_string(), "LOVE".to_string())
            .unwrap();
        std::thread::Builder::new()
            .name("wait-listen-rx".into())
            .spawn(|| {
                for resp in rx {
                    println!("listen the config {}", resp.get_content());
                }
            })
            .expect("wait-listen-rx could not spawn thread");

        sleep(Duration::from_secs(30)).await;
    }
}
