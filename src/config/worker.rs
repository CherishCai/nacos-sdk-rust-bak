use crate::api::client_config::ClientConfig;
use crate::api::config::ConfigResponse;
use crate::config::util;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Cache Data for Config
#[derive(Clone, Default)]
pub(crate) struct CacheData {
    pub(crate) data_id: String,
    pub(crate) group: String,
    pub(crate) tenant: String,
    /// Default text; text, json, properties, html, xml, yaml ...
    pub(crate) content_type: String,
    pub(crate) content: String,
    pub(crate) md5: String,
    /// whether content was encrypted with encryptedDataKey.
    pub(crate) encrypted_data_key: Option<String>,
    pub(crate) last_modified: i64,

    /// Mark the cache config is not the latest, need to query the server for synchronize
    pub(crate) need_sync_server: bool,

    /// who listen of config change.
    pub(crate) listeners: Arc<Mutex<Vec<Box<crate::api::config::ConfigChangeListenFn>>>>,
}

impl CacheData {
    pub(crate) fn new(data_id: String, group: String, tenant: String) -> Self {
        Self {
            data_id,
            group,
            tenant,
            content_type: "text".to_string(),
            ..Default::default()
        }
    }

    /// Add listener.
    pub(crate) fn add_listener(&mut self, listener: Box<crate::api::config::ConfigChangeListenFn>) {
        loop {
            let listen_lock = self.listeners.try_lock();
            if let Ok(mut mutex) = listen_lock {
                mutex.push(listener);
                break;
            }
        }
    }

    /// Notify listener.
    pub(crate) fn notify_listener(&mut self, config_response: ConfigResponse) {
        loop {
            let listen_lock = self.listeners.try_lock();
            if let Ok(mut mutex) = listen_lock {
                for listen in mutex.iter_mut() {
                    (listen)(config_response.clone());
                }
                break;
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct ConfigWorker {
    pub(crate) client_config: ClientConfig,
    pub(crate) cache_data_map: Arc<Mutex<HashMap<String, CacheData>>>,
}

impl ConfigWorker {
    pub(crate) fn new(client_config: ClientConfig) -> Self {
        Self {
            client_config,
            cache_data_map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Add listener.
    pub(crate) fn add_listener(
        &mut self,
        data_id: String,
        group: String,
        tenant: String,
        listener: Box<crate::api::config::ConfigChangeListenFn>,
    ) {
        let group_key = util::group_key(&data_id, &group, &tenant);
        loop {
            let cache_lock = self.cache_data_map.try_lock();
            if let Ok(mut mutex) = cache_lock {
                if !mutex.contains_key(group_key.as_str()) {
                    mutex.insert(
                        group_key.clone(),
                        CacheData::new(data_id.clone(), group.clone(), tenant.clone()),
                    );
                }
                let _ = mutex
                    .get_mut(group_key.as_str())
                    .map(|c| c.add_listener(listener));
                break;
            }
        }
    }

    /// notify config change
    pub(crate) fn notify_config_change(&mut self, data_id: String, group: String, tenant: String) {
        let group_key = util::group_key(&data_id, &group, &tenant);
        loop {
            let cache_lock = self.cache_data_map.try_lock();
            if let Ok(mut mutex) = cache_lock {
                if !mutex.contains_key(group_key.as_str()) {
                    break;
                }
                let _ = mutex.get_mut(group_key.as_str()).map(|c| {
                    // todo get the newest config to notify
                    c.notify_listener(ConfigResponse::new(
                        c.data_id.clone(),
                        c.group.clone(),
                        c.tenant.clone(),
                        c.content.clone(),
                        c.content_type.clone(),
                    ))
                });
                break;
            }
        }
    }
}
