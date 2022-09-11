mod client_request;
mod client_response;
mod server_request;
mod server_response;

use crate::api::client_config::ClientConfig;
use crate::api::{ConfigResponse, ConfigService};
use crate::common::remote::conn::Connection;
use crate::common::remote::request::server_request::*;
use crate::common::remote::request::*;
use crate::common::remote::response::client_response::*;
use crate::common::util::payload_helper;
use crate::config::client_request::*;
use crate::config::client_response::*;
use crate::config::server_request::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};

pub(crate) struct NacosConfigService {
    client_config: ClientConfig,
    connection: Connection,
    /// (type_url, headers, body_json_str)
    conn_server_req_payload_tx: Sender<(String, HashMap<String, String>, String)>,
    /// (type_url, headers, body_json_str)
    conn_server_req_payload_rx: Receiver<(String, HashMap<String, String>, String)>,

    /// config listen tx
    config_listen_tx_vec: Vec<std::sync::mpsc::Sender<ConfigResponse>>,
}

impl NacosConfigService {
    pub fn new(client_config: ClientConfig) -> Self {
        let connection = Connection::new(client_config.clone());
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        Self {
            client_config,
            connection,
            conn_server_req_payload_tx: tx,
            conn_server_req_payload_rx: rx,

            config_listen_tx_vec: Vec::new(),
        }
    }

    pub(crate) fn start(mut self) {
        std::thread::Builder::new()
            .name("config-remote-client".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                    .expect("config-remote-client runtime initialization failed");

                runtime.block_on(async move {
                    // todo temp to invoke ConfigBatchListenClientRequest
                    let tenant = self.client_config.namespace.clone();
                    let mut req = ConfigBatchListenClientRequest::new(true);
                    let req = req.add_config_listen_context(ConfigListenContext::new(
                        "hongwen.properties".to_string(),
                        "LOVE".to_string(),
                        tenant,
                        String::from(""),
                    ));
                    let resp = self.connection.send_client_req(req.clone()).await;
                    let resp = self.connection.send_client_req(req.clone()).await;
                    // temp end
                    loop {
                        tokio::select! { biased;
                            // deal with next_server_req_payload, basic conn interaction logic.
                            server_req_payload = self.connection.next_server_req_payload() => {
                                let (type_url, headers, body_json_str) = payload_helper::covert_payload(server_req_payload);
                                if TYPE_CLIENT_DETECTION_SERVER_REQUEST.eq(&type_url) {
                                    let de = ClientDetectionServerRequest::from(body_json_str.as_str());
                                    let de = de.headers(headers);
                                    self.connection
                                        .reply_client_resp(ClientDetectionClientResponse::new(de.get_request_id().clone()))
                                        .await;
                                } else if TYPE_CONNECT_RESET_SERVER_REQUEST.eq(&type_url) {
                                    let de = ConnectResetServerRequest::from(body_json_str.as_str());
                                    let de = de.headers(headers);
                                    self.connection
                                        .reply_client_resp(ConnectResetClientResponse::new(de.get_request_id().clone()))
                                        .await;
                                } else {
                                    // publish a server_req_payload, conn_server_req_payload_rx receive it once.
                                    if let Err(_) = self.conn_server_req_payload_tx.clone().send((type_url, headers, body_json_str)).await {
                                        tracing::error!("receiver dropped")
                                    }
                                }
                            },
                            // receive a server_req from conn_server_req_payload_tx
                            receive_server_req = self.conn_server_req_payload_rx.recv() => {
                                let (type_url, headers, body_str) = receive_server_req.unwrap();
                                if TYPE_CONFIG_CHANGE_NOTIFY_SERVER_REQUEST.eq(&type_url) {
                                    let server_req = ConfigChangeNotifyServerRequest::from(body_str.as_str());
                                    let server_req = server_req.headers(headers);
                                    self.connection
                                        .reply_client_resp(ConfigChangeNotifyClientResponse::new(server_req.get_request_id().clone()))
                                        .await;
                                    let tenant = server_req.tenant.or(Some("".to_string())).unwrap();
                                    tracing::info!(
                                        "receiver config change, dataId={},group={},namespace={}",
                                        &server_req.dataId,
                                        &server_req.group,
                                        tenant.clone()
                                    );
                                    println!(
                                        "receiver config change, dataId={},group={},namespace={}",
                                        &server_req.dataId, &server_req.group, tenant.clone()
                                    )
                                    // todo notify config change
                                } else {
                                    tracing::error!("unknown receiver type_url={}", type_url)
                                }
                            },
                        }
                    }
                });
            })
            .expect("config-remote-client could not spawn thread");
    }
}

impl ConfigService for NacosConfigService {
    fn get_config(
        mut self,
        data_id: String,
        group: String,
        timeout_ms: u32,
    ) -> crate::api::error::Result<String> {
        todo!()
    }

    fn listen(
        mut self,
        data_id: String,
        group: String,
    ) -> std::sync::mpsc::Receiver<ConfigResponse> {
        /*
        // todo 抽离到统一的发起地方
        let tenant = self.client_config.namespace.clone();
        let req = ConfigBatchListenClientRequest::new(true);
        req.add_config_listen_context(ConfigListenContext::new(
            data_id,
            group,
            tenant,
            String::from(""),
        ));
        // todo 抽离到统一的发起地方，取得结果
        let resp = self.connection.send_client_req(req).await;
        */

        let (tx, rx) = std::sync::mpsc::channel();
        self.config_listen_tx_vec.push(tx);
        return rx;
    }
}

#[cfg(test)]
mod tests {
    use crate::api::client_config::ClientConfig;
    use crate::api::ConfigService;
    use crate::config::NacosConfigService;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_config_service() {
        let mut config_service = NacosConfigService::new(ClientConfig::new());
        config_service.start();

        sleep(Duration::from_secs(30)).await;
    }
}
