mod client_response;
mod server_request;

use crate::api::client_config::ClientConfig;
use crate::api::{ConfigResponse, ConfigService};
use crate::common::remote::remote_client::GrpcRemoteClient;
use crate::common::remote::request::*;
use crate::config::client_response::*;
use crate::config::server_request::*;
use std::collections::HashMap;

pub(crate) struct NacosConfigService {
    remote_client: GrpcRemoteClient,
}

impl NacosConfigService {
    pub fn new(client_config: ClientConfig) -> Self {
        let remote_client = GrpcRemoteClient::new(client_config);
        NacosConfigService { remote_client }
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
                    loop {
                        tokio::select! { biased;
                            deal_with_connection = self.remote_client.deal_with_connection() => {
                                tracing::debug!("after deal_with_connection");
                            },
                            /*receive_server_req = self.remote_client.conn_server_req_payload_rx.recv() => {
                                let (type_url, headers, body_str) = receive_server_req.unwrap();
                                deal_with_server_req(self.remote_client.borrow_mut(), type_url, headers, body_str).await;
                            },*/
                        }
                    }
                });
            })
            .expect("config-remote-client could not spawn thread");
    }
}

async fn deal_with_server_req(
    remote_client: &mut GrpcRemoteClient,
    type_url: String,
    headers: HashMap<String, String>,
    body_str: String,
) {
    if TYPE_CONFIG_CHANGE_NOTIFY_SERVER_REQUEST.eq(&type_url) {
        let server_req = ConfigChangeNotifyServerRequest::from(body_str.as_str());
        let server_req = server_req.headers(headers);
        remote_client
            .reply_client_resp(ConfigChangeNotifyClientResponse::new(
                server_req.get_request_id().clone(),
            ))
            .await;
        tracing::info!(
            "receiver config change, dataId={},group={},namespace={}",
            &server_req.dataId,
            &server_req.group,
            &server_req.tenant
        );
        println!(
            "receiver config change, dataId={},group={},namespace={}",
            &server_req.dataId, &server_req.group, &server_req.tenant
        )
        // todo notify config change
    } else {
        tracing::error!("unknown receiver type_url={}", type_url)
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

    fn listener(self) -> std::sync::mpsc::Receiver<ConfigResponse> {
        todo!()
    }
}
