use crate::core::{
    arweave,
    constants::{
        APP_NAME, DEFAULT_AO_TOKEN_PROCESS_ID, DEFAULT_ARWEAVE_URL, DEFAULT_CU_URL,
        DEFAULT_GQL_URL, DEFAULT_PAGE_SIZE, DEFAULT_SU_URL, NETWORK_VERSION,
    },
    su,
    token::{fetch_ao_token_transfer_with_notices, fetch_ao_token_transfers},
};
use anyhow::{Context, Result, bail};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Value, json};
use std::env;

pub use crate::core::constants::SERVER_PORT;

#[derive(Debug, Clone)]
pub struct AppState {
    pub client: Client,
    pub config: AppConfig,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub su_url: String,
    pub arweave_url: String,
    pub gql_url: String,
    pub cu_url: String,
    pub ao_token_process_id: String,
    pub page_size: usize,
}

#[derive(Debug, Deserialize)]
pub struct BlockIdPath {
    block_id: String,
}

#[derive(Debug, Deserialize)]
pub struct MessageIdPath {
    id: String,
}

#[derive(Debug, Deserialize)]
pub struct TransferQuery {
    #[serde(default, alias = "notice-scan-blocks")]
    notice_scan_blocks: Option<u64>,
}

pub fn app_state_from_env() -> Result<AppState> {
    Ok(AppState { client: Client::new(), config: app_config_from_env()? })
}

pub async fn handle_route(State(state): State<AppState>) -> Json<Value> {
    let (su_probe, arweave_tip) = tokio::join!(
        su::probe_process(&state.client, &state.config.su_url, &state.config.ao_token_process_id),
        arweave::fetch_arweave_tip_height(&state.client, &state.config.arweave_url),
    );
    let su_error = su_probe.as_ref().err().map(std::string::ToString::to_string);
    let arweave_error = arweave_tip.as_ref().err().map(std::string::ToString::to_string);

    Json(json!({
        "status": if su_error.is_none() && arweave_error.is_none() { "ok" } else { "degraded" },
        "name": APP_NAME,
        "version": env!("CARGO_PKG_VERSION"),
        "routes": [
            "/",
            "/openapi.json",
            "/v1/token/ao/transfers/{block_id}",
            "/v1/token/ao/msg/{id}",
            "/v1/token/ao/transfer/{id}"
        ],
        "config": {
            "su_url": state.config.su_url,
            "arweave_gateway": state.config.arweave_url,
            "gql_url": state.config.gql_url,
            "cu_url": state.config.cu_url,
            "ao_token_process_id": state.config.ao_token_process_id,
            "page_size": state.config.page_size,
            "network": NETWORK_VERSION
        },
        "checks": {
            "su": {
                "ok": su_error.is_none(),
                "error": su_error,
            },
            "arweave": {
                "ok": arweave_error.is_none(),
                "tip_height": arweave_tip.ok(),
                "error": arweave_error,
            }
        }
    }))
}

pub async fn handle_ao_token_transfers(
    State(state): State<AppState>,
    Path(BlockIdPath { block_id }): Path<BlockIdPath>,
) -> Result<Json<crate::core::token::TokenTransfersResponse>, (StatusCode, Json<Value>)> {
    fetch_ao_token_transfers(&state.client, &state.config, &block_id)
        .await
        .map(Json)
        .map_err(into_http_error)
}

pub async fn handle_ao_token_message(
    State(state): State<AppState>,
    Path(MessageIdPath { id }): Path<MessageIdPath>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    su::fetch_message_value(
        &state.client,
        &state.config.su_url,
        &state.config.ao_token_process_id,
        &id,
    )
    .await
    .map(Json)
    .map_err(into_http_error)
}

pub async fn handle_ao_token_transfer(
    State(state): State<AppState>,
    Path(MessageIdPath { id }): Path<MessageIdPath>,
    Query(TransferQuery { notice_scan_blocks }): Query<TransferQuery>,
) -> Result<Json<crate::core::token::TokenTransferWithNoticesResponse>, (StatusCode, Json<Value>)> {
    fetch_ao_token_transfer_with_notices(
        &state.client,
        &state.config,
        &id,
        notice_scan_blocks.unwrap_or(1),
    )
    .await
    .map(Json)
    .map_err(into_http_error)
}

fn into_http_error(error: anyhow::Error) -> (StatusCode, Json<Value>) {
    let message = format_error_chain(&error);
    let status = if message.contains("block-height must be an integer")
        || message.contains("is not a Transfer")
        || message.contains("is not a valid AO token Transfer")
    {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::BAD_GATEWAY
    };

    (status, Json(json!({ "error": message })))
}

fn format_error_chain(error: &anyhow::Error) -> String {
    error.chain().map(std::string::ToString::to_string).collect::<Vec<_>>().join(": ")
}

fn app_config_from_env() -> Result<AppConfig> {
    let su_url = env::var("AO_LN_INSPECTOR_SU_URL").unwrap_or_else(|_| DEFAULT_SU_URL.to_string());
    let arweave_url =
        env::var("AO_LN_INSPECTOR_ARWEAVE_URL").unwrap_or_else(|_| DEFAULT_ARWEAVE_URL.to_string());
    let gql_url =
        env::var("AO_LN_INSPECTOR_GQL_URL").unwrap_or_else(|_| DEFAULT_GQL_URL.to_string());
    let cu_url = env::var("AO_LN_INSPECTOR_CU_URL").unwrap_or_else(|_| DEFAULT_CU_URL.to_string());
    let ao_token_process_id = env::var("AO_LN_INSPECTOR_AO_TOKEN_PROCESS_ID")
        .unwrap_or_else(|_| DEFAULT_AO_TOKEN_PROCESS_ID.to_string());

    let page_size = match env::var("AO_LN_INSPECTOR_PAGE_SIZE") {
        Ok(value) => value.parse::<usize>().context("invalid AO_LN_INSPECTOR_PAGE_SIZE")?,
        Err(_) => DEFAULT_PAGE_SIZE,
    };

    if page_size == 0 {
        bail!("AO_LN_INSPECTOR_PAGE_SIZE must be greater than zero");
    }

    Ok(AppConfig { su_url, arweave_url, gql_url, cu_url, ao_token_process_id, page_size })
}
