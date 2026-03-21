use crate::core::{
    constants::{
        APP_NAME, DEFAULT_AO_TOKEN_PROCESS_ID, DEFAULT_ARWEAVE_URL, DEFAULT_GQL_URL,
        DEFAULT_PAGE_SIZE, DEFAULT_SU_URL, NETWORK_VERSION,
    },
    token::fetch_ao_token_transfers,
};
use anyhow::{Context, Result, bail};
use axum::{
    Json,
    extract::{Path, State},
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
    pub ao_token_process_id: String,
    pub page_size: usize,
}

#[derive(Debug, Deserialize)]
pub struct BlockIdPath {
    block_id: String,
}

pub fn app_state_from_env() -> Result<AppState> {
    Ok(AppState { client: Client::new(), config: app_config_from_env()? })
}

pub async fn handle_route() -> Json<Value> {
    Json(json!({
        "status": "running",
        "name": APP_NAME,
        "version": env!("CARGO_PKG_VERSION"),
        "routes": [
            "/",
            "/v1/token/ao/transfers/{block_id}"
        ],
        "config": {
            "su_url": DEFAULT_SU_URL,
            "arweave_gateway": DEFAULT_ARWEAVE_URL,
            "gql_url": DEFAULT_GQL_URL,
            "ao_token_process_id": DEFAULT_AO_TOKEN_PROCESS_ID,
            "page_size": DEFAULT_PAGE_SIZE,
            "network": NETWORK_VERSION
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

fn into_http_error(error: anyhow::Error) -> (StatusCode, Json<Value>) {
    let message = format_error_chain(&error);
    let status = if message.contains("block-height must be an integer") {
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
    let ao_token_process_id = env::var("AO_LN_INSPECTOR_AO_TOKEN_PROCESS_ID")
        .unwrap_or_else(|_| DEFAULT_AO_TOKEN_PROCESS_ID.to_string());

    let page_size = match env::var("AO_LN_INSPECTOR_PAGE_SIZE") {
        Ok(value) => value.parse::<usize>().context("invalid AO_LN_INSPECTOR_PAGE_SIZE")?,
        Err(_) => DEFAULT_PAGE_SIZE,
    };

    if page_size == 0 {
        bail!("AO_LN_INSPECTOR_PAGE_SIZE must be greater than zero");
    }

    Ok(AppConfig { su_url, arweave_url, gql_url, ao_token_process_id, page_size })
}
