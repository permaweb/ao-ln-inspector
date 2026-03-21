use crate::core::{
    arweave::{ArweaveWindow, fetch_arweave_window},
    types::{HistoryEdge, HistoryNode, ProcessHistoryResponse},
};
use anyhow::{Context, Result, bail};
use reqwest::{Client, Url};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::Value;

#[derive(Debug)]
pub struct ProcessEdgesPage {
    pub page_count: usize,
    pub edges: Vec<HistoryEdge>,
}

#[derive(Debug)]
pub struct AssignmentBlockEdges {
    pub page_count: usize,
    pub edges: Vec<HistoryEdge>,
    pub arweave_window: ArweaveWindow,
}

#[derive(Debug, Deserialize)]
struct SuErrorEnvelope {
    error: Option<String>,
}

pub async fn fetch_process_edges_for_assignment_block(
    client: &Client,
    su_url: &str,
    arweave_url: &str,
    process_id: &str,
    block_height: &str,
    page_size: usize,
) -> Result<AssignmentBlockEdges> {
    let arweave_window = fetch_arweave_window(client, arweave_url, block_height).await?;
    let process_page = fetch_process_edges_for_window(
        client,
        su_url,
        process_id,
        arweave_window.from_timestamp_ms,
        arweave_window.to_timestamp_ms,
        page_size,
    )
    .await
    .with_context(|| format!("failed fetching SU history for process {process_id}"))?;

    if process_page.page_count == 0 {
        bail!("Process {process_id} not found on SU");
    }

    let edges = process_page
        .edges
        .into_iter()
        .filter(|edge| edge.node.assignment.matches_block_height(block_height))
        .collect();

    Ok(AssignmentBlockEdges { page_count: process_page.page_count, edges, arweave_window })
}

pub async fn fetch_message_value(
    client: &Client,
    base_url: &str,
    process_id: &str,
    message_id: &str,
) -> Result<Value> {
    fetch_message_json(client, base_url, process_id, message_id).await
}

pub async fn fetch_message_node(
    client: &Client,
    base_url: &str,
    process_id: &str,
    message_id: &str,
) -> Result<HistoryNode> {
    fetch_message_json(client, base_url, process_id, message_id).await
}

async fn fetch_message_json<T: DeserializeOwned>(
    client: &Client,
    base_url: &str,
    process_id: &str,
    message_id: &str,
) -> Result<T> {
    let url = message_url(base_url, process_id, message_id)?;
    let response = client.get(url).send().await.context("failed to contact SU")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        let su_error =
            serde_json::from_str::<SuErrorEnvelope>(&body).ok().and_then(|envelope| envelope.error);
        let detail = su_error.unwrap_or(body);
        bail!("SU returned {}: {}", status, detail.trim());
    }

    response.json::<T>().await.context("failed to deserialize SU response")
}

pub async fn fetch_process_edges_for_window(
    client: &Client,
    base_url: &str,
    process_id: &str,
    from_timestamp_ms: i64,
    to_timestamp_ms: Option<i64>,
    page_size: usize,
) -> Result<ProcessEdgesPage> {
    let mut from = None;
    let mut page_count = 0;
    let mut edges = Vec::new();

    loop {
        let Some(page) = fetch_process_page_optional(
            client,
            base_url,
            process_id,
            from.as_deref(),
            to_timestamp_ms,
            Some(from_timestamp_ms),
            page_size,
        )
        .await?
        else {
            return Ok(ProcessEdgesPage { page_count, edges });
        };

        page_count += 1;
        let next_cursor = page.edges.last().map(|edge| edge.cursor.clone());
        edges.extend(page.edges);

        if !page.page_info.has_next_page {
            break;
        }

        let cursor =
            next_cursor.context("SU response reported has_next_page=true without a cursor")?;
        from = Some(cursor);
    }

    Ok(ProcessEdgesPage { page_count, edges })
}

async fn fetch_process_page_optional(
    client: &Client,
    base_url: &str,
    process_id: &str,
    from: Option<&str>,
    to: Option<i64>,
    min_from: Option<i64>,
    page_size: usize,
) -> Result<Option<ProcessHistoryResponse>> {
    let url = process_history_url(base_url, process_id, from, to, min_from, page_size)?;
    let response = client.get(url).send().await.context("failed to contact SU")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        let su_error =
            serde_json::from_str::<SuErrorEnvelope>(&body).ok().and_then(|envelope| envelope.error);
        let error_text = su_error.as_deref().unwrap_or(body.as_str());

        if status.as_u16() == 400 && is_missing_su_process_error(error_text) {
            return Ok(None);
        }

        let detail = su_error.unwrap_or(body);
        bail!("SU returned {}: {}", status, detail.trim());
    }

    let page = response
        .json::<ProcessHistoryResponse>()
        .await
        .context("failed to deserialize SU response")?;

    Ok(Some(page))
}

fn process_history_url(
    base_url: &str,
    process_id: &str,
    from: Option<&str>,
    to: Option<i64>,
    min_from: Option<i64>,
    page_size: usize,
) -> Result<Url> {
    let mut url = Url::parse(&format!("{}/{}", base_url.trim_end_matches('/'), process_id))
        .context("invalid SU base URL")?;

    {
        let mut query = url.query_pairs_mut();
        query.append_pair("process-id", process_id);
        query.append_pair("limit", &page_size.to_string());
        if let Some(from) = from {
            query.append_pair("from", from);
        } else if let Some(min_from) = min_from {
            query.append_pair("from", &min_from.to_string());
        }
        if let Some(to) = to {
            query.append_pair("to", &to.to_string());
        }
    }

    Ok(url)
}

fn message_url(base_url: &str, process_id: &str, message_id: &str) -> Result<Url> {
    let mut url = Url::parse(&format!("{}/{}", base_url.trim_end_matches('/'), message_id))
        .context("invalid SU base URL")?;

    {
        let mut query = url.query_pairs_mut();
        query.append_pair("process-id", process_id);
    }

    Ok(url)
}

fn is_missing_su_process_error(message: &str) -> bool {
    message.contains("Message or Process not found")
        || message.contains("Process scheduler not found")
}
