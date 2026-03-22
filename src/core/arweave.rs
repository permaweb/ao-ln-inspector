use crate::core::{
    constants::{
        AO_LN_AUTHORITY, GQL_BATCH_SIZE, GQL_NOTICE_BATCH_SIZE,
        SETTLED_NOTICES_BY_CORRELATION_QUERY, SETTLEMENT_HEIGHTS_QUERY,
    },
    types::{HistoryEdge, normalize_block_height},
};
use anyhow::{Context, Result, bail};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Serialize)]
pub struct ArweaveWindow {
    pub arweave_gateway: String,
    pub target_block_height: u64,
    pub next_block_height: u64,
    pub from_timestamp_ms: i64,
    pub to_timestamp_ms: Option<i64>,
}

#[derive(Debug, Clone, Default)]
pub struct SettlementMetadata {
    pub settlement_block_height: Option<u64>,
    pub bundled_in_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SettledNotice {
    pub message_id: String,
    pub action: String,
    pub correlation_id: String,
    pub owner_address: String,
    pub settlement_block_height: Option<u64>,
    pub bundled_in_id: Option<String>,
    pub recipient: Option<String>,
    pub tags: Vec<crate::core::types::Tag>,
}

#[derive(Debug, Deserialize)]
struct ArweaveBlock {
    timestamp: i64,
}

#[derive(Debug, Deserialize)]
struct GraphQlEnvelope {
    data: Option<GraphQlData>,
    errors: Option<Vec<GraphQlError>>,
}

#[derive(Debug, Deserialize)]
struct GraphQlData {
    transactions: GraphQlTransactions,
}

#[derive(Debug, Deserialize)]
struct GraphQlTransactions {
    edges: Vec<GraphQlEdge>,
}

#[derive(Debug, Deserialize)]
struct GraphQlEdge {
    node: GraphQlNode,
}

#[derive(Debug, Deserialize)]
struct GraphQlNode {
    id: String,
    block: Option<GraphQlBlock>,
    #[serde(rename = "bundledIn")]
    bundled_in: Option<GraphQlBundledIn>,
}

#[derive(Debug, Deserialize)]
struct GraphQlBlock {
    height: u64,
}

#[derive(Debug, Deserialize)]
struct GraphQlBundledIn {
    id: String,
}

#[derive(Debug, Deserialize)]
struct GraphQlError {
    message: String,
}

#[derive(Debug, Deserialize)]
struct NoticeGraphQlEnvelope {
    data: Option<NoticeGraphQlData>,
    errors: Option<Vec<GraphQlError>>,
}

#[derive(Debug, Deserialize)]
struct NoticeGraphQlData {
    transactions: NoticeGraphQlTransactions,
}

#[derive(Debug, Deserialize)]
struct NoticeGraphQlTransactions {
    #[serde(rename = "pageInfo")]
    page_info: NoticeGraphQlPageInfo,
    edges: Vec<NoticeGraphQlEdge>,
}

#[derive(Debug, Deserialize)]
struct NoticeGraphQlPageInfo {
    #[serde(rename = "hasNextPage")]
    has_next_page: bool,
}

#[derive(Debug, Deserialize)]
struct NoticeGraphQlEdge {
    cursor: String,
    node: NoticeGraphQlNode,
}

#[derive(Debug, Deserialize)]
struct NoticeGraphQlNode {
    id: String,
    owner: NoticeGraphQlOwner,
    recipient: Option<String>,
    tags: Vec<crate::core::types::Tag>,
    block: Option<GraphQlBlock>,
    #[serde(rename = "bundledIn")]
    bundled_in: Option<GraphQlBundledIn>,
}

#[derive(Debug, Deserialize)]
struct NoticeGraphQlOwner {
    address: String,
}

pub async fn fetch_arweave_window(
    client: &Client,
    arweave_url: &str,
    block_height: &str,
) -> Result<ArweaveWindow> {
    let target_block_height = parse_block_height(block_height)?;
    let next_block_height = target_block_height.saturating_add(1);

    let target_block = fetch_arweave_block(client, arweave_url, target_block_height)
        .await
        .with_context(|| format!("failed fetching Arweave block {target_block_height}"))?;
    let next_block = fetch_arweave_block_optional(client, arweave_url, next_block_height)
        .await
        .with_context(|| format!("failed fetching Arweave block {next_block_height}"))?;

    Ok(build_arweave_window(
        arweave_url,
        target_block_height,
        next_block_height,
        target_block.timestamp,
        next_block.map(|block| block.timestamp),
    ))
}

pub async fn fetch_settlement_metadata_for_edges(
    client: &Client,
    gql_url: &str,
    edges: &[HistoryEdge],
) -> Result<HashMap<String, SettlementMetadata>> {
    let mut message_ids = Vec::new();
    let mut seen = HashSet::new();

    for edge in edges {
        let Some(message) = edge.node.message.as_ref() else {
            continue;
        };
        if seen.insert(message.id.clone()) {
            message_ids.push(message.id.clone());
        }
    }

    fetch_settlement_metadata(client, gql_url, &message_ids).await
}

pub async fn fetch_settled_notices_by_correlation(
    client: &Client,
    gql_url: &str,
    correlation_ids: &[String],
    from_process_id: &str,
) -> Result<HashMap<String, Vec<SettledNotice>>> {
    let mut grouped = HashMap::<String, Vec<SettledNotice>>::new();
    if correlation_ids.is_empty() {
        return Ok(grouped);
    }

    let url = Url::parse(gql_url).context("invalid GraphQL URL")?;
    let mut seen = HashSet::new();
    let deduped_correlation_ids = correlation_ids
        .iter()
        .filter(|correlation_id| seen.insert((*correlation_id).clone()))
        .cloned()
        .collect::<Vec<_>>();

    for batch in deduped_correlation_ids.chunks(GQL_NOTICE_BATCH_SIZE) {
        let mut after = None::<String>;

        loop {
            let envelope = client
                .post(url.clone())
                .json(&json!({
                    "query": SETTLED_NOTICES_BY_CORRELATION_QUERY,
                    "variables": {
                        "correlationIds": batch,
                        "fromProcessIds": [from_process_id],
                        "owners": [AO_LN_AUTHORITY],
                        "after": after,
                    },
                }))
                .send()
                .await
                .context("failed to contact GraphQL endpoint")?
                .error_for_status()
                .context("GraphQL endpoint returned an error response")?
                .json::<NoticeGraphQlEnvelope>()
                .await
                .context("failed to deserialize GraphQL response")?;

            if let Some(errors) = envelope.errors {
                let message =
                    errors.into_iter().map(|error| error.message).collect::<Vec<_>>().join("; ");
                bail!("GraphQL returned errors: {message}");
            }

            let data = envelope.data.context("GraphQL response did not include data")?;
            let mut last_cursor = None::<String>;
            for edge in data.transactions.edges {
                last_cursor = Some(edge.cursor.clone());
                if !edge.node.owner.address.eq_ignore_ascii_case(AO_LN_AUTHORITY) {
                    continue;
                }
                let Some(correlation_id) = tag_value(&edge.node.tags, "Pushed-For") else {
                    continue;
                };
                let Some(action) = tag_value(&edge.node.tags, "Action") else {
                    continue;
                };
                if !matches_ignore_ascii_case(action, "Credit-Notice")
                    && !matches_ignore_ascii_case(action, "Debit-Notice")
                {
                    continue;
                }

                grouped.entry(correlation_id.to_string()).or_default().push(SettledNotice {
                    message_id: edge.node.id,
                    action: action.to_string(),
                    correlation_id: correlation_id.to_string(),
                    owner_address: edge.node.owner.address,
                    settlement_block_height: edge.node.block.map(|block| block.height),
                    bundled_in_id: edge.node.bundled_in.map(|bundle| bundle.id),
                    recipient: edge.node.recipient,
                    tags: edge.node.tags,
                });
            }

            if !data.transactions.page_info.has_next_page {
                break;
            }
            after = last_cursor;
        }
    }

    Ok(grouped)
}

pub fn build_arweave_window(
    arweave_url: &str,
    target_block_height: u64,
    next_block_height: u64,
    target_block_timestamp_s: i64,
    next_block_timestamp_s: Option<i64>,
) -> ArweaveWindow {
    ArweaveWindow {
        arweave_gateway: arweave_url.to_string(),
        target_block_height,
        next_block_height,
        from_timestamp_ms: target_block_timestamp_s.saturating_mul(1000),
        to_timestamp_ms: next_block_timestamp_s.map(|timestamp_s| timestamp_s.saturating_mul(1000)),
    }
}

async fn fetch_settlement_metadata(
    client: &Client,
    gql_url: &str,
    message_ids: &[String],
) -> Result<HashMap<String, SettlementMetadata>> {
    let mut settlement_metadata = HashMap::new();
    if message_ids.is_empty() {
        return Ok(settlement_metadata);
    }

    let url = Url::parse(gql_url).context("invalid GraphQL URL")?;
    for batch in message_ids.chunks(GQL_BATCH_SIZE) {
        let envelope = client
            .post(url.clone())
            .json(&json!({
                "query": SETTLEMENT_HEIGHTS_QUERY,
                "variables": {
                    "ids": batch,
                },
            }))
            .send()
            .await
            .context("failed to contact GraphQL endpoint")?
            .error_for_status()
            .context("GraphQL endpoint returned an error response")?
            .json::<GraphQlEnvelope>()
            .await
            .context("failed to deserialize GraphQL response")?;

        if let Some(errors) = envelope.errors {
            let message =
                errors.into_iter().map(|error| error.message).collect::<Vec<_>>().join("; ");
            bail!("GraphQL returned errors: {message}");
        }

        let data = envelope.data.context("GraphQL response did not include data")?;
        for edge in data.transactions.edges {
            settlement_metadata.insert(
                edge.node.id,
                SettlementMetadata {
                    settlement_block_height: edge.node.block.map(|block| block.height),
                    bundled_in_id: edge.node.bundled_in.map(|bundle| bundle.id),
                },
            );
        }
    }

    Ok(settlement_metadata)
}

async fn fetch_arweave_block(
    client: &Client,
    arweave_url: &str,
    block_height: u64,
) -> Result<ArweaveBlock> {
    let url = arweave_block_url(arweave_url, block_height)?;
    let response = client
        .get(url)
        .send()
        .await
        .context("failed to contact Arweave")?
        .error_for_status()
        .context("Arweave returned an error response")?;

    response.json::<ArweaveBlock>().await.context("failed to deserialize Arweave block response")
}

async fn fetch_arweave_block_optional(
    client: &Client,
    arweave_url: &str,
    block_height: u64,
) -> Result<Option<ArweaveBlock>> {
    let url = arweave_block_url(arweave_url, block_height)?;
    let response = client.get(url).send().await.context("failed to contact Arweave")?;

    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }

    let response = response.error_for_status().context("Arweave returned an error response")?;

    response
        .json::<ArweaveBlock>()
        .await
        .map(Some)
        .context("failed to deserialize Arweave block response")
}

fn arweave_block_url(arweave_url: &str, block_height: u64) -> Result<Url> {
    Url::parse(&format!("{}/block/height/{}", arweave_url.trim_end_matches('/'), block_height))
        .context("invalid Arweave base URL")
}

fn parse_block_height(block_height: &str) -> Result<u64> {
    normalize_block_height(block_height).parse::<u64>().context("block-height must be an integer")
}

fn tag_value<'a>(tags: &'a [crate::core::types::Tag], name: &str) -> Option<&'a str> {
    tags.iter().find(|tag| tag.name.eq_ignore_ascii_case(name)).map(|tag| tag.value.as_str())
}

fn matches_ignore_ascii_case(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}
