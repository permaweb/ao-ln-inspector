use crate::core::{
    arweave::{
        ArweaveWindow, SettledNotice, SettlementMetadata,
        fetch_settled_notices_by_block_and_correlation, fetch_settlement_metadata_for_edges,
    },
    constants::AO_TOKEN_SYMBOL,
    server::AppConfig,
    su,
    types::{AoMessage, Assignment, HistoryEdge, HistoryNode, Tag, normalize_block_height},
};
use anyhow::{Context, Result};
use reqwest::Client;
use serde::Serialize;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Serialize)]
pub struct TokenTransfersResponse {
    pub token: &'static str,
    pub process_id: String,
    pub assignment_block_height_query: String,
    pub su_url: String,
    pub gql_url: String,
    pub page_size: usize,
    pub page_count: usize,
    pub transfer_count: usize,
    pub arweave_window: ArweaveWindow,
    pub transfers: Vec<TokenTransferRecord>,
}

#[derive(Debug, Serialize)]
pub struct TokenTransferRecord {
    pub correlation_id: String,
    pub transfer: TokenMessageRecord,
    pub credit_notices: Vec<TokenMessageRecord>,
    pub debit_notices: Vec<TokenMessageRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TokenMessageRecord {
    pub action: String,
    pub message_id: String,
    pub assignment_id: Option<String>,
    pub assignment_block_height: Option<u64>,
    pub settlement_block_height: Option<u64>,
    pub assignment_timestamp_ms: Option<i64>,
    pub bundled_in_id: Option<String>,
    pub from_process: Option<String>,
    pub sender: Option<String>,
    pub recipient: Option<String>,
    pub target: Option<String>,
    pub quantity: Option<String>,
    pub reference: Option<String>,
    pub pushed_for: Option<String>,
    pub data: Option<String>,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Default)]
struct TransferNoticeMatches {
    credit: Vec<TokenMessageRecord>,
    debit: Vec<TokenMessageRecord>,
}

#[derive(Debug, Clone, Default)]
struct TransferNoticeEdgeMatches {
    credit: Vec<HistoryEdge>,
    debit: Vec<HistoryEdge>,
}

pub async fn fetch_ao_token_transfers(
    client: &Client,
    config: &AppConfig,
    block_id: &str,
) -> Result<TokenTransfersResponse> {
    let page = su::fetch_process_edges_for_assignment_block(
        client,
        &config.su_url,
        &config.arweave_url,
        &config.ao_token_process_id,
        block_id,
        config.page_size,
    )
    .await
    .with_context(|| {
        format!(
            "failed fetching AO token transfers for process {} at assignment block {}",
            config.ao_token_process_id, block_id
        )
    })?;

    let transfer_edges = filter_transfer_edges(page.edges);
    let transfer_settlement_metadata =
        fetch_settlement_metadata_for_edges(client, &config.gql_url, &transfer_edges)
            .await
            .with_context(|| {
                format!("failed resolving transfer settlement metadata from {}", config.gql_url)
            })?;

    let notices_by_transfer = fetch_related_notices(
        client,
        config,
        &page.arweave_window,
        &transfer_edges,
        &transfer_settlement_metadata,
    )
    .await
    .context("failed resolving transfer notices")?;

    let transfer_count = transfer_edges.len();
    let transfers = transfer_edges
        .into_iter()
        .map(|edge| {
            build_token_transfer_record(edge, &transfer_settlement_metadata, &notices_by_transfer)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(TokenTransfersResponse {
        token: AO_TOKEN_SYMBOL,
        process_id: config.ao_token_process_id.clone(),
        assignment_block_height_query: normalize_block_height(block_id),
        // normalized_assignment_block_height: normalize_block_height(block_id),
        su_url: config.su_url.clone(),
        gql_url: config.gql_url.clone(),
        page_size: config.page_size,
        page_count: page.page_count,
        transfer_count,
        arweave_window: page.arweave_window,
        transfers,
    })
}

fn filter_transfer_edges(edges: Vec<HistoryEdge>) -> Vec<HistoryEdge> {
    edges
        .into_iter()
        .filter(|edge| edge.node.message.as_ref().is_some_and(is_transfer_message))
        .collect()
}

fn is_transfer_message(message: &AoMessage) -> bool {
    message.tag_value("Action").is_some_and(|value| value.eq_ignore_ascii_case("Transfer"))
}

fn notice_correlation_id<'a>(message: &'a AoMessage) -> &'a str {
    message.tag_value("Pushed-For").unwrap_or(message.id.as_str())
}

fn assignment_block_height_value(assignment: &Assignment) -> Option<u64> {
    assignment.block_height().and_then(|height| normalize_block_height(height).parse::<u64>().ok())
}

fn assignment_timestamp_value(assignment: &Assignment) -> Option<i64> {
    assignment.tag_value("Timestamp").and_then(|timestamp| timestamp.parse::<i64>().ok())
}

fn collect_notice_process_ids(query_process_id: &str, message: &AoMessage) -> Vec<String> {
    let mut process_ids = Vec::new();
    let mut seen = HashSet::new();

    for candidate in
        [Some(query_process_id), message.tag_value("From-Process"), message.tag_value("Recipient")]
    {
        let Some(candidate) = candidate else {
            continue;
        };
        let candidate = candidate.trim();
        if candidate.is_empty() {
            continue;
        }
        if seen.insert(candidate.to_string()) {
            process_ids.push(candidate.to_string());
        }
    }

    process_ids
}

async fn fetch_related_notices(
    client: &Client,
    config: &AppConfig,
    arweave_window: &ArweaveWindow,
    transfer_edges: &[HistoryEdge],
    transfer_settlement_metadata: &HashMap<String, SettlementMetadata>,
) -> Result<HashMap<String, TransferNoticeMatches>> {
    let su_notice_edges =
        fetch_related_notice_edges_from_su(client, config, arweave_window, transfer_edges).await?;
    let all_notice_edges = dedupe_edges_by_message_id(
        su_notice_edges
            .values()
            .flat_map(|matches| matches.credit.iter().chain(matches.debit.iter()))
            .cloned()
            .collect(),
    );
    let notice_settlement_metadata =
        fetch_settlement_metadata_for_edges(client, &config.gql_url, &all_notice_edges)
            .await
            .with_context(|| {
                format!("failed resolving notice settlement metadata from {}", config.gql_url)
            })?;

    let mut notices_by_transfer =
        convert_su_notice_edges_to_records(su_notice_edges, &notice_settlement_metadata)?;
    augment_missing_notices_from_gql(
        client,
        &config.gql_url,
        transfer_edges,
        transfer_settlement_metadata,
        &mut notices_by_transfer,
    )
    .await?;

    Ok(notices_by_transfer)
}

async fn fetch_related_notice_edges_from_su(
    client: &Client,
    config: &AppConfig,
    arweave_window: &ArweaveWindow,
    transfer_edges: &[HistoryEdge],
) -> Result<HashMap<String, TransferNoticeEdgeMatches>> {
    let mut process_cache = HashMap::<String, Vec<HistoryEdge>>::new();
    let mut notices_by_transfer = HashMap::<String, TransferNoticeEdgeMatches>::new();

    for transfer_edge in transfer_edges {
        let Some(message) = transfer_edge.node.message.as_ref() else {
            continue;
        };
        let transfer_correlation_id = notice_correlation_id(message).to_string();
        let mut matches = TransferNoticeEdgeMatches::default();

        for candidate_process_id in collect_notice_process_ids(&config.ao_token_process_id, message)
        {
            let candidate_edges = if let Some(cached) = process_cache.get(&candidate_process_id) {
                cached.clone()
            } else {
                let fetched = su::fetch_process_edges_for_window(
                    client,
                    &config.su_url,
                    &candidate_process_id,
                    arweave_window.from_timestamp_ms,
                    arweave_window.to_timestamp_ms,
                    config.page_size,
                )
                .await
                .with_context(|| {
                    format!(
                        "failed fetching candidate notice history for process {candidate_process_id}"
                    )
                })?
                .edges;
                process_cache.insert(candidate_process_id.clone(), fetched.clone());
                fetched
            };

            for candidate_edge in candidate_edges {
                let Some(candidate_message) = candidate_edge.node.message.as_ref() else {
                    continue;
                };
                if notice_correlation_id(candidate_message) != transfer_correlation_id.as_str() {
                    continue;
                }

                let Some(action) = candidate_message.tag_value("Action") else {
                    continue;
                };
                if action.eq_ignore_ascii_case("Credit-Notice") {
                    matches.credit.push(candidate_edge);
                } else if action.eq_ignore_ascii_case("Debit-Notice") {
                    matches.debit.push(candidate_edge);
                }
            }
        }

        matches.credit = dedupe_edges_by_message_id(matches.credit);
        matches.debit = dedupe_edges_by_message_id(matches.debit);
        notices_by_transfer.insert(message.id.clone(), matches);
    }

    Ok(notices_by_transfer)
}

fn convert_su_notice_edges_to_records(
    notice_edges_by_transfer: HashMap<String, TransferNoticeEdgeMatches>,
    notice_settlement_metadata: &HashMap<String, SettlementMetadata>,
) -> Result<HashMap<String, TransferNoticeMatches>> {
    let mut notices_by_transfer = HashMap::new();

    for (transfer_id, edge_matches) in notice_edges_by_transfer {
        let mut notice_matches = TransferNoticeMatches::default();

        for edge in edge_matches.credit {
            let settlement = settlement_for_edge(&edge, notice_settlement_metadata);
            notice_matches.credit.push(build_token_message_record(edge, settlement)?);
        }
        for edge in edge_matches.debit {
            let settlement = settlement_for_edge(&edge, notice_settlement_metadata);
            notice_matches.debit.push(build_token_message_record(edge, settlement)?);
        }

        notice_matches.credit = dedupe_notice_records_by_message_id(notice_matches.credit);
        notice_matches.debit = dedupe_notice_records_by_message_id(notice_matches.debit);
        notices_by_transfer.insert(transfer_id, notice_matches);
    }

    Ok(notices_by_transfer)
}

async fn augment_missing_notices_from_gql(
    client: &Client,
    gql_url: &str,
    transfer_edges: &[HistoryEdge],
    transfer_settlement_metadata: &HashMap<String, SettlementMetadata>,
    notices_by_transfer: &mut HashMap<String, TransferNoticeMatches>,
) -> Result<()> {
    let mut transfers_by_settlement_block = HashMap::<u64, Vec<HistoryEdge>>::new();

    for transfer_edge in transfer_edges {
        let Some(message) = transfer_edge.node.message.as_ref() else {
            continue;
        };
        let current = notices_by_transfer.get(&message.id).cloned().unwrap_or_default();
        if !current.credit.is_empty() && !current.debit.is_empty() {
            continue;
        }

        let Some(settlement_block_height) = transfer_settlement_metadata
            .get(&message.id)
            .and_then(|settlement| settlement.settlement_block_height)
        else {
            continue;
        };

        transfers_by_settlement_block
            .entry(settlement_block_height)
            .or_default()
            .push(transfer_edge.clone());
    }

    for (settlement_block_height, grouped_transfers) in transfers_by_settlement_block {
        let correlation_ids = grouped_transfers
            .iter()
            .filter_map(|transfer_edge| {
                transfer_edge.node.message.as_ref().map(notice_correlation_id).map(str::to_string)
            })
            .collect::<Vec<_>>();

        let settled_notices = fetch_settled_notices_by_block_and_correlation(
            client,
            gql_url,
            settlement_block_height,
            &correlation_ids,
        )
        .await
        .with_context(|| {
            format!("failed fetching settled notices from GraphQL block {settlement_block_height}")
        })?;

        for transfer_edge in grouped_transfers {
            let Some(transfer_message) = transfer_edge.node.message.as_ref() else {
                continue;
            };
            let transfer_message_id = transfer_message.id.clone();
            let correlation_id = notice_correlation_id(transfer_message);
            let Some(notices) = settled_notices.get(correlation_id) else {
                continue;
            };

            let entry = notices_by_transfer.entry(transfer_message_id).or_default();
            for notice in notices {
                match notice.action.as_str() {
                    action if action.eq_ignore_ascii_case("Credit-Notice") => {
                        entry.credit.push(build_token_message_record_from_settled_notice(
                            &transfer_edge,
                            notice,
                        ));
                    }
                    action if action.eq_ignore_ascii_case("Debit-Notice") => {
                        entry.debit.push(build_token_message_record_from_settled_notice(
                            &transfer_edge,
                            notice,
                        ));
                    }
                    _ => {}
                }
            }
            entry.credit = dedupe_notice_records_by_message_id(std::mem::take(&mut entry.credit));
            entry.debit = dedupe_notice_records_by_message_id(std::mem::take(&mut entry.debit));
        }
    }

    Ok(())
}

fn dedupe_edges_by_message_id(edges: Vec<HistoryEdge>) -> Vec<HistoryEdge> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();

    for edge in edges {
        let Some(message_id) = edge.node.message.as_ref().map(|message| message.id.as_str()) else {
            continue;
        };
        if seen.insert(message_id.to_string()) {
            deduped.push(edge);
        }
    }

    deduped
}

fn dedupe_notice_records_by_message_id(
    records: Vec<TokenMessageRecord>,
) -> Vec<TokenMessageRecord> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();

    for record in records {
        if seen.insert(record.message_id.clone()) {
            deduped.push(record);
        }
    }

    deduped
}

fn build_token_transfer_record(
    edge: HistoryEdge,
    transfer_settlement_metadata: &HashMap<String, SettlementMetadata>,
    notices_by_transfer: &HashMap<String, TransferNoticeMatches>,
) -> Result<TokenTransferRecord> {
    let message_id = edge
        .node
        .message
        .as_ref()
        .map(|message| message.id.clone())
        .context("encountered a transfer edge without a message payload")?;
    let correlation_id =
        edge.node.message.as_ref().map(notice_correlation_id).unwrap_or_default().to_string();
    let transfer_settlement = settlement_for_edge(&edge, transfer_settlement_metadata);
    let notice_matches = notices_by_transfer.get(&message_id).cloned().unwrap_or_default();

    Ok(TokenTransferRecord {
        correlation_id,
        transfer: build_token_message_record(edge, transfer_settlement)?,
        credit_notices: notice_matches.credit,
        debit_notices: notice_matches.debit,
    })
}

fn settlement_for_edge(
    edge: &HistoryEdge,
    settlement_metadata: &HashMap<String, SettlementMetadata>,
) -> SettlementMetadata {
    edge.node
        .message
        .as_ref()
        .and_then(|message| settlement_metadata.get(&message.id).cloned())
        .unwrap_or_default()
}

fn build_token_message_record(
    edge: HistoryEdge,
    settlement: SettlementMetadata,
) -> Result<TokenMessageRecord> {
    let HistoryEdge { node, .. } = edge;
    let HistoryNode { message, assignment } = node;
    let message = message.context("encountered a response edge without a message payload")?;
    let action = message.tag_value("Action").unwrap_or("Message").to_string();
    let from_process = message.tag_value("From-Process").map(str::to_string);
    let sender = message.tag_value("Sender").map(str::to_string);
    let recipient = message.tag_value("Recipient").map(str::to_string);
    let target = message.target.clone().or_else(|| message.tag_value("Target").map(str::to_string));
    let quantity = message.tag_value("Quantity").map(str::to_string);
    let reference = message.tag_value("Reference").map(str::to_string);
    let pushed_for = message.tag_value("Pushed-For").map(str::to_string);
    let assignment_block_height = assignment_block_height_value(&assignment);
    let assignment_timestamp_ms = assignment_timestamp_value(&assignment);

    Ok(TokenMessageRecord {
        action,
        message_id: message.id,
        assignment_id: Some(assignment.id),
        assignment_block_height,
        settlement_block_height: settlement.settlement_block_height,
        assignment_timestamp_ms,
        bundled_in_id: settlement.bundled_in_id,
        from_process,
        sender,
        recipient,
        target,
        quantity,
        reference,
        pushed_for,
        data: message.data,
        tags: message.tags,
    })
}

fn build_token_message_record_from_settled_notice(
    transfer_edge: &HistoryEdge,
    notice: &SettledNotice,
) -> TokenMessageRecord {
    let assignment_block_height = assignment_block_height_value(&transfer_edge.node.assignment);
    let from_process = tag_value_from_tags(&notice.tags, "From-Process").map(str::to_string);
    let sender = tag_value_from_tags(&notice.tags, "Sender").map(str::to_string);
    let recipient = tag_value_from_tags(&notice.tags, "Recipient")
        .map(str::to_string)
        .or_else(|| notice.recipient.clone());
    let quantity = tag_value_from_tags(&notice.tags, "Quantity").map(str::to_string);
    let reference = tag_value_from_tags(&notice.tags, "Reference").map(str::to_string);
    let pushed_for = tag_value_from_tags(&notice.tags, "Pushed-For").map(str::to_string);

    TokenMessageRecord {
        action: notice.action.clone(),
        message_id: notice.message_id.clone(),
        assignment_id: None,
        assignment_block_height,
        settlement_block_height: notice.settlement_block_height,
        assignment_timestamp_ms: None,
        bundled_in_id: notice.bundled_in_id.clone(),
        from_process,
        sender,
        recipient,
        target: notice.recipient.clone(),
        quantity,
        reference,
        pushed_for,
        data: None,
        tags: notice.tags.clone(),
    }
}

fn tag_value_from_tags<'a>(tags: &'a [Tag], name: &str) -> Option<&'a str> {
    tags.iter().find(|tag| tag.name.eq_ignore_ascii_case(name)).map(|tag| tag.value.as_str())
}
