use crate::core::{
    arweave::{
        ArweaveWindow, SettledNotice, SettlementMetadata, fetch_arweave_window,
        fetch_arweave_window_optional, fetch_settled_notices_by_correlation,
        fetch_settled_notices_by_reference, fetch_settlement_metadata_for_edges,
    },
    constants::{AO_LN_AUTHORITY, AO_TOKEN_SYMBOL, NETWORK_VERSION},
    cu,
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
    pub compute_error: Option<String>,
    pub credit_notices: Vec<TokenMessageRecord>,
    pub debit_notices: Vec<TokenMessageRecord>,
    pub pending_credit_notices: Vec<PendingTokenNoticeRecord>,
    pub pending_debit_notices: Vec<PendingTokenNoticeRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TokenTransferWithNoticesResponse {
    pub transfer: HistoryNode,
    pub compute_error: Option<String>,
    pub credit_notices: Vec<TokenMessageRecord>,
    pub debit_notices: Vec<TokenMessageRecord>,
    pub pending_credit_notices: Vec<PendingTokenNoticeRecord>,
    pub pending_debit_notices: Vec<PendingTokenNoticeRecord>,
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

#[derive(Debug, Clone, Serialize)]
pub struct PendingTokenNoticeRecord {
    pub action: String,
    pub reference: Option<String>,
    pub from_process: String,
    pub sender: Option<String>,
    pub recipient: Option<String>,
    pub target: Option<String>,
    pub quantity: Option<String>,
    pub pushed_for: String,
    pub data: Option<String>,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Default)]
struct TransferNoticeMatches {
    compute_error: Option<String>,
    credit: Vec<TokenMessageRecord>,
    debit: Vec<TokenMessageRecord>,
    pending_credit: Vec<PendingTokenNoticeRecord>,
    pending_debit: Vec<PendingTokenNoticeRecord>,
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

    let transfer_edges = filter_transfer_edges(page.edges, &config.ao_token_process_id);
    let transfer_settlement_metadata =
        fetch_settlement_metadata_for_edges(client, &config.gql_url, &transfer_edges)
            .await
            .with_context(|| {
                format!("failed resolving transfer settlement metadata from {}", config.gql_url)
            })?;

    let notices_by_transfer =
        fetch_related_notices(client, config, &page.arweave_window, &transfer_edges)
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

pub async fn fetch_ao_token_transfer_with_notices(
    client: &Client,
    config: &AppConfig,
    message_id: &str,
    notice_scan_blocks: u64,
) -> Result<TokenTransferWithNoticesResponse> {
    let transfer =
        su::fetch_message_node(client, &config.su_url, &config.ao_token_process_id, message_id)
            .await
            .with_context(|| {
                format!(
                    "failed fetching transfer message {message_id} for process {}",
                    config.ao_token_process_id
                )
            })?;

    let transfer_message = transfer
        .message
        .as_ref()
        .context("encountered a transfer response without a message payload")?;
    if !is_valid_transfer_message(
        transfer_message,
        &transfer.assignment,
        &config.ao_token_process_id,
    ) {
        anyhow::bail!("message {message_id} is not a valid AO token Transfer");
    }

    let block_height = transfer
        .assignment
        .block_height()
        .context("transfer is missing assignment Block-Height")?;
    let arweave_window = fetch_arweave_window(client, &config.arweave_url, block_height).await?;
    let end_block_height = normalize_block_height(block_height)
        .parse::<u64>()
        .context("transfer assignment Block-Height is not an integer")?
        .saturating_add(notice_scan_blocks);
    let end_arweave_window =
        fetch_arweave_window_optional(client, &config.arweave_url, &end_block_height.to_string())
            .await?;
    let from_timestamp_ms = assignment_timestamp_value(&transfer.assignment)
        .unwrap_or(arweave_window.from_timestamp_ms);
    let to_timestamp_ms = end_arweave_window.and_then(|window| window.to_timestamp_ms);

    let mut credit_notice_nodes = Vec::new();
    let mut debit_notice_nodes = Vec::new();
    let transfer_correlation_id = notice_correlation_id(transfer_message).to_string();

    for candidate_process_id in
        collect_notice_process_ids(&config.ao_token_process_id, transfer_message)
    {
        let candidate_edges = su::fetch_process_edges_for_window(
            client,
            &config.su_url,
            &candidate_process_id,
            from_timestamp_ms,
            to_timestamp_ms,
            config.page_size,
        )
        .await
        .with_context(|| {
            format!("failed fetching candidate notice history for process {candidate_process_id}")
        })?
        .edges;

        for candidate_edge in candidate_edges {
            let Some(candidate_message) = candidate_edge.node.message.as_ref() else {
                continue;
            };
            if !candidate_message.owner.address.eq_ignore_ascii_case(AO_LN_AUTHORITY) {
                continue;
            }
            if candidate_message
                .tag_value("From-Process")
                .is_none_or(|value| !value.eq_ignore_ascii_case(&config.ao_token_process_id))
            {
                continue;
            }
            if notice_correlation_id(candidate_message) != transfer_correlation_id.as_str() {
                continue;
            }

            let Some(action) = candidate_message.tag_value("Action") else {
                continue;
            };
            if action.eq_ignore_ascii_case("Credit-Notice") {
                credit_notice_nodes.push(candidate_edge.node);
            } else if action.eq_ignore_ascii_case("Debit-Notice") {
                debit_notice_nodes.push(candidate_edge.node);
            }
        }
    }

    let mut credit_notices = dedupe_history_nodes_by_message_id(credit_notice_nodes)
        .into_iter()
        .map(build_token_message_record_from_history_node)
        .collect::<Result<Vec<_>>>()?;
    let mut debit_notices = dedupe_history_nodes_by_message_id(debit_notice_nodes)
        .into_iter()
        .map(build_token_message_record_from_history_node)
        .collect::<Result<Vec<_>>>()?;

    backfill_transfer_notices_from_gql(
        client,
        config,
        &transfer.assignment,
        transfer_message,
        &mut credit_notices,
        &mut debit_notices,
    )
    .await;
    let pending_notices = backfill_transfer_notices_from_cu_result(
        client,
        config,
        &transfer.assignment,
        transfer_message,
        &mut credit_notices,
        &mut debit_notices,
    )
    .await;

    Ok(TokenTransferWithNoticesResponse {
        transfer,
        compute_error: pending_notices.compute_error,
        credit_notices,
        debit_notices,
        pending_credit_notices: pending_notices.pending_credit,
        pending_debit_notices: pending_notices.pending_debit,
    })
}

fn filter_transfer_edges(edges: Vec<HistoryEdge>, process_id: &str) -> Vec<HistoryEdge> {
    edges
        .into_iter()
        .filter(|edge| {
            edge.node.message.as_ref().is_some_and(|message| {
                is_valid_transfer_message(message, &edge.node.assignment, process_id)
            })
        })
        .collect()
}

fn is_transfer_message(message: &AoMessage) -> bool {
    message.tag_value("Action").is_some_and(|value| value.eq_ignore_ascii_case("Transfer"))
}

fn is_valid_transfer_message(
    message: &AoMessage,
    assignment: &Assignment,
    process_id: &str,
) -> bool {
    is_transfer_message(message)
        && message.tag_value("Data-Protocol").is_some_and(|value| value.eq_ignore_ascii_case("ao"))
        && message
            .tag_value("Variant")
            .is_some_and(|value| value.eq_ignore_ascii_case(NETWORK_VERSION))
        && message.tag_value("Type").is_some_and(|value| value.eq_ignore_ascii_case("Message"))
        && message
            .target
            .as_deref()
            .or_else(|| message.tag_value("Target"))
            .is_some_and(|target| target.eq_ignore_ascii_case(process_id))
        && assignment.owner.address.eq_ignore_ascii_case(AO_LN_AUTHORITY)
        && assignment
            .tag_value("Process")
            .is_some_and(|value| value.eq_ignore_ascii_case(process_id))
        && assignment
            .tag_value("Data-Protocol")
            .is_some_and(|value| value.eq_ignore_ascii_case("ao"))
        && assignment
            .tag_value("Variant")
            .is_some_and(|value| value.eq_ignore_ascii_case(NETWORK_VERSION))
        && assignment
            .tag_value("Type")
            .is_some_and(|value| value.eq_ignore_ascii_case("Assignment"))
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

async fn backfill_transfer_notices_from_gql(
    client: &Client,
    config: &AppConfig,
    transfer_assignment: &Assignment,
    transfer_message: &AoMessage,
    credit_notices: &mut Vec<TokenMessageRecord>,
    debit_notices: &mut Vec<TokenMessageRecord>,
) {
    let correlation_id = notice_correlation_id(transfer_message).to_string();
    let settled_notices = match fetch_settled_notices_by_correlation(
        client,
        &config.gql_url,
        std::slice::from_ref(&correlation_id),
        &config.ao_token_process_id,
    )
    .await
    {
        Ok(settled_notices) => settled_notices,
        Err(_) => return,
    };

    let Some(notices) = settled_notices.get(&correlation_id) else {
        return;
    };

    for notice in notices {
        if tag_value_from_tags(&notice.tags, "From-Process")
            .is_none_or(|value| !value.eq_ignore_ascii_case(&config.ao_token_process_id))
        {
            continue;
        }

        if notice.action.eq_ignore_ascii_case("Credit-Notice") && !credit_notices.is_empty() {
            continue;
        }
        if notice.action.eq_ignore_ascii_case("Debit-Notice") && !debit_notices.is_empty() {
            continue;
        }

        if notice.action.eq_ignore_ascii_case("Credit-Notice") {
            credit_notices.push(build_token_message_record_from_assignment_and_settled_notice(
                transfer_assignment,
                notice,
            ));
        } else if notice.action.eq_ignore_ascii_case("Debit-Notice") {
            debit_notices.push(build_token_message_record_from_assignment_and_settled_notice(
                transfer_assignment,
                notice,
            ));
        }
    }

    *credit_notices = dedupe_notice_records_by_message_id(std::mem::take(credit_notices));
    *debit_notices = dedupe_notice_records_by_message_id(std::mem::take(debit_notices));
}

async fn backfill_transfer_notices_from_cu_result(
    client: &Client,
    config: &AppConfig,
    transfer_assignment: &Assignment,
    transfer_message: &AoMessage,
    credit_notices: &mut Vec<TokenMessageRecord>,
    debit_notices: &mut Vec<TokenMessageRecord>,
) -> TransferNoticeMatches {
    let mut pending_matches = TransferNoticeMatches::default();
    if !credit_notices.is_empty() && !debit_notices.is_empty() {
        return pending_matches;
    }

    let cu_result = match cu::fetch_transfer_result(
        client,
        &config.cu_url,
        &config.ao_token_process_id,
        &transfer_message.id,
    )
    .await
    {
        Ok(cu_result) => cu_result,
        Err(_) => return pending_matches,
    };
    pending_matches.compute_error = cu_result.error;
    if pending_matches.compute_error.is_some() {
        return pending_matches;
    }

    let cu_pending_notices = cu_result.pending_notices;

    let requested_references = collect_missing_notice_references(
        &cu_pending_notices,
        credit_notices.is_empty(),
        debit_notices.is_empty(),
    );
    let settled_notices = if requested_references.is_empty() {
        HashMap::new()
    } else {
        match fetch_settled_notices_by_reference(
            client,
            &config.gql_url,
            &requested_references,
            &config.ao_token_process_id,
        )
        .await
        {
            Ok(settled_notices) => settled_notices,
            Err(_) => HashMap::new(),
        }
    };

    fill_missing_transfer_notices_from_references(
        &cu_pending_notices,
        &settled_notices,
        credit_notices,
        debit_notices,
        |notice| {
            build_token_message_record_from_assignment_and_settled_notice(
                transfer_assignment,
                notice,
            )
        },
    );
    fill_pending_transfer_notices_from_cu(
        &cu_pending_notices,
        transfer_message.id.as_str(),
        &config.ao_token_process_id,
        credit_notices.is_empty(),
        debit_notices.is_empty(),
        &mut pending_matches,
    );

    pending_matches
}

async fn fetch_related_notices(
    client: &Client,
    config: &AppConfig,
    arweave_window: &ArweaveWindow,
    transfer_edges: &[HistoryEdge],
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
        &config.ao_token_process_id,
        transfer_edges,
        &mut notices_by_transfer,
    )
    .await?;
    augment_missing_notices_from_cu_result(
        client,
        config,
        transfer_edges,
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
                if !candidate_message.owner.address.eq_ignore_ascii_case(AO_LN_AUTHORITY) {
                    continue;
                }
                if candidate_message
                    .tag_value("From-Process")
                    .is_none_or(|value| !value.eq_ignore_ascii_case(&config.ao_token_process_id))
                {
                    continue;
                }
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
    ao_token_process_id: &str,
    transfer_edges: &[HistoryEdge],
    notices_by_transfer: &mut HashMap<String, TransferNoticeMatches>,
) -> Result<()> {
    let missing_transfers = transfer_edges
        .iter()
        .filter_map(|transfer_edge| {
            let message = transfer_edge.node.message.as_ref()?;
            let current = notices_by_transfer.get(&message.id).cloned().unwrap_or_default();
            if !current.credit.is_empty() && !current.debit.is_empty() {
                return None;
            }
            Some(transfer_edge.clone())
        })
        .collect::<Vec<_>>();

    if missing_transfers.is_empty() {
        return Ok(());
    }

    let correlation_ids = missing_transfers
        .iter()
        .filter_map(|transfer_edge| {
            transfer_edge.node.message.as_ref().map(notice_correlation_id).map(str::to_string)
        })
        .collect::<Vec<_>>();

    let settled_notices = match fetch_settled_notices_by_correlation(
        client,
        gql_url,
        &correlation_ids,
        ao_token_process_id,
    )
    .await
    {
        Ok(settled_notices) => settled_notices,
        Err(_) => return Ok(()),
    };

    for transfer_edge in missing_transfers {
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

    Ok(())
}

async fn augment_missing_notices_from_cu_result(
    client: &Client,
    config: &AppConfig,
    transfer_edges: &[HistoryEdge],
    notices_by_transfer: &mut HashMap<String, TransferNoticeMatches>,
) -> Result<()> {
    let missing_transfers = transfer_edges
        .iter()
        .filter_map(|transfer_edge| {
            let message = transfer_edge.node.message.as_ref()?;
            let current = notices_by_transfer.get(&message.id).cloned().unwrap_or_default();
            if !current.credit.is_empty() && !current.debit.is_empty() {
                return None;
            }
            Some(transfer_edge)
        })
        .collect::<Vec<_>>();

    for transfer_edge in missing_transfers {
        let Some(transfer_message) = transfer_edge.node.message.as_ref() else {
            continue;
        };
        let current = notices_by_transfer.get(&transfer_message.id).cloned().unwrap_or_default();
        let cu_result = match cu::fetch_transfer_result(
            client,
            &config.cu_url,
            &config.ao_token_process_id,
            &transfer_message.id,
        )
        .await
        {
            Ok(cu_result) => cu_result,
            Err(_) => continue,
        };
        let entry = notices_by_transfer.entry(transfer_message.id.clone()).or_default();
        entry.compute_error = cu_result.error;
        if entry.compute_error.is_some() {
            continue;
        }
        let cu_pending_notices = cu_result.pending_notices;

        let requested_references = collect_missing_notice_references(
            &cu_pending_notices,
            current.credit.is_empty(),
            current.debit.is_empty(),
        );
        let settled_notices = if requested_references.is_empty() {
            HashMap::new()
        } else {
            match fetch_settled_notices_by_reference(
                client,
                &config.gql_url,
                &requested_references,
                &config.ao_token_process_id,
            )
            .await
            {
                Ok(settled_notices) => settled_notices,
                Err(_) => HashMap::new(),
            }
        };

        fill_missing_transfer_notices_from_references(
            &cu_pending_notices,
            &settled_notices,
            &mut entry.credit,
            &mut entry.debit,
            |notice| build_token_message_record_from_settled_notice(transfer_edge, notice),
        );
        fill_pending_transfer_notices_from_cu(
            &cu_pending_notices,
            transfer_message.id.as_str(),
            &config.ao_token_process_id,
            entry.credit.is_empty(),
            entry.debit.is_empty(),
            entry,
        );
    }

    Ok(())
}

fn collect_missing_notice_references(
    cu_pending_notices: &cu::CuPendingNotices,
    missing_credit: bool,
    missing_debit: bool,
) -> Vec<String> {
    let mut references = Vec::new();
    let mut seen = HashSet::new();

    if missing_credit {
        for notice in &cu_pending_notices.credit {
            let Some(reference) = notice.reference.as_ref() else {
                continue;
            };
            if seen.insert(reference.clone()) {
                references.push(reference.clone());
            }
        }
    }
    if missing_debit {
        for notice in &cu_pending_notices.debit {
            let Some(reference) = notice.reference.as_ref() else {
                continue;
            };
            if seen.insert(reference.clone()) {
                references.push(reference.clone());
            }
        }
    }

    references
}

fn fill_missing_transfer_notices_from_references<F>(
    cu_pending_notices: &cu::CuPendingNotices,
    settled_notices_by_reference: &HashMap<String, Vec<SettledNotice>>,
    credit_notices: &mut Vec<TokenMessageRecord>,
    debit_notices: &mut Vec<TokenMessageRecord>,
    mut build_record: F,
) where
    F: FnMut(&SettledNotice) -> TokenMessageRecord,
{
    if credit_notices.is_empty() {
        for notice_hint in &cu_pending_notices.credit {
            let Some(reference) = notice_hint.reference.as_ref() else {
                continue;
            };
            let Some(notices) = settled_notices_by_reference.get(reference) else {
                continue;
            };
            for notice in notices {
                if notice.action.eq_ignore_ascii_case("Credit-Notice") {
                    credit_notices.push(build_record(notice));
                }
            }
        }
    }

    if debit_notices.is_empty() {
        for notice_hint in &cu_pending_notices.debit {
            let Some(reference) = notice_hint.reference.as_ref() else {
                continue;
            };
            let Some(notices) = settled_notices_by_reference.get(reference) else {
                continue;
            };
            for notice in notices {
                if notice.action.eq_ignore_ascii_case("Debit-Notice") {
                    debit_notices.push(build_record(notice));
                }
            }
        }
    }

    *credit_notices = dedupe_notice_records_by_message_id(std::mem::take(credit_notices));
    *debit_notices = dedupe_notice_records_by_message_id(std::mem::take(debit_notices));
}

fn fill_pending_transfer_notices_from_cu(
    cu_pending_notices: &cu::CuPendingNotices,
    transfer_id: &str,
    ao_token_process_id: &str,
    missing_credit: bool,
    missing_debit: bool,
    matches: &mut TransferNoticeMatches,
) {
    if missing_credit {
        matches.pending_credit = cu_pending_notices
            .credit
            .iter()
            .map(|notice| {
                build_pending_token_notice_record(notice, transfer_id, ao_token_process_id)
            })
            .collect();
    }
    if missing_debit {
        matches.pending_debit = cu_pending_notices
            .debit
            .iter()
            .map(|notice| {
                build_pending_token_notice_record(notice, transfer_id, ao_token_process_id)
            })
            .collect();
    }
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

fn dedupe_history_nodes_by_message_id(nodes: Vec<HistoryNode>) -> Vec<HistoryNode> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();

    for node in nodes {
        let Some(message_id) = node.message.as_ref().map(|message| message.id.as_str()) else {
            continue;
        };
        if seen.insert(message_id.to_string()) {
            deduped.push(node);
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
        compute_error: notice_matches.compute_error,
        credit_notices: notice_matches.credit,
        debit_notices: notice_matches.debit,
        pending_credit_notices: notice_matches.pending_credit,
        pending_debit_notices: notice_matches.pending_debit,
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

fn build_token_message_record_from_assignment_and_settled_notice(
    transfer_assignment: &Assignment,
    notice: &SettledNotice,
) -> TokenMessageRecord {
    let assignment_block_height = assignment_block_height_value(transfer_assignment);
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

fn build_token_message_record_from_history_node(node: HistoryNode) -> Result<TokenMessageRecord> {
    let HistoryNode { message, assignment } = node;
    let message = message.context("encountered a response node without a message payload")?;
    let action = message.tag_value("Action").unwrap_or("Message").to_string();
    let assignment_id = assignment.id.clone();
    let from_process = message.tag_value("From-Process").map(str::to_string);
    let sender = message.tag_value("Sender").map(str::to_string);
    let recipient = message.tag_value("Recipient").map(str::to_string);
    let target = message.target.clone().or_else(|| message.tag_value("Target").map(str::to_string));
    let quantity = message.tag_value("Quantity").map(str::to_string);
    let reference = message.tag_value("Reference").map(str::to_string);
    let pushed_for = message.tag_value("Pushed-For").map(str::to_string);

    Ok(TokenMessageRecord {
        action,
        message_id: message.id,
        assignment_id: Some(assignment_id),
        assignment_block_height: assignment_block_height_value(&assignment),
        settlement_block_height: None,
        assignment_timestamp_ms: assignment_timestamp_value(&assignment),
        bundled_in_id: None,
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

fn build_pending_token_notice_record(
    notice: &cu::CuPendingNotice,
    transfer_id: &str,
    ao_token_process_id: &str,
) -> PendingTokenNoticeRecord {
    PendingTokenNoticeRecord {
        action: notice.action.clone(),
        reference: notice.reference.clone(),
        from_process: ao_token_process_id.to_string(),
        sender: notice.sender.clone(),
        recipient: notice.recipient.clone(),
        target: notice.target.clone(),
        quantity: notice.quantity.clone(),
        pushed_for: transfer_id.to_string(),
        data: notice.data.clone(),
        tags: notice.tags.clone(),
    }
}

fn tag_value_from_tags<'a>(tags: &'a [Tag], name: &str) -> Option<&'a str> {
    tags.iter().find(|tag| tag.name.eq_ignore_ascii_case(name)).map(|tag| tag.value.as_str())
}
