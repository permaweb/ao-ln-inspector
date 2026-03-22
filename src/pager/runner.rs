use crate::{
    core::{
        arweave::fetch_arweave_tip_height,
        server::{AppConfig, app_state_from_env},
        TokenTransferRecord, TokenTransfersResponse, fetch_ao_token_transfers,
    },
    pager::{bot::send_block_result, state},
};
use anyhow::Result;
use reqwest::Client;
use std::time::Duration;

const PAGER_TIP_LAG_BLOCKS: u64 = 20;
const PAGER_INTERVAL_SECS: u64 = 180;
const MISSING_EXAMPLES_LIMIT: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunnerOutcome {
    Scanned { block: u64, live_tip: u64 },
    Waiting { next_block: u64, live_tip: u64 },
}

pub async fn run_forever() -> Result<()> {
    let state = app_state_from_env()?;

    loop {
        match run_once(&state.client, &state.config).await {
            Ok(RunnerOutcome::Scanned { block, live_tip }) => {
                println!("pager scanned block {block} (live tip {live_tip})");
            }
            Ok(RunnerOutcome::Waiting { next_block, live_tip }) => {
                println!(
                    "pager waiting: next block {next_block} is within {PAGER_TIP_LAG_BLOCKS} blocks of live tip {live_tip}"
                );
            }
            Err(error) => {
                eprintln!("pager iteration failed: {error}");
            }
        }

        tokio::time::sleep(Duration::from_secs(PAGER_INTERVAL_SECS)).await;
    }
}

pub async fn run_once(client: &Client, config: &AppConfig) -> Result<RunnerOutcome> {
    let next_block = state::load_next_block()?;
    let live_tip = fetch_arweave_tip_height(client, &config.arweave_url).await?;

    if next_block.saturating_add(PAGER_TIP_LAG_BLOCKS) > live_tip {
        return Ok(RunnerOutcome::Waiting { next_block, live_tip });
    }

    let response = fetch_ao_token_transfers(client, config, &next_block.to_string()).await?;
    let summary = format_block_summary(&response, live_tip);
    send_block_result(summary).await?;
    state::save_next_block(next_block.saturating_add(1))?;

    Ok(RunnerOutcome::Scanned { block: next_block, live_tip })
}

fn format_block_summary(response: &TokenTransfersResponse, live_tip: u64) -> String {
    let mut complete = 0usize;
    let mut missing_credit = Vec::new();
    let mut missing_debit = Vec::new();
    let mut missing_both = Vec::new();

    for transfer in &response.transfers {
        let has_credit = !transfer.credit_notices.is_empty();
        let has_debit = !transfer.debit_notices.is_empty();

        match (has_credit, has_debit) {
            (true, true) => complete += 1,
            (false, true) => missing_credit.push(transfer),
            (true, false) => missing_debit.push(transfer),
            (false, false) => missing_both.push(transfer),
        }
    }

    let mut lines = vec![
        format!("ao-ln pager"),
        format!("assignment block: {}", response.assignment_block_height_query),
        format!("live tip: {live_tip}"),
        format!("transfers: {}", response.transfer_count),
        format!("complete: {complete}"),
        format!("missing credit: {}", missing_credit.len()),
        format!("missing debit: {}", missing_debit.len()),
        format!("missing both: {}", missing_both.len()),
    ];

    append_missing_examples(&mut lines, "missing credit", &missing_credit);
    append_missing_examples(&mut lines, "missing debit", &missing_debit);
    append_missing_examples(&mut lines, "missing both", &missing_both);

    lines.join("\n")
}

fn append_missing_examples(
    lines: &mut Vec<String>,
    label: &str,
    transfers: &[&TokenTransferRecord],
) {
    if transfers.is_empty() {
        return;
    }

    lines.push(format!("{label} examples:"));
    for transfer in transfers.iter().take(MISSING_EXAMPLES_LIMIT) {
        lines.push(format!(
            "{} {}",
            transfer.correlation_id, transfer.transfer.message_id
        ));
    }
}
