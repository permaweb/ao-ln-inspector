use crate::pager::START_BLOCK;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

pub const PAGER_STATE_DIR: &str = "pager";
pub const PAGER_STATE_PATH: &str = "pager/state.json";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PagerState {
    pub next_block: Option<u64>,
    pub latest_scanned_block: Option<u64>,
}

pub fn load_next_block() -> Result<u64> {
    let path = Path::new(PAGER_STATE_PATH);
    if !path.exists() {
        return Ok(START_BLOCK);
    }

    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed reading pager state from {PAGER_STATE_PATH}"))?;
    if raw.trim().is_empty() {
        return Ok(START_BLOCK);
    }

    let state: PagerState = serde_json::from_str(&raw)
        .with_context(|| format!("failed parsing pager state from {PAGER_STATE_PATH}"))?;

    if let Some(next_block) = state.next_block {
        return Ok(next_block);
    }

    if let Some(latest_scanned_block) = state.latest_scanned_block {
        return Ok(latest_scanned_block.saturating_add(1));
    }

    Ok(START_BLOCK)
}

pub fn save_next_block(next_block: u64) -> Result<()> {
    fs::create_dir_all(PAGER_STATE_DIR)
        .with_context(|| format!("failed creating pager state dir {PAGER_STATE_DIR}"))?;

    let state = PagerState {
        next_block: Some(next_block),
        latest_scanned_block: Some(next_block.saturating_sub(1)),
    };
    let json = serde_json::to_string_pretty(&state).context("failed serializing pager state")?;

    fs::write(PAGER_STATE_PATH, format!("{json}\n"))
        .with_context(|| format!("failed writing pager state to {PAGER_STATE_PATH}"))?;

    Ok(())
}
