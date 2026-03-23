use crate::core::{constants::NETWORK_VERSION, types::Tag};
use anyhow::{Context, Result};
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashSet;

#[derive(Debug, Clone, Default)]
pub struct CuPendingNotices {
    pub credit: Vec<CuPendingNotice>,
    pub debit: Vec<CuPendingNotice>,
}

#[derive(Debug, Clone, Default)]
pub struct CuTransferResult {
    pub error: Option<String>,
    pub pending_notices: CuPendingNotices,
}

#[derive(Debug, Clone)]
pub struct CuPendingNotice {
    pub action: String,
    pub reference: Option<String>,
    pub sender: Option<String>,
    pub recipient: Option<String>,
    pub target: Option<String>,
    pub quantity: Option<String>,
    pub data: Option<String>,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Deserialize)]
struct CuResultResponse {
    #[serde(rename = "Error")]
    error: Option<String>,
    #[serde(rename = "Messages", default)]
    messages: Vec<CuResultMessage>,
}

#[derive(Debug, Deserialize)]
struct CuResultMessage {
    #[serde(rename = "Data")]
    data: Option<String>,
    #[serde(rename = "Target")]
    target: Option<String>,
    #[serde(rename = "Tags", default)]
    tags: Vec<CuResultTag>,
}

#[derive(Debug, Deserialize)]
struct CuResultTag {
    name: String,
    value: Value,
}

pub async fn fetch_transfer_result(
    client: &Client,
    cu_url: &str,
    process_id: &str,
    transfer_id: &str,
) -> Result<CuTransferResult> {
    let url = Url::parse(&format!(
        "{}/result/{}?process-id={}",
        cu_url.trim_end_matches('/'),
        transfer_id,
        process_id
    ))
    .context("invalid CU URL")?;

    let response = client
        .get(url)
        .send()
        .await
        .context("failed to contact CU endpoint")?
        .error_for_status()
        .context("CU endpoint returned an error response")?
        .json::<CuResultResponse>()
        .await
        .context("failed to deserialize CU result response")?;

    let mut notices = CuPendingNotices::default();
    let mut seen_credit = HashSet::new();
    let mut seen_debit = HashSet::new();

    for message in response.messages {
        if !message.tag_value("Data-Protocol").is_some_and(|value| value.eq_ignore_ascii_case("ao"))
        {
            continue;
        }
        if !message
            .tag_value("Variant")
            .is_some_and(|value| value.eq_ignore_ascii_case(NETWORK_VERSION))
        {
            continue;
        }
        if !message.tag_value("Type").is_some_and(|value| value.eq_ignore_ascii_case("Message")) {
            continue;
        }

        let Some(action) = message.tag_value("Action") else {
            continue;
        };
        let notice = CuPendingNotice {
            action: action.to_string(),
            reference: message.tag_value("Reference").map(str::to_string),
            sender: message.tag_value("Sender").map(str::to_string),
            recipient: message.tag_value("Recipient").map(str::to_string),
            target: message.target.clone().filter(|value| !value.trim().is_empty()),
            quantity: message.tag_value("Quantity").map(str::to_string),
            data: message.data.clone().filter(|value| !value.trim().is_empty()),
            tags: message.string_tags(),
        };

        if action.eq_ignore_ascii_case("Credit-Notice")
            && seen_credit.insert(notice.reference.clone().unwrap_or_default())
        {
            notices.credit.push(notice);
        } else if action.eq_ignore_ascii_case("Debit-Notice")
            && seen_debit.insert(notice.reference.clone().unwrap_or_default())
        {
            notices.debit.push(notice);
        }
    }

    Ok(CuTransferResult {
        error: response.error.and_then(|error| {
            let trimmed = error.trim();
            if trimmed.is_empty() { None } else { Some(error) }
        }),
        pending_notices: notices,
    })
}

impl CuResultMessage {
    fn tag_value(&self, name: &str) -> Option<&str> {
        self.tags
            .iter()
            .find(|tag| tag.name.eq_ignore_ascii_case(name))
            .and_then(|tag| tag.value.as_str())
    }

    fn string_tags(&self) -> Vec<Tag> {
        self.tags
            .iter()
            .filter_map(|tag| {
                tag.value
                    .as_str()
                    .map(|value| Tag { name: tag.name.clone(), value: value.to_string() })
            })
            .collect()
    }
}
