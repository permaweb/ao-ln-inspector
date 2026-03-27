use crate::core::{constants::NETWORK_VERSION, types::Tag};
use anyhow::{Context, Result};
use reqwest::Client;
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
    pub checked: bool,
    pub has_balances_patch: bool,
    pub cu_sender: Option<String>,
    pub cu_receiver: Option<String>,
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
    let url =
        format!("{}/result/{transfer_id}?process-id={process_id}", cu_url.trim_end_matches('/'));

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

    for message in &response.messages {
        if !message.is_ao_message() {
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

    let cu_sender =
        unique_notice_value(notices.credit.iter().filter_map(|notice| notice.sender.as_deref()));
    let cu_receiver =
        unique_notice_value(notices.debit.iter().filter_map(|notice| notice.recipient.as_deref()));
    let has_balances_patch = response
        .messages
        .iter()
        .any(|message| message.has_balances_patch(cu_sender.as_deref(), cu_receiver.as_deref()));

    Ok(CuTransferResult {
        error: response.error.and_then(|error| {
            let trimmed = error.trim();
            if trimmed.is_empty() { None } else { Some(error) }
        }),
        checked: true,
        has_balances_patch,
        cu_sender,
        cu_receiver,
        pending_notices: notices,
    })
}

impl CuResultMessage {
    fn is_ao_message(&self) -> bool {
        self.tag_value("Data-Protocol").is_some_and(|value| value.eq_ignore_ascii_case("ao"))
            && self
                .tag_value("Variant")
                .is_some_and(|value| value.eq_ignore_ascii_case(NETWORK_VERSION))
            && self.tag_value("Type").is_some_and(|value| value.eq_ignore_ascii_case("Message"))
    }

    fn has_balances_patch(&self, cu_sender: Option<&str>, cu_receiver: Option<&str>) -> bool {
        let Some(cu_sender) = cu_sender else {
            return false;
        };
        let Some(cu_receiver) = cu_receiver else {
            return false;
        };
        let Some(balances) = self.tag_json("balances").and_then(Value::as_object) else {
            return false;
        };

        self.is_ao_message()
            && self
                .tag_json("device")
                .and_then(Value::as_str)
                .is_some_and(|value| value.eq_ignore_ascii_case("patch@1.0"))
            && balances.contains_key(cu_sender)
            && balances.contains_key(cu_receiver)
    }

    fn tag_value(&self, name: &str) -> Option<&str> {
        self.tag_json(name).and_then(Value::as_str)
    }

    fn tag_json(&self, name: &str) -> Option<&Value> {
        self.tags.iter().find(|tag| tag.name.eq_ignore_ascii_case(name)).map(|tag| &tag.value)
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

fn unique_notice_value<'a>(mut values: impl Iterator<Item = &'a str>) -> Option<String> {
    let first = values.find(|value| !value.trim().is_empty())?;
    if values.all(|value| value == first) { Some(first.to_string()) } else { None }
}
