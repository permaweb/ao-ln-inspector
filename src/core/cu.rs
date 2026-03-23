use crate::core::constants::NETWORK_VERSION;
use anyhow::{Context, Result};
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashSet;

#[derive(Debug, Clone, Default)]
pub struct CuNoticeReferences {
    pub credit: Vec<String>,
    pub debit: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CuResultResponse {
    #[serde(rename = "Messages", default)]
    messages: Vec<CuResultMessage>,
}

#[derive(Debug, Deserialize)]
struct CuResultMessage {
    #[serde(rename = "Tags", default)]
    tags: Vec<CuResultTag>,
}

#[derive(Debug, Deserialize)]
struct CuResultTag {
    name: String,
    value: Value,
}

pub async fn fetch_notice_references_for_transfer(
    client: &Client,
    cu_url: &str,
    process_id: &str,
    transfer_id: &str,
) -> Result<CuNoticeReferences> {
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

    let mut references = CuNoticeReferences::default();
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
        let Some(reference) = message.tag_value("Reference") else {
            continue;
        };

        if action.eq_ignore_ascii_case("Credit-Notice") && seen_credit.insert(reference.to_string())
        {
            references.credit.push(reference.to_string());
        } else if action.eq_ignore_ascii_case("Debit-Notice")
            && seen_debit.insert(reference.to_string())
        {
            references.debit.push(reference.to_string());
        }
    }

    Ok(references)
}

impl CuResultMessage {
    fn tag_value(&self, name: &str) -> Option<&str> {
        self.tags
            .iter()
            .find(|tag| tag.name.eq_ignore_ascii_case(name))
            .and_then(|tag| tag.value.as_str())
    }
}
