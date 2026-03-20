use serde::{Deserialize, Deserializer, Serialize};

/// Response envelope returned by the `su-router` process history endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ProcessHistoryResponse {
    pub page_info: PageInfo,
    pub edges: Vec<HistoryEdge>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PageInfo {
    pub has_next_page: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct HistoryEdge {
    pub node: HistoryNode,
    pub cursor: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct HistoryNode {
    pub message: AoMessage,
    pub assignment: Assignment,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct AoMessage {
    pub id: String,
    pub owner: Owner,
    pub data: String,
    pub tags: Vec<Tag>,
    pub signature: String,
    pub anchor: Option<String>,
    #[serde(default, deserialize_with = "empty_string_is_none")]
    pub target: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Assignment {
    pub id: String,
    pub owner: Owner,
    pub tags: Vec<Tag>,
    pub signature: String,
    pub anchor: Option<String>,
    #[serde(default, deserialize_with = "empty_string_is_none")]
    pub target: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Owner {
    pub address: String,
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Tag {
    pub name: String,
    pub value: String,
}

fn empty_string_is_none<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    Ok(value.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(value)
        }
    }))
}
