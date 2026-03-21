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
    pub message: Option<AoMessage>,
    pub assignment: Assignment,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct AoMessage {
    pub id: String,
    pub owner: Owner,
    pub data: Option<String>,
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

impl Assignment {
    pub fn tag_value(&self, name: &str) -> Option<&str> {
        self.tags
            .iter()
            .find(|tag| tag.name.eq_ignore_ascii_case(name))
            .map(|tag| tag.value.as_str())
    }

    pub fn block_height(&self) -> Option<&str> {
        self.tag_value("Block-Height")
    }

    pub fn matches_block_height(&self, query: &str) -> bool {
        self.block_height()
            .is_some_and(|height| normalize_block_height(height) == normalize_block_height(query))
    }
}

impl AoMessage {
    pub fn tag_value(&self, name: &str) -> Option<&str> {
        self.tags
            .iter()
            .find(|tag| tag.name.eq_ignore_ascii_case(name))
            .map(|tag| tag.value.as_str())
    }
}

pub fn normalize_block_height(value: &str) -> String {
    let trimmed = value.trim();
    let normalized = trimmed.trim_start_matches('0');
    if normalized.is_empty() { "0".to_string() } else { normalized.to_string() }
}

fn empty_string_is_none<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    Ok(value.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() { None } else { Some(value) }
    }))
}

#[cfg(test)]
mod tests {
    use super::{AoMessage, Assignment, Owner, Tag, normalize_block_height};

    fn assignment_with_block_height(value: &str) -> Assignment {
        Assignment {
            id: "assignment".to_string(),
            owner: Owner { address: "owner".to_string(), key: "key".to_string() },
            tags: vec![Tag { name: "Block-Height".to_string(), value: value.to_string() }],
            signature: "signature".to_string(),
            anchor: None,
            target: None,
        }
    }

    #[test]
    fn normalizes_block_height_queries() {
        assert_eq!(normalize_block_height("000001606027"), "1606027");
        assert_eq!(normalize_block_height("1606027"), "1606027");
        assert_eq!(normalize_block_height("0000"), "0");
    }

    #[test]
    fn matches_padded_and_unpadded_block_heights() {
        let assignment = assignment_with_block_height("000001606027");
        assert!(assignment.matches_block_height("1606027"));
        assert!(assignment.matches_block_height("000001606027"));
        assert!(!assignment.matches_block_height("1606028"));
    }

    #[test]
    fn message_tag_lookup_is_case_insensitive_for_names() {
        let message = AoMessage {
            id: "message".to_string(),
            owner: Owner { address: "owner".to_string(), key: "key".to_string() },
            data: None,
            tags: vec![Tag { name: "Action".to_string(), value: "Transfer".to_string() }],
            signature: "signature".to_string(),
            anchor: None,
            target: None,
        };

        assert_eq!(message.tag_value("Action"), Some("Transfer"));
        assert_eq!(message.tag_value("action"), Some("Transfer"));
    }
}
