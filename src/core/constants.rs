pub const APP_NAME: &str = "ao-ln-inspector";
pub const SERVER_HOST: &str = "0.0.0.0";
pub const SERVER_PORT: u16 = 3131;
pub const DEFAULT_SU_URL: &str = "https://su-router.ao-testnet.xyz";
pub const DEFAULT_ARWEAVE_URL: &str = "https://arweave.net";
pub const DEFAULT_GQL_URL: &str = "https://arweave.net/graphql";
pub const DEFAULT_AO_TOKEN_PROCESS_ID: &str = "0syT13r0s0tgPmIed95bJnuSqaD29HQNN8D3ElLSrsc";
pub const AO_LN_AUTHORITY: &str = "fcoN_xJeisVsPXA-trzVAuIiqO3ydLQxM-L4XbrQKzY";
pub const DEFAULT_PAGE_SIZE: usize = 100;
pub const NETWORK_VERSION: &str = "ao.TN.1";

pub const AO_TOKEN_SYMBOL: &str = "ao";

pub(crate) const GQL_BATCH_SIZE: usize = 100;
pub(crate) const GQL_NOTICE_BATCH_SIZE: usize = 100;

pub(crate) const SETTLEMENT_HEIGHTS_QUERY: &str = r#"
query SettlementHeights($ids: [ID!]!) {
  transactions(ids: $ids) {
    edges {
      node {
        id
        block {
          height
        }
        bundledIn {
          id
        }
      }
    }
  }
}
"#;

pub(crate) const SETTLED_NOTICES_BY_CORRELATION_QUERY: &str = r#"
query SettledNoticesByCorrelation(
  $correlationIds: [String!]!,
  $fromProcessIds: [String!]!,
  $owners: [String!]!,
  $after: String
) {
  transactions(
    first: 100
    after: $after
    sort: HEIGHT_ASC
    owners: $owners
    tags: [
      { name: "Action", values: ["Credit-Notice", "Debit-Notice"] }
      { name: "Pushed-For", values: $correlationIds }
      { name: "From-Process", values: $fromProcessIds }
      { name: "Data-Protocol", values: ["ao"] }
      { name: "Type", values: ["Message"] }
    ]
  ) {
    pageInfo {
      hasNextPage
    }
    edges {
      cursor
      node {
        id
        owner {
          address
        }
        recipient
        tags {
          name
          value
        }
        block {
          height
        }
        bundledIn {
          id
        }
      }
    }
  }
}
"#;
