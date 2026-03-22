use crate::core::env_var::get_env_var;
use anyhow::Error;
use reqwest::Client;
use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct Message {
    text: String,
    chat_id: String,
    disable_web_page_preview: bool,
}

pub async fn send_block_result(res: String) -> Result<(), Error> {
    let BOT_API_KEY = get_env_var("TELEGRAM_BOT_KEY")?;
    let CHAT_ID = get_env_var("TG_GC_PAGER_ID")?;
    let url = format!("https://api.telegram.org/bot{BOT_API_KEY}/sendMessage");
    let body = Message { text: res, chat_id: CHAT_ID, disable_web_page_preview: true };
    let body_json = serde_json::to_value(&body)?;

    let res =
        Client::new().post(url).json(&body_json).send().await?.error_for_status()?.text().await?;

    println!("bot update res: {res}");

    Ok(())
}
