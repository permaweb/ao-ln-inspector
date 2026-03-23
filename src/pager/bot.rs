use crate::core::env_var::get_env_var;
use anyhow::Error;
use reqwest::Client;
use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct Message {
    text: String,
    chat_id: String,
    disable_web_page_preview: bool,
    parse_mode: String,
}

pub async fn send_block_result(res: String) -> Result<(), Error> {
    let bot_api_key = get_env_var("TELEGRAM_BOT_KEY")?;
    let chat_id = get_env_var("TG_GC_PAGER_ID")?;
    let url = format!("https://api.telegram.org/bot{bot_api_key}/sendMessage");
    let body = Message {
        text: res,
        chat_id,
        disable_web_page_preview: true,
        parse_mode: "HTML".to_string(),
    };
    let body_json = serde_json::to_value(&body)?;

    let res =
        Client::new().post(url).json(&body_json).send().await?.error_for_status()?.text().await?;

    println!("bot update res: {res}");

    Ok(())
}
