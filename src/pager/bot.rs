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

#[derive(Debug, Serialize)]
struct PinMessage {
    chat_id: String,
    message_id: i64,
    disable_notification: bool,
}

#[derive(Debug, Deserialize)]
struct SendMessageResponse {
    result: SendMessageResult,
}

#[derive(Debug, Deserialize)]
struct SendMessageResult {
    message_id: i64,
}

pub async fn send_block_result(res: String, should_pin: bool) -> Result<(), Error> {
    let bot_api_key = get_env_var("TELEGRAM_BOT_KEY")?;
    let chat_id = get_env_var("TG_GC_PAGER_ID")?;
    let send_url = format!("https://api.telegram.org/bot{bot_api_key}/sendMessage");
    let body = Message {
        text: res,
        chat_id: chat_id.clone(),
        disable_web_page_preview: true,
        parse_mode: "HTML".to_string(),
    };
    let client = Client::new();
    let response = client
        .post(send_url)
        .json(&body)
        .send()
        .await?
        .error_for_status()?
        .json::<SendMessageResponse>()
        .await?;

    if should_pin {
        let pin_url = format!("https://api.telegram.org/bot{bot_api_key}/pinChatMessage");
        let pin_body = PinMessage {
            chat_id,
            message_id: response.result.message_id,
            disable_notification: true,
        };
        client.post(pin_url).json(&pin_body).send().await?.error_for_status()?;
    }

    Ok(())
}
