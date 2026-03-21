use ao_ln_inspector::core::{
    constants::SERVER_HOST,
    server::{
        SERVER_PORT, app_state_from_env, handle_ao_token_message, handle_ao_token_transfer,
        handle_ao_token_transfers, handle_route,
    },
};
use axum::{Router, routing::get};
use dotenvy::dotenv;
use tower_http::cors::CorsLayer;

#[tokio::main]
async fn main() {
    dotenv().ok();

    let cors = CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods(tower_http::cors::Any)
        .allow_headers(tower_http::cors::Any);

    let state = app_state_from_env().expect("failed to load application state from environment");

    let router = Router::new()
        .route("/", get(handle_route))
        .route("/v1/token/ao/transfers/{block_id}", get(handle_ao_token_transfers))
        .route("/v1/token/ao/msg/{id}", get(handle_ao_token_message))
        .route("/v1/token/ao/transfer/{id}", get(handle_ao_token_transfer))
        .with_state(state)
        .layer(cors);

    let port = std::env::var("SERVER_PORT").unwrap_or_else(|_| SERVER_PORT.to_string());

    let listener = tokio::net::TcpListener::bind(format!("{SERVER_HOST}:{port}")).await.unwrap();
    println!("Server running on PORT: {port}");
    axum::serve(listener, router).await.unwrap();
}
