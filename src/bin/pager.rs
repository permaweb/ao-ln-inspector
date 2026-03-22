use ao_ln_inspector::pager::runner::run_forever;
use dotenvy::dotenv;

#[tokio::main]
async fn main() {
    dotenv().ok();
    run_forever().await.expect("pager runner failed");
}
