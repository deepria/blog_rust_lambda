use lambda_http::{run, service_fn, tracing, Error};
mod api;
mod config;
mod domain;
mod dynamodb;
mod gemini;
mod http_handler;
mod routes;
mod s3;

use http_handler::function_handler;

#[tokio::main]
async fn main() -> Result<(), Error> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    tracing::init_default_subscriber();

    run(service_fn(function_handler)).await
}
