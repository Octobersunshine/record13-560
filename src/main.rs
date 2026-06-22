mod models;
mod segmenter;
mod broadcaster;
mod handlers;

use std::net::SocketAddr;

use axum::{
    routing::{get, post},
    Router,
};
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::broadcaster::Broadcaster;
use crate::handlers::*;

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "live_script_broadcaster=info,tower_http=info,axum=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let broadcaster = Broadcaster::new();

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/health", get(health_check))
        .route("/api/script", post(upload_script))
        .route("/api/broadcast/state", get(get_broadcast_state))
        .route("/api/broadcast/control", post(control_broadcast))
        .route("/api/broadcast/ack", post(acknowledge_segment))
        .route("/api/broadcast/stream", get(broadcast_sse))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(broadcaster);

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    tracing::info!("服务启动，监听地址: {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
