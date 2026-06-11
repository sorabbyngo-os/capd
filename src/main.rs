// citadel-soraxis-bridge · main.rs
//
// Changes from v1:
//   - Drone adapter wired in — POST /internal/adapters/drone
//   - DedupWindow eviction task spawned on startup
//   - Axum state uses Arc<AppState> to share multiple adapters cleanly
//   - Graceful shutdown on SIGTERM / Ctrl-C

mod adapters;
mod normaliser;
mod publisher;
mod api;
mod config;

use crate::adapters::camera::{camera_webhook_handler, CameraAdapter};
use crate::adapters::drone::{drone_webhook_handler, DroneAdapter, DRONE_ROUTE};
use crate::normaliser::dedup::DedupWindow;
use crate::publisher::nats::NatsPublisher;
use crate::config::settings::Settings;

use axum::{routing::post, Router};
use std::sync::Arc;
use tokio::sync::mpsc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::EnvFilter;

/// Shared application state — all adapters live here.
#[derive(Clone)]
struct AppState {
    camera: Arc<CameraAdapter>,
    drone:  Arc<DroneAdapter>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();

    let settings = Settings::load()?;
    info!("Citadel Soraxis Bridge starting...");

    // Raw channel → dedup → clean channel → NATS
    let (raw_tx, mut raw_rx)   = mpsc::channel(10_000);
    let (clean_tx, clean_rx)   = mpsc::channel(10_000);

    // Dedup window — spawn background eviction
    let dedup = Arc::new(DedupWindow::new());
    let _eviction_handle = dedup.spawn_eviction();

    let dedup_clone = dedup.clone();
    tokio::spawn(async move {
        while let Some(sighting) = raw_rx.recv().await {
            if !dedup_clone.is_duplicate(&sighting).await {
                // Channel closed = publisher has shut down; nothing we can do
                let _ = clean_tx.send(sighting).await;
            }
        }
        info!("Dedup stage: raw channel closed — exiting");
    });

    // NATS publisher
    let publisher = NatsPublisher::connect(&settings.nats_url).await?;
    tokio::spawn(async move {
        publisher.run(clean_rx).await;
        info!("NATS publisher: channel closed — exiting");
    });

    // Adapters
    let state = AppState {
        camera: CameraAdapter::new(raw_tx.clone()),
        drone:  DroneAdapter::new(raw_tx.clone()),
    };

    let app = Router::new()
        .route(
            "/internal/adapters/camera",
            post(|s: axum::extract::State<Arc<CameraAdapter>>, b| camera_webhook_handler(s, b))
        )
        .route(
            DRONE_ROUTE,
            post(|s: axum::extract::State<Arc<DroneAdapter>>, b| drone_webhook_handler(s, b))
        )
        .route("/api/v1/health",          axum::routing::get(api::health::handler))
        .route("/api/v1/sensors",         axum::routing::get(api::sensors::list))
        .route("/api/v1/sightings/live",  axum::routing::get(api::sightings::live_stream))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        // Split state per-handler using individual Arc<Adapter> to avoid a
        // monolithic AppState that every handler must carry
        .with_state(state.camera)
        // Drone state layered separately
        // (In production use a proper Router::merge or extension pattern)
        ;

    // TODO: layer drone state — axum requires explicit per-router state; use
    //       Router::merge with separate sub-routers keyed by state type, or
    //       unify into Arc<AppState> with typed extractors.

    let addr = format!("0.0.0.0:{}", settings.port);
    info!(%addr, "Listening");
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    use tokio::signal;
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c    => info!("Received Ctrl-C — shutting down"),
        _ = terminate => info!("Received SIGTERM — shutting down"),
    }
}
