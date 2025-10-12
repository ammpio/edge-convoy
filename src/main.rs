use clap::Parser;
use convoy::{Bridge, CacheManager, Config};
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "convoy")]
#[command(about = "MQTT bridge with SQLite cache", long_about = None)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "config.toml")]
    config: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Initialize logging
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .init();

    info!("Convoy MQTT Bridge starting...");
    info!("Loading configuration from: {}", args.config);

    // Load configuration
    let config = match Config::from_file(&args.config) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to load configuration: {}", e);
            std::process::exit(1);
        }
    };

    // Initialize cache
    let cache = match CacheManager::new(config.cache.clone()) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            error!("Failed to initialize cache: {}", e);
            std::process::exit(1);
        }
    };

    // Show cache status
    match cache.count() {
        Ok(count) if count > 0 => {
            info!("Cache contains {} messages from previous session", count);
        }
        Ok(_) => {
            info!("Cache is empty");
        }
        Err(e) => {
            error!("Failed to query cache: {}", e);
        }
    }

    // Create and run bridge
    match Bridge::new(config.bridge, cache).await {
        Ok(bridge) => {
            info!("Bridge initialized, starting event loop...");
            if let Err(e) = bridge.run().await {
                error!("Bridge error: {}", e);
                std::process::exit(1);
            }
        }
        Err(e) => {
            error!("Failed to create bridge: {}", e);
            std::process::exit(1);
        }
    }

    info!("Convoy shutting down");
}
