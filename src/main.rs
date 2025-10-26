use clap::Parser;
use convoy::{Bridge, Config};
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
    #[arg(short, long, default_value = "debug")]
    log_level: String,
}

fn main() {
    let args = Args::parse();

    // Initialize logging
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .compact()
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

    // Create and run bridge
    match Bridge::new(config.bridge, config.cache) {
        Ok(bridge) => {
            info!("Bridge initialized, starting event loop...");
            if let Err(e) = bridge.run() {
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
