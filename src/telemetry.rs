//! Log export, so this service reports where the runner already does.
//!
//! Three shapes, in order of how much is configured:
//! - without the `telemetry` feature: `env_logger`, and `RUST_LOG` decides
//! - with it, no `OTLP_ENDPOINT`: the same lines on the console, through tracing
//! - with it and an endpoint: the same, plus an OTLP batch export
//!
//! The crate keeps its `log::` macros throughout; `tracing-log` bridges them,
//! which is why no call site mentions tracing.

/// Set up logging for the process. Call once, before anything logs.
pub fn init() {
    #[cfg(not(feature = "telemetry"))]
    {
        env_logger::init();
    }

    #[cfg(feature = "telemetry")]
    {
        match std::env::var("OTLP_ENDPOINT") {
            Ok(endpoint) => {
                if let Err(err) = init_otlp(endpoint) {
                    eprintln!("OTLP export unavailable, logging to console: {err}");
                    init_console();
                }
            }
            Err(_) => init_console(),
        }
    }
}

#[cfg(feature = "telemetry")]
fn filter() -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
}

#[cfg(feature = "telemetry")]
fn init_console() {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    tracing_subscriber::registry()
        .with(filter())
        .with(tracing_subscriber::fmt::layer())
        .init();
}

#[cfg(feature = "telemetry")]
fn init_otlp(endpoint: String) -> Result<(), Box<dyn std::error::Error>> {
    use opentelemetry::KeyValue;
    use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_otlp::WithTonicConfig;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::logs::SdkLoggerProvider;
    use tonic::metadata::MetadataKey;
    use tonic::metadata::MetadataMap;
    use tonic::metadata::MetadataValue;
    use tonic::transport::ClientTlsConfig;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let mut metadata = MetadataMap::new();

    // OTLP_HEADERS carries the collector's credentials, as key=value,key=value
    for header in std::env::var("OTLP_HEADERS").unwrap_or_default().split(',') {
        let Some((key, value)) = header.trim().split_once('=') else {
            continue;
        };

        if let (Ok(key), Ok(value)) = (
            MetadataKey::from_bytes(key.trim().as_bytes()),
            MetadataValue::try_from(value.trim()),
        ) {
            metadata.insert(key, value);
        }
    }

    let mut exporter = opentelemetry_otlp::LogExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .with_metadata(metadata);

    if endpoint.starts_with("https://") {
        exporter = exporter.with_tls_config(ClientTlsConfig::new().with_native_roots());
    }

    // An instance id separates the replicas of one service in the collector
    let instance_id = uuid::Uuid::new_v4().to_string()[..8].to_string();

    let service_name =
        std::env::var("OTLP_SERVICE_NAME").unwrap_or_else(|_| env!("CARGO_PKG_NAME").to_string());

    let resource = Resource::builder()
        .with_attribute(KeyValue::new("service.name", service_name))
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        .with_attribute(KeyValue::new("service.instance.id", instance_id))
        .build();

    let provider = SdkLoggerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(exporter.build()?)
        .build();

    tracing_subscriber::registry()
        .with(filter())
        .with(tracing_subscriber::fmt::layer())
        .with(OpenTelemetryTracingBridge::new(&provider))
        .init();

    Ok(())
}
