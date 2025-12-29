use base64::engine::Engine;
use base64::engine::general_purpose::STANDARD;

pub async fn nats_connect() -> async_nats::Client {
    let nats_servers = std::env::var("NATS_SERVERS").expect("NATS_SERVERS must be set");

    log::info!("Connecting to NATS: {}", nats_servers);

    let client = match std::env::var("NATS_CREDENTIALS") {
        Ok(credentials) if !credentials.is_empty() => {
            // Decode the base64 encoded credentials
            let credentials: Vec<u8> = STANDARD
                .decode(credentials)
                .expect("Failed to decode NATS credentials");
            let credentials_str = String::from_utf8(credentials)
                .expect("Failed to convert NATS credentials to string");

            async_nats::ConnectOptions::with_credentials(&credentials_str)
                .expect("Failed to create NATS options")
                .connect(&nats_servers)
                .await
        }
        _ => async_nats::connect(&nats_servers).await,
    };

    client.expect("Failed to connect to NATS")
}
