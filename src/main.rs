mod db;
mod nats;
mod routes;

use actix_web::{App, HttpServer, middleware, web};
use chrono::Utc;
use futures::StreamExt;
use uuid::Uuid;

use db::{LogEntry, LogLevel, create_pool, insert_log};
use routes::{AppState, health, stream_worker_logs};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();
    env_logger::init();

    log::info!("Starting OpenWorkers Logs Service...");

    // Connect to database
    let pool = create_pool().await.expect("Failed to connect to database");

    // Connect to NATS
    let nats_client = nats::nats_connect().await;

    // Spawn NATS subscriber task for database persistence
    let pool_clone = pool.clone();
    let nats_clone = nats_client.clone();

    tokio::spawn(async move {
        log::info!("Starting NATS subscriber for database persistence...");

        // Subscribe to all worker logs: *.console.*
        let mut sub = nats_clone
            .subscribe("*.console.*")
            .await
            .expect("Failed to subscribe to NATS");

        log::info!("Subscribed to *.console.* on NATS");

        while let Some(msg) = sub.next().await {
            // Parse subject: {worker_id}.console.{level}
            let subject_parts: Vec<&str> = msg.subject.split('.').collect();

            if subject_parts.len() != 3 {
                log::warn!("Invalid subject format: {}", msg.subject);
                continue;
            }

            let worker_id_str = subject_parts[0];
            let level_str = subject_parts[2];

            let worker_id = match Uuid::parse_str(worker_id_str) {
                Ok(id) => id,
                Err(e) => {
                    log::warn!("Invalid worker_id in subject {}: {}", msg.subject, e);
                    continue;
                }
            };

            let level = level_str.parse().unwrap_or(LogLevel::Info);

            let message = match String::from_utf8(msg.payload.to_vec()) {
                Ok(m) => m,
                Err(e) => {
                    log::warn!("Invalid UTF-8 in message: {}", e);
                    continue;
                }
            };

            let log_entry = LogEntry {
                date: Utc::now(),
                worker_id,
                message: message.clone(),
                level,
            };

            // Insert into database
            if let Err(e) = insert_log(&pool_clone, log_entry).await {
                log::error!("Failed to insert log: {:?}", e);
            }
        }

        log::warn!("NATS subscriber stopped");
    });

    // Start HTTP server
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse::<u16>()
        .expect("Invalid PORT");

    log::info!("Starting HTTP server on 0.0.0.0:{}", port);

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(AppState {
                pool: pool.clone(),
                nats_client: nats_client.clone(),
            }))
            .wrap(middleware::Logger::default())
            .service(health)
            .service(stream_worker_logs)
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await
}
