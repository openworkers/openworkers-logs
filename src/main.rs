mod db;
mod nats;
mod routes;

use actix_web::{middleware, web, App, HttpServer};
use chrono::Utc;
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::broadcast;
use uuid::Uuid;

use db::{create_pool, insert_log, LogEntry, LogLevel};
use routes::{get_worker_logs, health, stream_worker_logs, AppState};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();

    log::info!("Starting OpenWorkers Logs Service...");

    // Connect to database
    let pool = create_pool().await.expect("Failed to connect to database");

    // Create broadcast channel for SSE
    let (tx, _rx) = broadcast::channel::<LogEntry>(100);
    let broadcaster = Arc::new(tx);

    // Spawn NATS subscriber task
    let pool_clone = pool.clone();
    let broadcaster_clone = broadcaster.clone();

    tokio::spawn(async move {
        log::info!("Starting NATS subscriber...");

        let nc = nats::nats_connect().await;

        // Subscribe to all worker logs: *.console.*
        let mut sub = nc
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

            let level = match level_str {
                "error" => LogLevel::Error,
                "warn" => LogLevel::Warn,
                "info" => LogLevel::Info,
                "log" => LogLevel::Log,
                "debug" => LogLevel::Debug,
                "trace" => LogLevel::Trace,
                _ => {
                    log::warn!("Unknown log level: {}", level_str);
                    LogLevel::Info
                }
            };

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
            if let Err(e) = insert_log(&pool_clone, log_entry.clone()).await {
                log::error!("Failed to insert log: {:?}", e);
            }

            // Broadcast to SSE subscribers
            let _ = broadcaster_clone.send(log_entry);
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
                broadcaster: broadcaster.clone(),
            }))
            .wrap(middleware::Logger::default())
            .service(health)
            .service(get_worker_logs)
            .service(stream_worker_logs)
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await
}
