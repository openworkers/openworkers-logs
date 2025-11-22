use actix_web::{get, web, HttpResponse, Responder};
use actix_web_lab::sse::{self, Sse};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::db::{get_logs, LogEntry};

pub struct AppState {
    pub pool: PgPool,
    pub broadcaster: Arc<broadcast::Sender<LogEntry>>,
}

#[get("/health")]
pub async fn health() -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "ok",
        "service": "openworkers-logs"
    }))
}

#[get("/logs/{worker_id}")]
pub async fn get_worker_logs(
    worker_id: web::Path<Uuid>,
    data: web::Data<AppState>,
) -> impl Responder {
    match get_logs(&data.pool, *worker_id, 100).await {
        Ok(logs) => HttpResponse::Ok().json(logs),
        Err(e) => {
            log::error!("Failed to fetch logs: {:?}", e);
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Failed to fetch logs"
            }))
        }
    }
}

#[get("/logs/{worker_id}/stream")]
pub async fn stream_worker_logs(
    worker_id: web::Path<Uuid>,
    data: web::Data<AppState>,
) -> impl Responder {
    let worker_id = *worker_id;
    let mut rx = data.broadcaster.subscribe();

    let stream = async_stream::stream! {
        while let Ok(log) = rx.recv().await {
            // Only send logs for this worker
            if log.worker_id == worker_id {
                let event = sse::Event::Data(
                    sse::Data::new_json(serde_json::json!({
                        "date": log.date,
                        "message": log.message,
                        "level": log.level
                    }))
                    .unwrap()
                );
                yield Ok::<_, actix_web::Error>(event);
            }
        }
    };

    Sse::from_stream(stream).with_keep_alive(std::time::Duration::from_secs(5))
}
