use actix_web::{get, web, HttpResponse, Responder};
use actix_web_lab::sse::{self, Sse};
use futures::StreamExt;
use sqlx::PgPool;
use uuid::Uuid;

use crate::db::{get_logs, LogEntry};

pub struct AppState {
    pub pool: PgPool,
    pub nats_client: async_nats::Client,
}

// Helper struct for creating SSE log events from raw data
struct LogEventData {
    date: i64,
    level: String,
    message: String,
}

impl Into<sse::Data> for LogEntry {
    fn into(self) -> sse::Data {
        sse::Data::new_json(serde_json::json!({
            "date": self.date.timestamp_millis(),
            "level": format!("{:?}", self.level).to_lowercase(),
            "message": self.message
        }))
        .unwrap()
        .event("log")
    }
}

impl Into<sse::Data> for LogEventData {
    fn into(self) -> sse::Data {
        sse::Data::new_json(serde_json::json!({
            "date": self.date,
            "level": self.level,
            "message": self.message
        }))
        .unwrap()
        .event("log")
    }
}

#[get("/api/v1/health")]
pub async fn health() -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "ok",
        "service": "openworkers-logs"
    }))
}

#[get("/api/v1/workers/{worker_id}/logs")]
pub async fn stream_worker_logs(
    worker_id: web::Path<Uuid>,
    data: web::Data<AppState>,
) -> impl Responder {
    let worker_id = *worker_id;

    // Fetch last 10 logs from database
    let historical_logs = match get_logs(&data.pool, worker_id, 10).await {
        Ok(logs) => logs,
        Err(e) => {
            log::error!("Failed to fetch historical logs: {:?}", e);
            vec![]
        }
    };

    // Subscribe to NATS for this specific worker's logs
    let subject = format!("{}.console.>", worker_id);
    let nats_sub = data.nats_client.subscribe(subject).await.ok();

    if nats_sub.is_none() {
        log::error!("Failed to subscribe to NATS");
    }

    let mut nats_sub = nats_sub.unwrap();

    let mut id_counter: u64 = 0;

    let stream = async_stream::stream! {
        // First, yield historical logs in reverse order (oldest first)
        for log in historical_logs.into_iter().rev() {
            let mut data: sse::Data = log.into();
            data.set_id(format!("{}", id_counter));
            id_counter += 1;
            yield Ok::<_, actix_web::Error>(sse::Event::Data(data));
        }

        // Then stream new logs in real-time from NATS
        while let Some(msg) = nats_sub.next().await {
            // Parse level from subject: {worker_id}.console.{level}
            let level_str = msg.subject.split('.').nth(2).unwrap_or("info");

            let message = match String::from_utf8(msg.payload.to_vec()) {
                Ok(m) => m,
                Err(_) => continue,
            };

            let data = LogEventData {
                date: chrono::Utc::now().timestamp_millis(),
                level: level_str.to_string(),
                message,
            };

            let mut data: sse::Data = data.into();
            data.set_id(format!("{}", id_counter));
            id_counter += 1;

            yield Ok::<_, actix_web::Error>(sse::Event::Data(data));
        }
    };

    Sse::from_stream(stream).with_keep_alive(std::time::Duration::from_secs(5))
}
