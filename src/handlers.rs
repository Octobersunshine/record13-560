use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json, Sse},
    response::sse::{Event, KeepAlive},
};
use futures::stream::Stream;
use std::convert::Infallible;
use std::time::Duration;
use tracing::info;

use crate::broadcaster::Broadcaster;
use crate::models::*;
use crate::segmenter::parse_script;

pub async fn upload_script(
    State(broadcaster): State<Broadcaster>,
    Json(req): Json<UploadScriptRequest>,
) -> Result<Json<UploadScriptResponse>, AppError> {
    info!(
        "上传台词: title={}, content_len={}, segment_by={:?}",
        req.title,
        req.content.len(),
        req.segment_by
    );

    let script = parse_script(
        &req.title,
        &req.content,
        req.default_duration_ms,
        req.segment_by,
    );

    let response = UploadScriptResponse {
        script_id: script.id,
        title: script.title.clone(),
        segments_count: script.segments.len(),
        total_duration_ms: script.total_duration_ms,
    };

    broadcaster.load_script(script).await;

    Ok(Json(response))
}

pub async fn get_broadcast_state(
    State(broadcaster): State<Broadcaster>,
) -> Json<CurrentBroadcastResponse> {
    let state = broadcaster.get_state().await;
    let script = broadcaster.get_script().await;

    Json(CurrentBroadcastResponse { state, script })
}

pub async fn control_broadcast(
    State(broadcaster): State<Broadcaster>,
    Json(req): Json<ControlRequest>,
) -> Result<Json<BroadcastState>, AppError> {
    info!("控制播报: action={:?}", req.action);
    let state = broadcaster.control(req.action).await?;
    Ok(Json(state))
}

pub async fn broadcast_sse(
    State(broadcaster): State<Broadcaster>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = async_stream::stream! {
        let mut last_text: Option<String> = None;
        loop {
            let state = broadcaster.get_state().await;
            let current_text = state.current_segment.as_ref().map(|s| s.text.clone());

            if current_text != last_text {
                if let Some(text) = &current_text {
                    let data = serde_json::json!({
                        "type": "segment",
                        "text": text,
                        "segment_index": state.current_segment_index,
                        "segments_total": state.segments_total,
                        "segments_broadcasted": state.segments_broadcasted,
                        "status": state.status,
                        "duration_ms": state.current_segment.as_ref().map(|s| s.duration_ms),
                    });
                    yield Ok(Event::default().data(data.to_string()));
                }

                let status_data = serde_json::json!({
                    "type": "state",
                    "status": state.status,
                    "segment_index": state.current_segment_index,
                    "segments_total": state.segments_total,
                    "segments_broadcasted": state.segments_broadcasted,
                });
                yield Ok(Event::default().data(status_data.to_string()));

                last_text = current_text;
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::default())
}

pub async fn health_check() -> &'static str {
    "OK"
}

pub struct AppError(pub String);

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let status = StatusCode::BAD_REQUEST;
        let body = Json(ErrorResponse { error: self.0 });
        (status, body).into_response()
    }
}

impl From<String> for AppError {
    fn from(s: String) -> Self {
        AppError(s)
    }
}
