use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json, Sse},
    response::sse::{Event, KeepAlive},
};
use futures::stream::Stream;
use std::convert::Infallible;
use std::time::Duration;
use tracing::{info, debug};

use crate::broadcaster::Broadcaster;
use crate::models::*;
use crate::segmenter::parse_script;

pub async fn upload_script(
    State(broadcaster): State<Broadcaster>,
    Json(req): Json<UploadScriptRequest>,
) -> Result<Json<UploadScriptResponse>, AppError> {
    info!(
        "上传台词: title={}, content_len={}, segment_by={:?}, push_mode={:?}",
        req.title,
        req.content.len(),
        req.segment_by,
        req.push_mode
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

    broadcaster
        .load_script(
            script,
            req.push_mode,
            req.max_pending_ack,
            req.ack_timeout_ms,
        )
        .await;

    Ok(Json(response))
}

pub async fn get_broadcast_state(
    State(broadcaster): State<Broadcaster>,
) -> Json<CurrentBroadcastResponse> {
    let state = broadcaster.get_state().await;
    let script = broadcaster.get_script().await;

    Json(CurrentBroadcastResponse { state, script })
}

pub async fn acknowledge_segment(
    State(broadcaster): State<Broadcaster>,
    Json(req): Json<AckRequest>,
) -> Json<AckResponse> {
    debug!(
        "收到 ACK 请求: seq={}, segment={}",
        req.sequence, req.segment_index
    );
    let response = broadcaster.acknowledge(req).await;
    Json(response)
}

pub async fn control_broadcast(
    State(broadcaster): State<Broadcaster>,
    Json(req): Json<ControlRequest>,
) -> Result<Json<BroadcastState>, AppError> {
    info!(
        "控制播报: action={:?}, target_index={:?}, back_count={:?}, reason={:?}",
        req.action, req.target_index, req.back_count, req.reason
    );
    let state = broadcaster.control(req).await?;
    Ok(Json(state))
}

pub async fn broadcast_sse(
    State(broadcaster): State<Broadcaster>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let notify = broadcaster.get_push_notify();

    let stream = async_stream::stream! {
        let mut last_sequence: u64 = 0;

        let initial_state = broadcaster.get_state().await;
        if initial_state.last_pushed_sequence > 0 {
            if let Some(msg) = broadcaster.get_last_push_message().await {
                let data = serde_json::json!({
                    "type": "segment",
                    "sequence": msg.sequence,
                    "segment_index": msg.segment_index,
                    "segment_id": msg.segment_id,
                    "text": msg.text,
                    "duration_ms": msg.duration_ms,
                    "segments_total": msg.segments_total,
                    "segments_acked": initial_state.segments_acked,
                    "segments_broadcasted": initial_state.segments_broadcasted,
                    "push_timestamp": msg.push_timestamp,
                    "is_retry": msg.is_retry,
                    "status": initial_state.status,
                });
                yield Ok(Event::default().data(data.to_string()));
                last_sequence = msg.sequence;
            }
        }

        let state_msg = broadcaster.create_state_message().await;
        let state_data = serde_json::json!({
            "type": "state",
            "sequence": state_msg.sequence,
            "status": state_msg.status,
            "segment_index": state_msg.segment_index,
            "segments_total": state_msg.segments_total,
            "segments_acked": state_msg.segments_acked,
            "segments_broadcasted": state_msg.segments_broadcasted,
            "push_timestamp": state_msg.push_timestamp,
            "interrupt_reason": initial_state.interrupt_reason,
            "replay_count": initial_state.replay_count,
        });
        yield Ok(Event::default().data(state_data.to_string()));

        loop {
            tokio::select! {
                _ = notify.notified() => {
                    let state = broadcaster.get_state().await;
                    let current_seq = state.last_pushed_sequence;

                    if current_seq > last_sequence {
                        if let Some(msg) = broadcaster.get_last_push_message().await {
                            if msg.sequence > last_sequence {
                                let data = serde_json::json!({
                                    "type": "segment",
                                    "sequence": msg.sequence,
                                    "segment_index": msg.segment_index,
                                    "segment_id": msg.segment_id,
                                    "text": msg.text,
                                    "duration_ms": msg.duration_ms,
                                    "segments_total": msg.segments_total,
                                    "segments_acked": state.segments_acked,
                                    "segments_broadcasted": state.segments_broadcasted,
                                    "push_timestamp": msg.push_timestamp,
                                    "is_retry": msg.is_retry,
                                    "status": state.status,
                                    "pending_ack_count": state.pending_ack_count,
                                });
                                yield Ok(Event::default().data(data.to_string()));
                                last_sequence = msg.sequence;

                                debug!(
                                    "SSE推送段落 seq={}, segment={}/{}, text={}",
                                    msg.sequence,
                                    msg.segment_index + 1,
                                    msg.segments_total,
                                    truncate(&msg.text, 40)
                                );
                            }
                        }

                        let state_msg = broadcaster.create_state_message().await;
                        let state_data = serde_json::json!({
                            "type": "state",
                            "sequence": state_msg.sequence,
                            "status": state_msg.status,
                            "segment_index": state_msg.segment_index,
                            "segments_total": state_msg.segments_total,
                            "segments_acked": state_msg.segments_acked,
                            "segments_broadcasted": state_msg.segments_broadcasted,
                            "push_timestamp": state_msg.push_timestamp,
                            "pending_ack_count": state.pending_ack_count,
                            "push_mode": state.push_mode,
                            "interrupt_reason": state.interrupt_reason,
                            "replay_count": state.replay_count,
                        });
                        yield Ok(Event::default().data(state_data.to_string()));
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(15)) => {
                    let state = broadcaster.get_state().await;
                    let state_msg = StatePushMessage {
                        sequence: state.last_pushed_sequence,
                        status: state.status.clone(),
                        segment_index: state.current_segment_index,
                        segments_total: state.segments_total,
                        segments_acked: state.segments_acked,
                        segments_broadcasted: state.segments_broadcasted,
                        push_timestamp: chrono::Utc::now(),
                    };
                    let heartbeat = serde_json::json!({
                        "type": "heartbeat",
                        "sequence": state_msg.sequence,
                        "status": state_msg.status,
                        "push_timestamp": state_msg.push_timestamp,
                    });
                    yield Ok(Event::default().data(heartbeat.to_string()));
                }
            }
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

fn truncate(s: &str, max_chars: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max_chars {
        s.to_string()
    } else {
        let mut result: String = chars.iter().take(max_chars).collect();
        result.push_str("...");
        result
    }
}
