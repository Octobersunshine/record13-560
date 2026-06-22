use serde::{Deserialize, Serialize};
use uuid::Uuid;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptSegment {
    pub id: Uuid,
    pub index: usize,
    pub text: String,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveScript {
    pub id: Uuid,
    pub title: String,
    pub created_at: DateTime<Utc>,
    pub segments: Vec<ScriptSegment>,
    pub total_duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BroadcastStatus {
    Idle,
    Running,
    Paused,
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastState {
    pub script_id: Option<Uuid>,
    pub status: BroadcastStatus,
    pub current_segment_index: Option<usize>,
    pub current_segment: Option<ScriptSegment>,
    pub started_at: Option<DateTime<Utc>>,
    pub next_push_at: Option<DateTime<Utc>>,
    pub segments_broadcasted: usize,
    pub segments_total: usize,
}

impl Default for BroadcastState {
    fn default() -> Self {
        Self {
            script_id: None,
            status: BroadcastStatus::Idle,
            current_segment_index: None,
            current_segment: None,
            started_at: None,
            next_push_at: None,
            segments_broadcasted: 0,
            segments_total: 0,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct UploadScriptRequest {
    pub title: String,
    pub content: String,
    #[serde(default = "default_segment_duration")]
    pub default_duration_ms: u64,
    #[serde(default)]
    pub segment_by: SegmentBy,
}

fn default_segment_duration() -> u64 {
    5000
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SegmentBy {
    #[default]
    Paragraph,
    Sentence,
    Newline,
    FixedLength,
}

#[derive(Debug, Serialize)]
pub struct UploadScriptResponse {
    pub script_id: Uuid,
    pub title: String,
    pub segments_count: usize,
    pub total_duration_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct CurrentBroadcastResponse {
    pub state: BroadcastState,
    pub script: Option<LiveScript>,
}

#[derive(Debug, Deserialize)]
pub struct ControlRequest {
    pub action: ControlAction,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlAction {
    Start,
    Pause,
    Resume,
    Stop,
    Next,
    Prev,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}
