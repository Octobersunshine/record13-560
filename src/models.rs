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
pub struct SegmentPushMessage {
    pub sequence: u64,
    pub segment_index: usize,
    pub segment_id: Uuid,
    pub text: String,
    pub duration_ms: u64,
    pub segments_total: usize,
    pub push_timestamp: DateTime<Utc>,
    pub is_retry: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatePushMessage {
    pub sequence: u64,
    pub status: BroadcastStatus,
    pub segment_index: Option<usize>,
    pub segments_total: usize,
    pub segments_acked: usize,
    pub segments_broadcasted: usize,
    pub push_timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct AckRequest {
    pub sequence: u64,
    pub segment_index: usize,
    pub segment_id: Uuid,
    pub play_duration_ms: u64,
    pub ack_timestamp: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct AckResponse {
    pub success: bool,
    pub next_available: bool,
    pub next_segment_index: Option<usize>,
    pub message: String,
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
    pub segments_acked: usize,
    pub segments_total: usize,
    pub last_pushed_sequence: u64,
    pub last_acked_sequence: Option<u64>,
    pub last_pushed_at: Option<DateTime<Utc>>,
    pub last_acked_at: Option<DateTime<Utc>>,
    pub push_mode: PushMode,
    pub pending_ack_count: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PushMode {
    TimeDriven,
    AckDriven,
}

impl Default for PushMode {
    fn default() -> Self {
        PushMode::AckDriven
    }
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
            segments_acked: 0,
            segments_total: 0,
            last_pushed_sequence: 0,
            last_acked_sequence: None,
            last_pushed_at: None,
            last_acked_at: None,
            push_mode: PushMode::default(),
            pending_ack_count: 0,
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
    #[serde(default)]
    pub push_mode: PushMode,
    #[serde(default = "default_max_pending_ack")]
    pub max_pending_ack: usize,
    #[serde(default = "default_ack_timeout_ms")]
    pub ack_timeout_ms: u64,
}

fn default_max_pending_ack() -> usize {
    1
}

fn default_ack_timeout_ms() -> u64 {
    30000
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
