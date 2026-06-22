use std::sync::Arc;
use tokio::sync::{RwLock, Notify};
use tokio::time::{self, Duration};
use chrono::{DateTime, Utc};
use tracing::{info, warn};

use crate::models::{
    AckRequest, AckResponse, BroadcastState, BroadcastStatus, ControlAction,
    LiveScript, PushMode, SegmentPushMessage, StatePushMessage,
};

#[derive(Clone)]
pub struct Broadcaster {
    inner: Arc<RwLock<BroadcasterInner>>,
    push_notify: Arc<Notify>,
}

struct BroadcasterInner {
    script: Option<LiveScript>,
    state: BroadcastState,
    should_stop: bool,
    should_pause: bool,
    max_pending_ack: usize,
    ack_timeout_ms: u64,
    last_push_message: Option<SegmentPushMessage>,
    retry_count: u32,
    max_retries: u32,
}

impl Broadcaster {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(BroadcasterInner {
                script: None,
                state: BroadcastState::default(),
                should_stop: false,
                should_pause: false,
                max_pending_ack: 1,
                ack_timeout_ms: 30000,
                last_push_message: None,
                retry_count: 0,
                max_retries: 3,
            })),
            push_notify: Arc::new(Notify::new()),
        }
    }

    pub async fn load_script(
        &self,
        script: LiveScript,
        push_mode: PushMode,
        max_pending_ack: usize,
        ack_timeout_ms: u64,
    ) {
        let mut inner = self.inner.write().await;
        let total = script.segments.len();
        inner.script = Some(script.clone());
        inner.state = BroadcastState {
            script_id: Some(script.id),
            status: BroadcastStatus::Idle,
            current_segment_index: None,
            current_segment: None,
            started_at: None,
            next_push_at: None,
            segments_broadcasted: 0,
            segments_acked: 0,
            segments_total: total,
            last_pushed_sequence: 0,
            last_acked_sequence: None,
            last_pushed_at: None,
            last_acked_at: None,
            push_mode,
            pending_ack_count: 0,
        };
        inner.max_pending_ack = max_pending_ack;
        inner.ack_timeout_ms = ack_timeout_ms;
        inner.should_stop = false;
        inner.should_pause = false;
        inner.last_push_message = None;
        inner.retry_count = 0;
        info!(
            "加载台词成功: {} 段, 推送模式: {:?}",
            total, push_mode
        );
    }

    pub async fn get_state(&self) -> BroadcastState {
        self.inner.read().await.state.clone()
    }

    pub async fn get_script(&self) -> Option<LiveScript> {
        self.inner.read().await.script.clone()
    }

    pub async fn get_last_push_message(&self) -> Option<SegmentPushMessage> {
        self.inner.read().await.last_push_message.clone()
    }

    pub fn get_push_notify(&self) -> Arc<Notify> {
        self.push_notify.clone()
    }

    pub async fn acknowledge(&self, req: AckRequest) -> AckResponse {
        let mut inner = self.inner.write().await;

        if inner.state.last_pushed_sequence == 0 {
            return AckResponse {
                success: false,
                next_available: false,
                next_segment_index: None,
                message: "没有正在进行的播报".to_string(),
            };
        }

        let expected_seq = inner.state.last_pushed_sequence;
        if req.sequence != expected_seq {
            warn!(
                "ACK 序列号不匹配: 期望 {}, 收到 {}",
                expected_seq, req.sequence
            );
            return AckResponse {
                success: false,
                next_available: false,
                next_segment_index: inner.state.current_segment_index,
                message: format!(
                    "序列号不匹配: 期望 {}, 收到 {}",
                    expected_seq, req.sequence
                ),
            };
        }

        if let Some(script) = &inner.script {
            if let Some(current_idx) = inner.state.current_segment_index {
                let expected_segment = &script.segments[current_idx];
                if req.segment_id != expected_segment.id {
                    warn!(
                        "ACK 段落ID不匹配: 期望 {}, 收到 {}",
                        expected_segment.id, req.segment_id
                    );
                    return AckResponse {
                        success: false,
                        next_available: false,
                        next_segment_index: Some(current_idx),
                        message: "段落ID不匹配".to_string(),
                    };
                }
            }
        }

        inner.state.segments_acked += 1;
        inner.state.last_acked_sequence = Some(req.sequence);
        inner.state.last_acked_at = Some(req.ack_timestamp);
        inner.state.pending_ack_count = inner.state.pending_ack_count.saturating_sub(1);
        inner.retry_count = 0;

        info!(
            "收到 ACK: seq={}, segment={}, 播放耗时={}ms, 已确认 {}/{}",
            req.sequence,
            req.segment_index + 1,
            req.play_duration_ms,
            inner.state.segments_acked,
            inner.state.segments_total
        );

        let has_next = inner
            .state
            .current_segment_index
            .map(|idx| idx + 1 < inner.state.segments_total)
            .unwrap_or(false);

        let next_idx = if has_next {
            inner.state.current_segment_index.map(|idx| idx + 1)
        } else {
            if inner.state.segments_acked >= inner.state.segments_total {
                inner.state.status = BroadcastStatus::Completed;
                info!("所有段落已确认播报完毕");
            }
            None
        };

        drop(inner);
        self.push_notify.notify_waiters();

        AckResponse {
            success: true,
            next_available: has_next,
            next_segment_index: next_idx,
            message: "确认成功".to_string(),
        }
    }

    pub async fn control(&self, action: ControlAction) -> Result<BroadcastState, String> {
        let result = match action {
            ControlAction::Start => self.start().await,
            ControlAction::Pause => self.pause().await,
            ControlAction::Resume => self.resume().await,
            ControlAction::Stop => self.stop().await,
            ControlAction::Next => self.next_segment().await,
            ControlAction::Prev => self.prev_segment().await,
        };

        if result.is_ok() {
            self.push_notify.notify_waiters();
        }

        result
    }

    async fn start(&self) -> Result<BroadcastState, String> {
        {
            let inner = self.inner.read().await;
            if inner.script.is_none() {
                return Err("未加载台词脚本".to_string());
            }
            if matches!(inner.state.status, BroadcastStatus::Running) {
                return Err("播报已在运行中".to_string());
            }
        }

        let broadcaster = self.clone();
        tokio::spawn(async move {
            broadcaster.run_broadcast().await;
        });

        Ok(self.get_state().await)
    }

    async fn pause(&self) -> Result<BroadcastState, String> {
        let mut inner = self.inner.write().await;
        if matches!(inner.state.status, BroadcastStatus::Running) {
            inner.state.status = BroadcastStatus::Paused;
            inner.should_pause = true;
            info!("播报已暂停");
        }
        Ok(inner.state.clone())
    }

    async fn resume(&self) -> Result<BroadcastState, String> {
        let mut inner = self.inner.write().await;
        if matches!(inner.state.status, BroadcastStatus::Paused) {
            inner.state.status = BroadcastStatus::Running;
            inner.should_pause = false;
            info!("播报已恢复");
        }
        Ok(inner.state.clone())
    }

    async fn stop(&self) -> Result<BroadcastState, String> {
        let mut inner = self.inner.write().await;
        inner.should_stop = true;
        inner.should_pause = false;
        inner.state.status = BroadcastStatus::Idle;
        inner.state.current_segment_index = None;
        inner.state.current_segment = None;
        inner.state.started_at = None;
        inner.state.next_push_at = None;
        inner.state.segments_broadcasted = 0;
        inner.state.segments_acked = 0;
        inner.state.last_pushed_sequence = 0;
        inner.state.last_acked_sequence = None;
        inner.state.pending_ack_count = 0;
        inner.last_push_message = None;
        inner.retry_count = 0;
        info!("播报已停止");
        Ok(inner.state.clone())
    }

    async fn next_segment(&self) -> Result<BroadcastState, String> {
        let mut inner = self.inner.write().await;
        let script = inner.script.clone().ok_or("未加载台词脚本")?;

        let next_idx = match inner.state.current_segment_index {
            Some(idx) if idx + 1 < script.segments.len() => idx + 1,
            None if !script.segments.is_empty() => 0,
            _ => return Err("已经是最后一段".to_string()),
        };

        let segment = script.segments[next_idx].clone();
        inner.state.current_segment_index = Some(next_idx);
        inner.state.current_segment = Some(segment.clone());
        inner.state.segments_broadcasted = next_idx + 1;
        inner.state.pending_ack_count = inner.state.pending_ack_count.saturating_add(1);

        info!("跳转到第 {} 段: {}", next_idx + 1, truncate(&segment.text, 50));
        Ok(inner.state.clone())
    }

    async fn prev_segment(&self) -> Result<BroadcastState, String> {
        let mut inner = self.inner.write().await;
        let script = inner.script.clone().ok_or("未加载台词脚本")?;

        let prev_idx = match inner.state.current_segment_index {
            Some(idx) if idx > 0 => idx - 1,
            _ => return Err("已经是第一段".to_string()),
        };

        let segment = script.segments[prev_idx].clone();
        inner.state.current_segment_index = Some(prev_idx);
        inner.state.current_segment = Some(segment.clone());
        inner.state.segments_broadcasted = prev_idx + 1;

        info!("跳转到第 {} 段: {}", prev_idx + 1, truncate(&segment.text, 50));
        Ok(inner.state.clone())
    }

    async fn run_broadcast(&self) {
        info!("开始执行播报任务");

        {
            let mut inner = self.inner.write().await;
            inner.state.status = BroadcastStatus::Running;
            inner.state.started_at = Some(Utc::now());
            inner.should_stop = false;
            inner.should_pause = false;
            inner.retry_count = 0;
        }

        loop {
            {
                let inner = self.inner.read().await;
                if inner.should_stop {
                    info!("收到停止信号，退出播报");
                    return;
                }
                if inner.should_pause {
                    drop(inner);
                    time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }

            let push_mode = self.inner.read().await.state.push_mode;

            match push_mode {
                PushMode::TimeDriven => {
                    self.run_time_driven().await;
                }
                PushMode::AckDriven => {
                    self.run_ack_driven().await;
                }
            }

            let should_complete = {
                let inner = self.inner.read().await;
                inner.state.segments_acked >= inner.state.segments_total
                    && inner.state.segments_total > 0
            };

            if should_complete {
                let mut inner = self.inner.write().await;
                inner.state.status = BroadcastStatus::Completed;
                inner.state.next_push_at = None;
                info!("所有段落播报确认完毕");
                return;
            }

            time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn run_time_driven(&self) {
        let (script, current_idx) = {
            let inner = self.inner.read().await;
            (inner.script.clone(), inner.state.current_segment_index)
        };

        let script = match script {
            Some(s) => s,
            None => {
                warn!("脚本不存在，退出播报");
                return;
            }
        };

        let next_idx = match current_idx {
            Some(idx) => idx + 1,
            None => 0,
        };

        if next_idx >= script.segments.len() {
            return;
        }

        let segment = script.segments[next_idx].clone();
        let duration = Duration::from_millis(segment.duration_ms);
        let next_push: DateTime<Utc> = Utc::now() + chrono::Duration::from_std(duration).unwrap();

        {
            let mut inner = self.inner.write().await;
            inner.state.last_pushed_sequence += 1;
            let seq = inner.state.last_pushed_sequence;

            let push_msg = SegmentPushMessage {
                sequence: seq,
                segment_index: next_idx,
                segment_id: segment.id,
                text: segment.text.clone(),
                duration_ms: segment.duration_ms,
                segments_total: script.segments.len(),
                push_timestamp: Utc::now(),
                is_retry: false,
            };

            inner.state.current_segment_index = Some(next_idx);
            inner.state.current_segment = Some(segment.clone());
            inner.state.segments_broadcasted = next_idx + 1;
            inner.state.next_push_at = Some(next_push);
            inner.state.last_pushed_at = Some(Utc::now());
            inner.state.pending_ack_count = inner.state.pending_ack_count.saturating_add(1);
            inner.last_push_message = Some(push_msg);
        }

        self.push_notify.notify_waiters();

        {
            let inner = self.inner.read().await;
            info!(
                "推送第 {}/{} 段 seq={} ({}ms): {}",
                next_idx + 1,
                script.segments.len(),
                inner.state.last_pushed_sequence,
                segment.duration_ms,
                truncate(&segment.text, 60)
            );
        }

        self.sleep_with_cancellation(duration).await;
    }

    async fn run_ack_driven(&self) {
        let (pending_ack, max_pending) = {
            let inner = self.inner.read().await;
            (inner.state.pending_ack_count, inner.max_pending_ack)
        };

        if pending_ack >= max_pending {
            if self.check_ack_timeout().await {
                return;
            }
            let notify = self.push_notify.notified();
            let _ = tokio::time::timeout(Duration::from_millis(500), notify).await;
            return;
        }

        let (script, current_idx, pending_ack, max_pending) = {
            let inner = self.inner.read().await;
            (
                inner.script.clone(),
                inner.state.current_segment_index,
                inner.state.pending_ack_count,
                inner.max_pending_ack,
            )
        };

        if pending_ack >= max_pending {
            return;
        }

        let script = match script {
            Some(s) => s,
            None => {
                warn!("脚本不存在，退出播报");
                return;
            }
        };

        let next_idx = match current_idx {
            Some(idx) => idx + 1,
            None => 0,
        };

        if next_idx >= script.segments.len() {
            return;
        }

        let segment = script.segments[next_idx].clone();

        {
            let mut inner = self.inner.write().await;
            inner.state.last_pushed_sequence += 1;
            let seq = inner.state.last_pushed_sequence;

            let push_msg = SegmentPushMessage {
                sequence: seq,
                segment_index: next_idx,
                segment_id: segment.id,
                text: segment.text.clone(),
                duration_ms: segment.duration_ms,
                segments_total: script.segments.len(),
                push_timestamp: Utc::now(),
                is_retry: false,
            };

            inner.state.current_segment_index = Some(next_idx);
            inner.state.current_segment = Some(segment.clone());
            inner.state.segments_broadcasted = next_idx + 1;
            inner.state.last_pushed_at = Some(Utc::now());
            inner.state.pending_ack_count = inner.state.pending_ack_count.saturating_add(1);
            inner.last_push_message = Some(push_msg);
        }

        self.push_notify.notify_waiters();

        {
            let inner = self.inner.read().await;
            info!(
                "ACK模式推送第 {}/{} 段 seq={} ({}ms): {}",
                next_idx + 1,
                script.segments.len(),
                inner.state.last_pushed_sequence,
                segment.duration_ms,
                truncate(&segment.text, 60)
            );
        }
    }

    async fn check_ack_timeout(&self) -> bool {
        let mut inner = self.inner.write().await;

        let last_pushed_at = match inner.state.last_pushed_at {
            Some(t) => t,
            None => return false,
        };

        let elapsed = (Utc::now() - last_pushed_at).num_milliseconds() as u64;
        if elapsed < inner.ack_timeout_ms {
            return false;
        }

        if inner.retry_count >= inner.max_retries {
            warn!(
                "ACK 超时已达最大重试次数 {}，跳过当前段",
                inner.max_retries
            );
            inner.state.pending_ack_count = inner.state.pending_ack_count.saturating_sub(1);
            inner.state.segments_acked += 1;
            inner.retry_count = 0;
            return true;
        }

        inner.retry_count += 1;
        inner.state.last_pushed_sequence += 1;

        if let Some(last_msg) = inner.last_push_message.clone() {
            let new_msg = SegmentPushMessage {
                sequence: inner.state.last_pushed_sequence,
                segment_index: last_msg.segment_index,
                segment_id: last_msg.segment_id,
                text: last_msg.text,
                duration_ms: last_msg.duration_ms,
                segments_total: last_msg.segments_total,
                push_timestamp: Utc::now(),
                is_retry: true,
            };
            inner.last_push_message = Some(new_msg);
            inner.state.last_pushed_at = Some(Utc::now());

            warn!(
                "ACK 超时 ({}ms)，第 {} 次重传 seq={}",
                elapsed, inner.retry_count, inner.state.last_pushed_sequence
            );
            drop(inner);
            self.push_notify.notify_waiters();
            return true;
        }

        false
    }

    pub async fn create_state_message(&self) -> StatePushMessage {
        let mut inner = self.inner.write().await;
        inner.state.last_pushed_sequence += 1;
        StatePushMessage {
            sequence: inner.state.last_pushed_sequence,
            status: inner.state.status.clone(),
            segment_index: inner.state.current_segment_index,
            segments_total: inner.state.segments_total,
            segments_acked: inner.state.segments_acked,
            segments_broadcasted: inner.state.segments_broadcasted,
            push_timestamp: Utc::now(),
        }
    }

    async fn sleep_with_cancellation(&self, duration: Duration) {
        let start = tokio::time::Instant::now();
        let total = duration.as_millis() as u64;
        let check_interval = 50u64;

        loop {
            let elapsed = start.elapsed().as_millis() as u64;
            if elapsed >= total {
                break;
            }

            {
                let inner = self.inner.read().await;
                if inner.should_stop {
                    return;
                }
            }

            let remaining = total.saturating_sub(elapsed);
            let sleep_for = remaining.min(check_interval);
            time::sleep(Duration::from_millis(sleep_for)).await;
        }
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

impl Default for Broadcaster {
    fn default() -> Self {
        Self::new()
    }
}
