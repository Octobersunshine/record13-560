use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{self, Duration};
use chrono::{DateTime, Utc};
use tracing::{info, warn};

use crate::models::{
    BroadcastState, BroadcastStatus, ControlAction, LiveScript,
};

#[derive(Clone)]
pub struct Broadcaster {
    inner: Arc<RwLock<BroadcasterInner>>,
}

struct BroadcasterInner {
    script: Option<LiveScript>,
    state: BroadcastState,
    should_stop: bool,
    should_pause: bool,
}

impl Broadcaster {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(BroadcasterInner {
                script: None,
                state: BroadcastState::default(),
                should_stop: false,
                should_pause: false,
            })),
        }
    }

    pub async fn load_script(&self, script: LiveScript) {
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
            segments_total: total,
        };
        inner.should_stop = false;
        inner.should_pause = false;
    }

    pub async fn get_state(&self) -> BroadcastState {
        self.inner.read().await.state.clone()
    }

    pub async fn get_script(&self) -> Option<LiveScript> {
        self.inner.read().await.script.clone()
    }

    pub async fn control(&self, action: ControlAction) -> Result<BroadcastState, String> {
        match action {
            ControlAction::Start => self.start().await,
            ControlAction::Pause => self.pause().await,
            ControlAction::Resume => self.resume().await,
            ControlAction::Stop => self.stop().await,
            ControlAction::Next => self.next_segment().await,
            ControlAction::Prev => self.prev_segment().await,
        }
    }

    async fn start(&self) -> Result<BroadcastState, String> {
        {
            let inner = self.inner.read().await;
            if inner.script.is_none() {
                return Err("未加载台词脚本".to_string());
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
                let mut inner = self.inner.write().await;
                inner.state.status = BroadcastStatus::Completed;
                inner.state.next_push_at = None;
                info!("所有段落播报完毕");
                return;
            }

            let segment = script.segments[next_idx].clone();
            let duration = Duration::from_millis(segment.duration_ms);
            let next_push: DateTime<Utc> = Utc::now() + chrono::Duration::from_std(duration).unwrap();

            {
                let mut inner = self.inner.write().await;
                inner.state.current_segment_index = Some(next_idx);
                inner.state.current_segment = Some(segment.clone());
                inner.state.segments_broadcasted = next_idx + 1;
                inner.state.next_push_at = Some(next_push);
            }

            info!(
                "推送第 {}/{} 段 ({}ms): {}",
                next_idx + 1,
                script.segments.len(),
                segment.duration_ms,
                truncate(&segment.text, 60)
            );

            self.sleep_with_cancellation(duration).await;
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
