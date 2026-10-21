//! Responses WebSocket 冷建连的 origin 级熔断器

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::time::Instant;
use uuid::Uuid;

const DEFAULT_FAILURE_THRESHOLD: usize = 3;
const DEFAULT_FAILURE_WINDOW: Duration = Duration::from_secs(30);
const DEFAULT_OPEN_DURATION: Duration = Duration::from_secs(30);

/// origin 熔断策略
#[derive(Debug, Clone, Copy)]
pub struct WebSocketOriginBreakerConfig {
    pub failure_threshold: usize,
    pub failure_window: Duration,
    pub open_duration: Duration,
}

impl Default for WebSocketOriginBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: DEFAULT_FAILURE_THRESHOLD,
            failure_window: DEFAULT_FAILURE_WINDOW,
            open_duration: DEFAULT_OPEN_DURATION,
        }
    }
}

/// 冷建连是否获准进入 origin
pub enum WebSocketOriginBreakerDecision {
    Allowed(WebSocketOriginBreakerPermit),
    Open,
    HalfOpenBusy,
}

/// origin 级 WebSocket 建连熔断器
#[derive(Clone)]
pub struct WebSocketOriginBreaker {
    inner: Arc<Mutex<HashMap<String, CircuitState>>>,
    config: WebSocketOriginBreakerConfig,
}

impl Default for WebSocketOriginBreaker {
    fn default() -> Self {
        Self::with_config(WebSocketOriginBreakerConfig::default())
    }
}

impl WebSocketOriginBreaker {
    pub fn with_config(config: WebSocketOriginBreakerConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            config: WebSocketOriginBreakerConfig {
                failure_threshold: config.failure_threshold.max(1),
                ..config
            },
        }
    }

    /// 已有热 socket 不经过这里；本方法只裁决新的 WebSocket opening
    pub fn try_acquire(&self, origin_key: &str) -> WebSocketOriginBreakerDecision {
        let now = Instant::now();
        let mut circuits = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = circuits
            .entry(origin_key.to_string())
            .or_insert_with(CircuitState::closed);

        match state {
            CircuitState::Closed {
                generation,
                failures,
            } => {
                retain_window(failures, now, self.config.failure_window);
                WebSocketOriginBreakerDecision::Allowed(WebSocketOriginBreakerPermit::new(
                    self.clone(),
                    origin_key.to_string(),
                    *generation,
                ))
            }
            CircuitState::Open { until } if now < *until => WebSocketOriginBreakerDecision::Open,
            CircuitState::Open { .. } => {
                let generation = Uuid::new_v4();
                *state = CircuitState::HalfOpen { generation };
                WebSocketOriginBreakerDecision::Allowed(WebSocketOriginBreakerPermit::new(
                    self.clone(),
                    origin_key.to_string(),
                    generation,
                ))
            }
            CircuitState::HalfOpen { .. } => WebSocketOriginBreakerDecision::HalfOpenBusy,
        }
    }

    fn record_success(&self, origin_key: &str, generation: Uuid) {
        let mut circuits = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(state) = circuits.get_mut(origin_key) else {
            return;
        };
        if !permit_still_owns_state(Some(state), generation) {
            return;
        }
        match state {
            CircuitState::Closed { failures, .. } => {
                // 保留同轮尚未完成的 opening，让其后续结果继续参与计数
                failures.clear();
            }
            CircuitState::HalfOpen { .. } => *state = CircuitState::closed(),
            CircuitState::Open { .. } => {}
        }
    }

    fn record_failure(&self, origin_key: &str, generation: Uuid) {
        let now = Instant::now();
        let mut circuits = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(state) = circuits.get_mut(origin_key) else {
            return;
        };
        if !permit_still_owns_state(Some(state), generation) {
            return;
        }
        match state {
            CircuitState::HalfOpen { .. } => {
                *state = CircuitState::Open {
                    until: now + self.config.open_duration,
                };
            }
            CircuitState::Closed { failures, .. } => {
                retain_window(failures, now, self.config.failure_window);
                failures.push_back(now);
                if failures.len() >= self.config.failure_threshold {
                    *state = CircuitState::Open {
                        until: now + self.config.open_duration,
                    };
                }
            }
            CircuitState::Open { .. } => {}
        }
    }

    fn cancel_probe(&self, origin_key: &str, generation: Uuid) {
        let mut circuits = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(circuits.get(origin_key), Some(CircuitState::HalfOpen { generation: active }) if *active == generation)
        {
            circuits.insert(
                origin_key.to_string(),
                CircuitState::Open {
                    until: Instant::now(),
                },
            );
        }
    }
}

/// 一次新的 WebSocket opening 许可；消费式完成保证 half-open 探针不会泄漏
pub struct WebSocketOriginBreakerPermit {
    breaker: WebSocketOriginBreaker,
    origin_key: String,
    generation: Uuid,
    armed: bool,
}

impl WebSocketOriginBreakerPermit {
    fn new(breaker: WebSocketOriginBreaker, origin_key: String, generation: Uuid) -> Self {
        Self {
            breaker,
            origin_key,
            generation,
            armed: true,
        }
    }

    pub fn succeed(mut self) {
        self.breaker
            .record_success(&self.origin_key, self.generation);
        self.armed = false;
    }

    /// 非池化 opening 已被前台时限终止，按连接失败计数
    pub fn fast_timeout(self) {
        self.fail();
    }

    pub fn fail(mut self) {
        self.breaker
            .record_failure(&self.origin_key, self.generation);
        self.armed = false;
    }

    /// opening 因账号驱逐或服务关闭而取消，不污染 origin 可用性
    pub(crate) fn cancel(mut self) {
        self.breaker.cancel_probe(&self.origin_key, self.generation);
        self.armed = false;
    }
}

impl Drop for WebSocketOriginBreakerPermit {
    fn drop(&mut self) {
        if self.armed {
            self.breaker
                .record_failure(&self.origin_key, self.generation);
        }
    }
}

enum CircuitState {
    Closed {
        generation: Uuid,
        failures: VecDeque<Instant>,
    },
    Open {
        until: Instant,
    },
    HalfOpen {
        generation: Uuid,
    },
}

impl CircuitState {
    fn closed() -> Self {
        Self::Closed {
            // 初始化或半开恢复时创建新代次，隔离之前熔断周期的迟到结果
            generation: Uuid::new_v4(),
            failures: VecDeque::new(),
        }
    }
}

fn retain_window(timeouts: &mut VecDeque<Instant>, now: Instant, window: Duration) {
    while timeouts
        .front()
        .is_some_and(|recorded_at| now.duration_since(*recorded_at) > window)
    {
        timeouts.pop_front();
    }
}

fn permit_still_owns_state(state: Option<&CircuitState>, generation: Uuid) -> bool {
    match state {
        Some(
            CircuitState::Closed {
                generation: active, ..
            }
            | CircuitState::HalfOpen { generation: active },
        ) => *active == generation,
        _ => false,
    }
}
