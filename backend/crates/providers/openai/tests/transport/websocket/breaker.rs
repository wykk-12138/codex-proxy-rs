//! 验证 WebSocket 熔断器的并发建连结果与跨轮次隔离

use std::time::Duration;

use provider_openai::transport::websocket::{
    WebSocketOriginBreaker, WebSocketOriginBreakerConfig, WebSocketOriginBreakerDecision,
    WebSocketOriginBreakerPermit,
};

const ORIGIN: &str = "https://example.test:443";

fn acquire(breaker: &WebSocketOriginBreaker) -> WebSocketOriginBreakerPermit {
    let WebSocketOriginBreakerDecision::Allowed(permit) = breaker.try_acquire(ORIGIN) else {
        panic!("expected an opening permit");
    };
    permit
}

#[test]
fn closed_success_should_keep_counting_concurrent_failures() {
    let config = WebSocketOriginBreakerConfig::default();
    let breaker = WebSocketOriginBreaker::with_config(config);
    let success = acquire(&breaker);
    let pending_failures = (0..config.failure_threshold)
        .map(|_| acquire(&breaker))
        .collect::<Vec<_>>();

    success.succeed();
    for permit in pending_failures {
        permit.fail();
    }

    assert!(matches!(
        breaker.try_acquire(ORIGIN),
        WebSocketOriginBreakerDecision::Open
    ));
}

#[test]
fn concurrent_closed_success_should_clear_previous_failures() {
    let config = WebSocketOriginBreakerConfig::default();
    let breaker = WebSocketOriginBreaker::with_config(config);
    let first_success = acquire(&breaker);
    let pending_success = acquire(&breaker);

    first_success.succeed();
    for _ in 0..config.failure_threshold - 1 {
        acquire(&breaker).fail();
    }
    pending_success.succeed();
    for _ in 0..config.failure_threshold - 1 {
        acquire(&breaker).fail();
    }

    acquire(&breaker).succeed();
}

#[tokio::test(start_paused = true)]
async fn stale_closed_permits_should_not_change_recovered_failure_history() {
    for outcome in ["success", "failure", "drop"] {
        let breaker = WebSocketOriginBreaker::with_config(WebSocketOriginBreakerConfig {
            failure_threshold: 2,
            failure_window: Duration::from_secs(30),
            open_duration: Duration::from_secs(30),
        });
        let stale = acquire(&breaker);
        acquire(&breaker).fail();
        acquire(&breaker).fail();
        assert!(matches!(
            breaker.try_acquire(ORIGIN),
            WebSocketOriginBreakerDecision::Open
        ));

        tokio::time::advance(Duration::from_secs(30)).await;
        acquire(&breaker).succeed();
        acquire(&breaker).fail();
        match outcome {
            "success" => stale.succeed(),
            "failure" => stale.fail(),
            _ => drop(stale),
        }

        // 新代次的一次失败不能被旧成功清除，也不能被旧失败或 Drop 凑满阈值
        acquire(&breaker).fail();
        assert!(matches!(
            breaker.try_acquire(ORIGIN),
            WebSocketOriginBreakerDecision::Open
        ));
    }
}

#[tokio::test(start_paused = true)]
async fn stale_closed_permits_should_not_finish_a_half_open_probe() {
    for outcome in ["success", "failure", "drop"] {
        let breaker = WebSocketOriginBreaker::with_config(WebSocketOriginBreakerConfig {
            failure_threshold: 1,
            failure_window: Duration::from_secs(30),
            open_duration: Duration::from_secs(30),
        });
        let stale = acquire(&breaker);
        acquire(&breaker).fail();
        tokio::time::advance(Duration::from_secs(30)).await;
        let probe = acquire(&breaker);
        match outcome {
            "success" => stale.succeed(),
            "failure" => stale.fail(),
            _ => drop(stale),
        }
        assert!(matches!(
            breaker.try_acquire(ORIGIN),
            WebSocketOriginBreakerDecision::HalfOpenBusy
        ));
        probe.succeed();
        acquire(&breaker).succeed();
    }
}
