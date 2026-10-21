//! 验证各传输共享建连恢复预算，且不限制成功建连后的执行

use gateway_core::engine::connection::ConnectionBudget;

#[test]
fn elapsed_recovery_window_is_shared_without_waiting_for_real_time() {
    use std::{
        cell::Cell,
        time::{Duration, Instant},
    };
    thread_local! {
        static NOW: Cell<Instant> = Cell::new(Instant::now());
    }
    fn now() -> Instant {
        NOW.get()
    }
    let request = ConnectionBudget::with_clock(now);
    let fallback = request.clone();
    assert_eq!(request.begin().unwrap(), Some(Duration::from_secs(30)));
    NOW.set(now() + Duration::from_secs(29));
    assert_eq!(fallback.begin().unwrap(), Some(Duration::from_secs(1)));
    assert_eq!(request.startup_remaining(), Some(Duration::from_secs(1)));
    NOW.set(now() + Duration::from_secs(1));
    assert!(request.exhausted());
    assert!(fallback.begin().is_err());
    assert!(request.remaining().is_none());
    assert_eq!(fallback.startup_remaining(), Some(Duration::ZERO));
}

#[test]
fn same_account_retry_preserves_time_and_an_opening_for_rotation() {
    use std::{
        cell::Cell,
        time::{Duration, Instant},
    };
    thread_local! {
        static NOW: Cell<Instant> = Cell::new(Instant::now());
    }
    fn now() -> Instant {
        NOW.get()
    }
    let request = ConnectionBudget::with_clock(now);
    // 后台 WS 先占一次，HTTP 首次建连再占一次，模拟代理持续不响应
    let background = request.clone();
    assert_eq!(background.begin().unwrap(), Some(Duration::from_secs(30)));
    NOW.set(now() + Duration::from_secs(1));
    assert_eq!(request.begin().unwrap(), Some(Duration::from_secs(29)));
    NOW.set(now() + Duration::from_secs(15));
    assert_eq!(request.remaining_openings(), 2);
    let retry = request.clone().reserve_next_opening();
    assert_eq!(retry.begin().unwrap(), Some(Duration::from_secs(7)));
    assert_eq!(request.remaining_openings(), 1);
    assert!(retry.begin().is_err());
    assert!(!request.exhausted());
    NOW.set(now() + Duration::from_secs(7));
    // 重选使用原预算，既不继承 A 的局部时间限制，也不能重置总次数
    assert_eq!(request.begin().unwrap(), Some(Duration::from_secs(7)));
    assert!(request.exhausted());
    assert!(background.begin().is_err());
    request.complete();
    assert_eq!(retry.begin().unwrap(), None);
}

#[test]
fn all_transports_share_one_opening_budget_but_other_requests_do_not() {
    let request = ConnectionBudget::default();
    let http = request.clone();
    let websocket = request.clone();
    for transport in [&http, &websocket, &http, &websocket] {
        assert!(transport.begin().unwrap().is_some());
    }
    assert!(request.exhausted());
    assert!(http.begin().is_err());
    assert!(websocket.retry_delay(0, "req_shared").is_none());
    assert!(ConnectionBudget::default().begin().is_ok());
}

#[test]
fn successful_opening_does_not_limit_later_generation_or_protocol_recovery() {
    let request = ConnectionBudget::default();
    request.begin().unwrap();
    request.complete();
    assert!(request.startup_remaining().is_none());
    assert!(request.retry_delay(0, "req_sent").is_none());
    assert!(!request.exhausted());
    assert!(request.begin().unwrap().is_none());
}
