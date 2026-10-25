//! 验证 Codex 原生状态 metadata 的官方字段解析、优先级与未知取值处理

use gateway_protocol::openai::{
    CodexHistoryInitialization, CodexNativeStateMetadata, CodexSessionIdentity,
    CodexSessionSources, codex_native_state_metadata,
};
use serde_json::{Map, Value, json};

fn metadata(body: &Map<String, Value>, context: &Map<String, Value>) -> CodexNativeStateMetadata {
    codex_native_state_metadata(CodexSessionSources {
        body,
        context,
        turn_metadata: None,
        session_id: None,
        endpoint_session: None,
    })
}

fn metadata_with_turn_metadata<'a>(
    body: &'a Map<String, Value>,
    context: &'a Map<String, Value>,
    turn_metadata: &'a str,
) -> CodexNativeStateMetadata {
    codex_native_state_metadata(CodexSessionSources {
        body,
        context,
        turn_metadata: Some(turn_metadata),
        session_id: None,
        endpoint_session: None,
    })
}

fn object(value: Value) -> Map<String, Value> {
    value.as_object().cloned().unwrap_or_default()
}

#[test]
fn state_metadata_should_read_official_turn_metadata_fields() {
    let body = object(json!({
        "session_id": "session-flat",
        "turn_metadata": json!({
            "session_id": "session-turn",
            "thread_id": "thread-turn",
            "agent_name": "/root/child",
            "window_id": "thread-turn:3",
            "context_window_id": "0193F0B7-1D2A-7C31-9B0E-2A3F5C6D7E80",
            "history_initialization": "cold_resume",
            "request_kind": "turn",
        })
        .to_string(),
        "previous_response_id": "resp_parent",
    }));

    let state = metadata(&body, &Map::new());

    // 逻辑声明只出现在 turn metadata，正文 session_id 仅作为传输兼底候选
    assert_eq!(
        state.session,
        CodexSessionIdentity::Resolved("session-turn".to_owned())
    );
    assert_eq!(state.thread_id.as_deref(), Some("thread-turn"));
    assert_eq!(state.agent_name.as_deref(), Some("/root/child"));
    assert_eq!(state.window_id.as_deref(), Some("thread-turn:3"));
    // UUID 只规范大小写与连字符形式，不改变取值
    assert_eq!(
        state.context_window_id.as_deref(),
        Some("0193f0b7-1d2a-7c31-9b0e-2a3f5c6d7e80")
    );
    assert_eq!(
        state.history_initialization,
        Some(CodexHistoryInitialization::ColdResume)
    );
    assert_eq!(state.request_kind.as_deref(), Some("turn"));
    assert_eq!(state.previous_response_id.as_deref(), Some("resp_parent"));
}

#[test]
fn state_metadata_should_prefer_body_turn_metadata_over_context() {
    let body = object(
        json!({"turn_metadata": json!({"thread_id": "thread-body", "window_id": "thread-body:2"}).to_string()}),
    );
    let context = object(
        json!({"turn_metadata": json!({"thread_id": "thread-context", "window_id": "thread-context:9"}).to_string()}),
    );

    let state = metadata(&body, &context);

    assert_eq!(state.thread_id.as_deref(), Some("thread-body"));
    assert_eq!(state.window_id.as_deref(), Some("thread-body:2"));
}

#[test]
fn state_metadata_should_prefer_turn_metadata_header_and_skip_unparsable_sources() {
    let body = object(
        json!({"turn_metadata": json!({"thread_id": "thread-body", "window_id": "thread-body:2"}).to_string()}),
    );
    let header = json!({"thread_id": "thread-header", "window_id": "thread-header:7"}).to_string();

    let state = metadata_with_turn_metadata(&body, &Map::new(), &header);
    assert_eq!(state.thread_id.as_deref(), Some("thread-header"));
    assert_eq!(state.window_id.as_deref(), Some("thread-header:7"));

    // 头投影不是可解析对象时回退到正文，不静默丢失事实
    let state = metadata_with_turn_metadata(&body, &Map::new(), "not-json");
    assert_eq!(state.thread_id.as_deref(), Some("thread-body"));
    assert_eq!(state.window_id.as_deref(), Some("thread-body:2"));
}

#[test]
fn state_metadata_should_fall_back_to_client_metadata_and_header_projection() {
    let body = object(
        json!({"client_metadata": {"x-codex-window-id": "thread-meta:4", "thread_id": "thread-meta"}}),
    );
    let state = metadata(&body, &Map::new());
    assert_eq!(state.window_id.as_deref(), Some("thread-meta:4"));
    assert_eq!(state.thread_id.as_deref(), Some("thread-meta"));

    let context = object(json!({"codex_window_id": "thread-header:5"}));
    let state = metadata(&Map::new(), &context);
    assert_eq!(state.window_id.as_deref(), Some("thread-header:5"));
}

#[test]
fn state_metadata_should_not_guess_unknown_initialization_or_invalid_context_window() {
    let body = object(json!({
        "turn_metadata": json!({
            "history_initialization": "Unknown_Value",
            "context_window_id": "not-a-uuid",
        })
        .to_string(),
    }));

    let state = metadata(&body, &Map::new());

    assert_eq!(state.history_initialization, None);
    assert_eq!(state.context_window_id, None);
    assert_eq!(state.request_kind, None);
    assert_eq!(state.window_id, None);
    assert_eq!(state.previous_response_id, None);
}

#[test]
fn state_metadata_should_keep_opaque_previous_response_id_and_reject_empty_values() {
    for (value, expected) in [
        (json!("  resp_padded  "), Some("  resp_padded  ")),
        (json!(""), None),
        (json!(7), None),
        (json!(null), None),
    ] {
        let body = object(json!({"previous_response_id": value}));
        let state = metadata(&body, &Map::new());
        assert_eq!(state.previous_response_id.as_deref(), expected);
    }
}

#[test]
fn state_metadata_should_reuse_session_conflict_resolution() {
    let body = object(json!({"client_metadata": {"session_id": "session-a"}}));
    let context = object(json!({"turn_metadata": json!({"session_id": "session-b"}).to_string()}));

    let state = metadata(&body, &context);

    assert_eq!(state.session, CodexSessionIdentity::Conflict);
}

#[test]
fn state_metadata_should_report_missing_session_without_guessing() {
    let state = metadata(&Map::new(), &Map::new());

    assert_eq!(state.session, CodexSessionIdentity::Missing);
    assert_eq!(state.history_initialization, None);
}
