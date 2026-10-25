//! 验证 Codex 原生状态值对象的窗口身份、notes 域代次、操作状态与脱敏边界

use gateway_core::account::ProviderAccountId;
use gateway_core::policy::ClientApiKeyId;
use gateway_core::provider_ports::{
    CodexHistoryItem, CodexHistoryRole, CodexNativeWindowId, CodexNotesCompletion,
    CodexNotesGeneration, CodexNotesOperation, CodexNotesOperationKind, CodexNotesOperationStatus,
    CodexNotesReplay, CodexNotesReplica, CodexSessionOrigin, CodexSessionState,
    NewCodexNotesOperation, ProviderSessionAffinityKey, ProviderStoreErrorKind,
};

#[test]
fn native_window_id_should_parse_thread_and_window_number() {
    let window = CodexNativeWindowId::try_new("0193f0b7-root:3").expect("valid window");
    assert_eq!(window.expose_to_store(), "0193f0b7-root:3");
    assert_eq!(window.thread_id(), "0193f0b7-root");
    assert_eq!(window.window_number(), 3);

    // 线程 ID 内的冒号按最后一个冒号切分，编号只取尾部
    let nested = CodexNativeWindowId::try_new("nested:thread:12").expect("valid window");
    assert_eq!(nested.thread_id(), "nested:thread");
    assert_eq!(nested.window_number(), 12);
}

#[test]
fn native_window_id_should_accept_the_first_official_window_zero() {
    // 官方 auto compact 窗口从 0 开始，首窗口必须可持久化与查询
    let first = CodexNativeWindowId::try_new("0193f0b7-root:0").expect("first window");
    assert_eq!(first.expose_to_store(), "0193f0b7-root:0");
    assert_eq!(first.thread_id(), "0193f0b7-root");
    assert_eq!(first.window_number(), 0);
}

#[test]
fn native_window_id_should_reject_shapes_outside_the_official_contract() {
    for value in [
        "",
        ":1",
        "thread:",
        "thread:-1",
        "thread:1.0",
        "thread: 1",
        "  :1",
        "thread-only",
    ] {
        let error = CodexNativeWindowId::try_new(value).expect_err("invalid window id");
        assert_eq!(error.kind(), ProviderStoreErrorKind::InvalidData, "{value}");
    }
}

#[test]
fn notes_generation_should_require_canonical_uuid_text() {
    let generated = CodexNotesGeneration::generate();
    let parsed = CodexNotesGeneration::try_new(generated.expose_to_store()).expect("valid");
    assert_eq!(parsed, generated);
    assert_ne!(CodexNotesGeneration::generate(), generated);

    for value in [
        "not-a-uuid",
        "0193F0B7-1D2A-7C31-9B0E-2A3F5C6D7E80",
        "{0193f0b7-1d2a-7c31-9b0e-2a3f5c6d7e80}",
        "0193f0b71d2a7c319b0e2a3f5c6d7e80",
    ] {
        let error = CodexNotesGeneration::try_new(value).expect_err("invalid generation");
        assert_eq!(error.kind(), ProviderStoreErrorKind::InvalidData, "{value}");
    }
}

#[test]
fn notes_replica_should_distinguish_native_and_rebuilt_domains() {
    let account = ProviderAccountId::new("acct_primary").expect("valid account");
    let native = CodexNotesReplica::native(account.clone());
    assert_eq!(native.account(), &account);
    assert_eq!(native.generation(), None);

    let generation = CodexNotesGeneration::generate();
    let rebuilt = CodexNotesReplica::rebuilt(account.clone(), generation.clone());
    assert_eq!(rebuilt.account(), &account);
    assert_eq!(rebuilt.generation(), Some(&generation));
    assert_ne!(native, rebuilt);
}

#[test]
fn notes_replay_should_reject_native_target_and_advancing_past_frozen_position() {
    let account = ProviderAccountId::new("acct_primary").expect("valid account");
    let native = CodexNotesReplica::native(account.clone());
    let error = CodexNotesReplay::try_new(native, 3, 0).expect_err("native target");
    assert_eq!(error.kind(), ProviderStoreErrorKind::InvalidData);

    let rebuilt = CodexNotesReplica::rebuilt(account, CodexNotesGeneration::generate());
    let replay = CodexNotesReplay::try_new(rebuilt.clone(), 3, 3).expect("valid replay");
    assert_eq!(replay.frozen_through_seq, 3);
    assert_eq!(replay.replayed_through_seq, 3);

    let error = CodexNotesReplay::try_new(rebuilt, 3, 4).expect_err("progress past target");
    assert_eq!(error.kind(), ProviderStoreErrorKind::InvalidData);
}

#[test]
fn notes_operation_status_should_only_replay_complete_responses() {
    for (status, terminal, replay) in [
        (CodexNotesOperationStatus::Unsent, false, false),
        (CodexNotesOperationStatus::InFlight, false, false),
        (CodexNotesOperationStatus::Responded, true, true),
        (CodexNotesOperationStatus::Failed, true, false),
        (CodexNotesOperationStatus::Unknown, false, false),
    ] {
        assert_eq!(status.is_terminal(), terminal, "{status:?}");
        assert_eq!(status.needs_replay(), replay, "{status:?}");
    }
}

#[test]
fn notes_operation_debug_should_not_leak_encrypted_payloads() {
    let operation = notes_operation(
        "opaque-request-ciphertext",
        Some("opaque-response-ciphertext"),
    );
    let rendered = format!("{operation:?}");

    assert!(!rendered.contains("opaque-request-ciphertext"));
    assert!(!rendered.contains("opaque-response-ciphertext"));
    assert!(rendered.contains("seq: 7"));

    let completion = CodexNotesCompletion {
        key_id: ClientApiKeyId::new("key_fixture").expect("valid key"),
        session_key: ProviderSessionAffinityKey::try_new("session-fixture").expect("valid key"),
        seq: 7,
        status: CodexNotesOperationStatus::Responded,
        response: Some("{cipher}".to_owned()),
        response_status: Some(200),
    };
    assert!(!format!("{completion:?}").contains("{cipher}"));

    let new_operation = NewCodexNotesOperation {
        key_id: ClientApiKeyId::new("key_fixture").expect("valid key"),
        session_key: ProviderSessionAffinityKey::try_new("session-fixture").expect("valid key"),
        replica: CodexNotesReplica::native(
            ProviderAccountId::new("acct_primary").expect("valid account"),
        ),
        agent_name: "/root".to_owned(),
        kind: CodexNotesOperationKind::AppendToFile,
        request: "new-op-request-ciphertext".to_owned(),
    };
    assert!(!format!("{new_operation:?}").contains("new-op-request-ciphertext"));
}

#[test]
fn history_item_debug_should_not_leak_raw_object_or_projection() {
    let item = CodexHistoryItem {
        item_id: "item_1".to_owned(),
        role: Some(CodexHistoryRole::Assistant),
        tool_namespace: None,
        tool_name: None,
        content_type: "reasoning".to_owned(),
        raw: "opaque-raw-object".to_owned(),
        text: Some("readable-projection".to_owned()),
        identity: "identity-item-1".to_owned(),
    };
    let rendered = format!("{item:?}");

    assert!(!rendered.contains("opaque-raw-object"));
    assert!(!rendered.contains("readable-projection"));
    assert!(rendered.contains("item_1"));
}

#[test]
fn session_state_should_keep_origin_and_published_replica_separate_from_replay_target() {
    let account = ProviderAccountId::new("acct_primary").expect("valid account");
    let native = CodexNotesReplica::native(account.clone());
    let state = CodexSessionState {
        origin: CodexSessionOrigin::Unclassified,
        owner: account.clone(),
        generation: 4,
        notes: native.clone(),
        notes_replay: None,
    };
    assert_eq!(state.owner, account);
    assert_eq!(state.notes, native);
    assert!(state.notes_replay.is_none());
    assert_ne!(
        CodexSessionOrigin::Unclassified,
        CodexSessionOrigin::Managed
    );
}

fn notes_operation(request: &str, response: Option<&str>) -> CodexNotesOperation {
    CodexNotesOperation {
        seq: 7,
        replica: CodexNotesReplica::native(
            ProviderAccountId::new("acct_primary").expect("valid account"),
        ),
        agent_name: "/root".to_owned(),
        kind: CodexNotesOperationKind::WriteFile,
        status: CodexNotesOperationStatus::Responded,
        request: request.to_owned(),
        response: response.map(ToOwned::to_owned),
        response_status: Some(200),
    }
}
