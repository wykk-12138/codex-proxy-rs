//! 验证本地 history 四工具的窗口/条目过滤、排序、Unicode 字符范围、检索与预算

use futures::{executor::block_on, future::BoxFuture};
use gateway_core::account::ProviderAccountId;
use gateway_core::engine::codex_history::{
    CodexHistoryFilterQuery, CodexHistoryListItemsQuery, CodexHistoryQueryBudget,
    CodexHistoryQueryError, CodexHistoryReadItemOutcome, CodexHistoryReadQuery, CodexHistoryRoute,
    CodexHistoryScope, CodexHistorySearchQuery, CodexHistoryService, CodexHistoryToolOutcome,
    CodexHistoryToolQuery, CodexHistoryToolResult, CodexHistoryWindowQuery, codex_history_item_id,
    resolve_agent_name,
};
use gateway_core::policy::ClientApiKeyId;
use gateway_core::provider_ports::{
    CodexHistoryItem, CodexHistoryPendingInput, CodexHistoryRequest, CodexHistoryResponse,
    CodexHistoryResponseAnchor, CodexHistoryRole, CodexHistoryStoredItem, CodexHistoryStoredWindow,
    CodexHistoryWindow, CodexNativeWindowId, CodexNotesCompletion, CodexNotesOperation,
    CodexNotesPublish, CodexNotesPublishOutcome, CodexNotesReplay, CodexNotesReplica,
    CodexSessionContentPort, CodexSessionOrigin, CodexSessionState, CodexSessionStatePort,
    NewCodexNotesOperation, ProviderSessionAffinityKey, ProviderStoreError, ProviderStoreErrorKind,
};

const BUDGET: CodexHistoryQueryBudget = CodexHistoryQueryBudget::new(10_000);

fn unavailable(operation: &'static str) -> ProviderStoreError {
    ProviderStoreError::new(ProviderStoreErrorKind::Unavailable, operation)
}

/// 只实现 history 读取的测试存储；窗口顺序故意与持久顺序不同
#[derive(Clone, Default)]
struct FakeHistory {
    windows: Vec<CodexHistoryStoredWindow>,
    items: Vec<CodexHistoryStoredItem>,
    unavailable: bool,
}

impl CodexSessionContentPort for FakeHistory {
    fn commit_history_request<'a>(
        &'a self,
        _record: &'a CodexHistoryRequest,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async { Err(unavailable("commit Codex history request")) })
    }

    fn commit_history_response<'a>(
        &'a self,
        _record: &'a CodexHistoryResponse,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async { Err(unavailable("commit Codex history response")) })
    }

    fn load_history_response<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        _response_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<CodexHistoryResponseAnchor>, ProviderStoreError>> {
        Box::pin(async { Err(unavailable("load Codex history response")) })
    }

    fn load_latest_history_response<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        _window_id: &'a CodexNativeWindowId,
    ) -> BoxFuture<'a, Result<Option<CodexHistoryResponseAnchor>, ProviderStoreError>> {
        Box::pin(async { Ok(None) })
    }

    fn load_history_windows<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Vec<CodexHistoryStoredWindow>, ProviderStoreError>> {
        let windows = self.windows.clone();
        let failed = self.unavailable;
        Box::pin(async move {
            if failed {
                return Err(unavailable("load Codex history windows"));
            }
            Ok(windows)
        })
    }

    fn load_history_items<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Vec<CodexHistoryStoredItem>, ProviderStoreError>> {
        let items = self.items.clone();
        let failed = self.unavailable;
        Box::pin(async move {
            if failed {
                return Err(unavailable("load Codex history items"));
            }
            Ok(items)
        })
    }

    fn load_history_window_items<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        window_id: &'a CodexNativeWindowId,
    ) -> BoxFuture<'a, Result<Vec<CodexHistoryStoredItem>, ProviderStoreError>> {
        let items = self
            .items
            .iter()
            .filter(|item| item.window_id == *window_id)
            .cloned()
            .collect::<Vec<_>>();
        let failed = self.unavailable;
        Box::pin(async move {
            if failed {
                return Err(unavailable("load Codex history window items"));
            }
            Ok(items)
        })
    }

    // 本用例集只验证四工具查询，不跑采集；发送前快照按存储合同如实失败
    fn save_history_pending_input<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        _pending: &'a CodexHistoryPendingInput,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async { Err(unavailable("save Codex history pending input")) })
    }

    fn load_history_pending_input<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        _window_id: &'a CodexNativeWindowId,
    ) -> BoxFuture<'a, Result<Option<CodexHistoryPendingInput>, ProviderStoreError>> {
        Box::pin(async { Ok(None) })
    }

    fn append_notes_operation<'a>(
        &'a self,
        _operation: &'a NewCodexNotesOperation,
    ) -> BoxFuture<'a, Result<CodexNotesOperation, ProviderStoreError>> {
        Box::pin(async { Err(unavailable("append Codex notes operation")) })
    }

    fn start_notes_operation<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        _seq: u64,
    ) -> BoxFuture<'a, Result<CodexNotesOperation, ProviderStoreError>> {
        Box::pin(async { Err(unavailable("start Codex notes operation")) })
    }

    fn complete_notes_operation<'a>(
        &'a self,
        _completion: &'a CodexNotesCompletion,
    ) -> BoxFuture<'a, Result<CodexNotesOperation, ProviderStoreError>> {
        Box::pin(async { Err(unavailable("complete Codex notes operation")) })
    }

    fn load_notes_operations<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Vec<CodexNotesOperation>, ProviderStoreError>> {
        Box::pin(async { Err(unavailable("load Codex notes operations")) })
    }
}

#[derive(Clone)]
struct FakeSessionState {
    state: Option<CodexSessionState>,
}

impl CodexSessionStatePort for FakeSessionState {
    fn load_session<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Option<CodexSessionState>, ProviderStoreError>> {
        let state = self.state.clone();
        Box::pin(async move { Ok(state) })
    }

    fn resolve_session_origin<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        _origin: CodexSessionOrigin,
    ) -> BoxFuture<'a, Result<CodexSessionState, ProviderStoreError>> {
        Box::pin(async { Err(unavailable("resolve Codex session origin")) })
    }

    fn save_notes_replay<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        _replay: &'a CodexNotesReplay,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async { Err(unavailable("save Codex notes replay")) })
    }

    fn publish_notes_replay<'a>(
        &'a self,
        _publish: &'a CodexNotesPublish,
    ) -> BoxFuture<'a, Result<CodexNotesPublishOutcome, ProviderStoreError>> {
        Box::pin(async { Err(unavailable("publish Codex notes replay")) })
    }
}

fn window(window_id: &str, agent_name: Option<&str>, item_count: u64) -> CodexHistoryStoredWindow {
    CodexHistoryStoredWindow {
        window: CodexHistoryWindow {
            window_id: CodexNativeWindowId::try_new(window_id).expect("window id"),
            agent_name: agent_name.map(ToOwned::to_owned),
            context_window_id: None,
        },
        item_count,
    }
}

// 参数顺序与条目投影字段一一对应，便于按官方形状阅读测试夹具
#[expect(clippy::too_many_arguments)]
fn item(
    window_id: &str,
    ordinal: u64,
    item_id: &str,
    role: Option<CodexHistoryRole>,
    content_type: &str,
    text: Option<&str>,
    tool_namespace: Option<&str>,
    tool_name: Option<&str>,
) -> CodexHistoryStoredItem {
    CodexHistoryStoredItem {
        window_id: CodexNativeWindowId::try_new(window_id).expect("window id"),
        ordinal,
        item: CodexHistoryItem {
            item_id: item_id.to_owned(),
            role,
            tool_namespace: tool_namespace.map(ToOwned::to_owned),
            tool_name: tool_name.map(ToOwned::to_owned),
            content_type: content_type.to_owned(),
            raw: format!("{{\"type\":\"{content_type}\",\"id\":\"{item_id}\"}}"),
            text: text.map(ToOwned::to_owned),
            identity: item_id.to_owned(),
        },
    }
}

const UNICODE_TEXT: &str = "定价规则：单位 250 分\nunit_cents: 250";

fn history() -> FakeHistory {
    FakeHistory {
        // 窗口目录故意按倒序给出，排序只依据持久条目顺序
        windows: vec![
            window("thread-b:0", Some("/root/worker"), 2),
            window("thread-a:1", Some("/root"), 2),
            window("thread-a:0", Some("/root"), 4),
        ],
        items: vec![
            item(
                "thread-a:0",
                0,
                "msg_u1",
                Some(CodexHistoryRole::User),
                "message",
                Some(UNICODE_TEXT),
                None,
                None,
            ),
            item(
                "thread-a:0",
                1,
                "msg_a1",
                Some(CodexHistoryRole::Assistant),
                "message",
                Some("same text"),
                None,
                None,
            ),
            item(
                "thread-a:0",
                2,
                "msg_a2",
                Some(CodexHistoryRole::Assistant),
                "message",
                Some("same text"),
                None,
                None,
            ),
            item(
                "thread-a:0",
                3,
                "cmp_1",
                None,
                "compaction",
                None,
                None,
                None,
            ),
            item(
                "thread-a:1",
                4,
                "msg_u2",
                Some(CodexHistoryRole::User),
                "message",
                Some("第二条窗口的明文 needle"),
                None,
                None,
            ),
            item(
                "thread-a:1",
                5,
                "fc_1",
                Some(CodexHistoryRole::Assistant),
                "function_call",
                Some("{\"path\":\"/root/notes/a.txt\"}"),
                Some("notes"),
                Some("write_file"),
            ),
            item(
                "thread-b:0",
                6,
                "msg_w1",
                Some(CodexHistoryRole::User),
                "message",
                Some("worker 私有"),
                None,
                None,
            ),
            item(
                "thread-b:0",
                7,
                "cmp_2",
                None,
                "compaction",
                None,
                None,
                None,
            ),
        ],
        unavailable: false,
    }
}

fn key() -> ClientApiKeyId {
    ClientApiKeyId::new("key_personal").expect("key id")
}

fn session_key() -> ProviderSessionAffinityKey {
    ProviderSessionAffinityKey::try_new("session-1").expect("session key")
}

fn service(history: FakeHistory, origin: Option<CodexSessionOrigin>) -> CodexHistoryService {
    let account = ProviderAccountId::new("acct_primary").expect("account");
    let state = origin.map(|origin| CodexSessionState {
        origin,
        owner: account.clone(),
        generation: 0,
        notes: CodexNotesReplica::native(account),
        notes_replay: None,
    });
    CodexHistoryService::new(
        std::sync::Arc::new(history),
        std::sync::Arc::new(FakeSessionState { state }),
        true,
    )
}

fn assert_invalid(error: CodexHistoryQueryError, argument: &'static str) {
    match error {
        CodexHistoryQueryError::InvalidArgument(value) => assert_eq!(value, argument),
        CodexHistoryQueryError::Store(error) => panic!("unexpected store error: {error}"),
    }
}

fn assert_store_unavailable(error: CodexHistoryQueryError) {
    match error {
        CodexHistoryQueryError::Store(error) => {
            assert_eq!(error.kind(), ProviderStoreErrorKind::Unavailable);
        }
        CodexHistoryQueryError::InvalidArgument(value) => panic!("unexpected invalid `{value}`"),
    }
}

#[test]
fn list_windows_should_follow_persisted_order_and_agent_scope() {
    let key = key();
    let session = session_key();
    let service = service(history(), Some(CodexSessionOrigin::Managed));
    let scope = CodexHistoryScope {
        key_id: &key,
        session_key: &session,
        current_agent_name: "/root",
    };

    let list = block_on(service.list_windows(&scope, &CodexHistoryWindowQuery::default(), BUDGET))
        .expect("root windows");
    assert_eq!(list.agent_name, "/root");
    assert_eq!(list.total_windows, 2);
    assert_eq!(list.window_count, 2);
    assert!(!list.truncated);
    assert_eq!(list.windows[0].window_id, "thread-a:0");
    assert_eq!(list.windows[0].item_count, 4);
    assert_eq!(list.windows[1].window_id, "thread-a:1");

    let recent = block_on(service.list_windows(
        &scope,
        &CodexHistoryWindowQuery {
            recent_first: true,
            ..CodexHistoryWindowQuery::default()
        },
        BUDGET,
    ))
    .expect("recent first");
    assert_eq!(recent.windows[0].window_id, "thread-a:1");

    let limited = block_on(service.list_windows(
        &scope,
        &CodexHistoryWindowQuery {
            limit: Some(1),
            ..CodexHistoryWindowQuery::default()
        },
        BUDGET,
    ))
    .expect("limited");
    assert_eq!(limited.window_count, 1);
    assert_eq!(limited.total_windows, 2);
    assert!(limited.truncated);

    let worker = block_on(service.list_windows(
        &scope,
        &CodexHistoryWindowQuery {
            agent_name: Some("worker".to_owned()),
            ..CodexHistoryWindowQuery::default()
        },
        BUDGET,
    ))
    .expect("relative agent");
    assert_eq!(worker.agent_name, "/root/worker");
    assert_eq!(worker.windows[0].window_id, "thread-b:0");

    let absolute = block_on(service.list_windows(
        &scope,
        &CodexHistoryWindowQuery {
            agent_name: Some("/root/worker".to_owned()),
            ..CodexHistoryWindowQuery::default()
        },
        BUDGET,
    ))
    .expect("absolute agent");
    assert_eq!(
        absolute.windows, worker.windows,
        "absolute and relative names address the same agent"
    );

    let error = block_on(service.list_windows(
        &scope,
        &CodexHistoryWindowQuery {
            limit: Some(0),
            ..CodexHistoryWindowQuery::default()
        },
        BUDGET,
    ))
    .expect_err("zero limit");
    assert_invalid(error, "limit");
}

#[test]
fn list_items_should_filter_window_agent_role_and_tools() {
    let key = key();
    let session = session_key();
    let service = service(history(), Some(CodexSessionOrigin::Managed));
    let scope = CodexHistoryScope {
        key_id: &key,
        session_key: &session,
        current_agent_name: "/root",
    };

    let all = block_on(service.list_items(&scope, &CodexHistoryListItemsQuery::default(), BUDGET))
        .expect("all root items");
    assert_eq!(all.item_count, 6);
    assert_eq!(all.returned, 6);
    assert_eq!(all.items[0].item_id, "msg_u1");
    assert_eq!(all.items[5].item_id, "fc_1");
    assert!(all.items.iter().all(|item| item.agent_name == "/root"));

    let window = block_on(service.list_items(
        &scope,
        &CodexHistoryListItemsQuery {
            filter: CodexHistoryFilterQuery {
                window_id: Some("thread-a:1".to_owned()),
                ..CodexHistoryFilterQuery::default()
            },
            ..CodexHistoryListItemsQuery::default()
        },
        BUDGET,
    ))
    .expect("window filter");
    assert_eq!(window.window_id.as_deref(), Some("thread-a:1"));
    assert_eq!(window.item_count, 2);
    assert_eq!(window.items[0].item_id, "msg_u2");

    let users = block_on(service.list_items(
        &scope,
        &CodexHistoryListItemsQuery {
            filter: CodexHistoryFilterQuery {
                role: Some(CodexHistoryRole::User),
                ..CodexHistoryFilterQuery::default()
            },
            ..CodexHistoryListItemsQuery::default()
        },
        BUDGET,
    ))
    .expect("role filter");
    assert_eq!(users.item_count, 2);
    assert_eq!(users.items[1].item_id, "msg_u2");

    let tools = block_on(service.list_items(
        &scope,
        &CodexHistoryListItemsQuery {
            filter: CodexHistoryFilterQuery {
                tool_namespace: Some("notes".to_owned()),
                tool_name: Some("write_file".to_owned()),
                ..CodexHistoryFilterQuery::default()
            },
            ..CodexHistoryListItemsQuery::default()
        },
        BUDGET,
    ))
    .expect("tool filter");
    assert_eq!(tools.item_count, 1);
    assert_eq!(tools.items[0].tool_name.as_deref(), Some("write_file"));

    let other_agent_window = block_on(service.list_items(
        &scope,
        &CodexHistoryListItemsQuery {
            filter: CodexHistoryFilterQuery {
                window_id: Some("thread-b:0".to_owned()),
                ..CodexHistoryFilterQuery::default()
            },
            ..CodexHistoryListItemsQuery::default()
        },
        BUDGET,
    ))
    .expect("cross agent window");
    assert_eq!(other_agent_window.item_count, 0);
    assert!(
        other_agent_window.items.is_empty(),
        "no other window is substituted"
    );
    assert_eq!(other_agent_window.window_id.as_deref(), Some("thread-b:0"));

    let unknown_window = block_on(service.list_items(
        &scope,
        &CodexHistoryListItemsQuery {
            filter: CodexHistoryFilterQuery {
                window_id: Some("thread-a:9".to_owned()),
                ..CodexHistoryFilterQuery::default()
            },
            ..CodexHistoryListItemsQuery::default()
        },
        BUDGET,
    ))
    .expect("unknown window");
    assert_eq!(unknown_window.item_count, 0);
    assert!(!unknown_window.truncated);

    let error = block_on(service.list_items(
        &scope,
        &CodexHistoryListItemsQuery {
            filter: CodexHistoryFilterQuery {
                window_id: Some("thread-a".to_owned()),
                ..CodexHistoryFilterQuery::default()
            },
            ..CodexHistoryListItemsQuery::default()
        },
        BUDGET,
    ))
    .expect_err("malformed window id");
    assert_invalid(error, "window_id");
}

#[test]
fn list_items_should_truncate_content_in_characters_and_report_opaque_items() {
    let key = key();
    let session = session_key();
    let service = service(history(), Some(CodexSessionOrigin::Managed));
    let scope = CodexHistoryScope {
        key_id: &key,
        session_key: &session,
        current_agent_name: "/root",
    };

    let page = block_on(service.list_items(
        &scope,
        &CodexHistoryListItemsQuery {
            filter: CodexHistoryFilterQuery {
                window_id: Some("thread-a:0".to_owned()),
                ..CodexHistoryFilterQuery::default()
            },
            max_chars_per_item: Some(4),
        },
        BUDGET,
    ))
    .expect("truncated page");
    assert_eq!(page.max_chars_per_item, 4);
    assert_eq!(page.items[0].truncated_content.as_deref(), Some("定价规则"));
    assert_eq!(
        page.items[0].n_chars,
        UNICODE_TEXT.chars().count() as u64,
        "n_chars counts the complete projection in characters"
    );
    assert!(page.items[0].readable);

    let compaction = page
        .items
        .iter()
        .find(|item| item.item_id == "cmp_1")
        .expect("retained opaque item");
    assert_eq!(compaction.content_type, "compaction");
    assert!(!compaction.readable);
    assert_eq!(compaction.n_chars, 0);
    assert_eq!(compaction.truncated_content, None);
}

#[test]
fn search_should_be_case_sensitive_literal_and_scoped() {
    let key = key();
    let session = session_key();
    let service = service(history(), Some(CodexSessionOrigin::Managed));
    let scope = CodexHistoryScope {
        key_id: &key,
        session_key: &session,
        current_agent_name: "/root",
    };
    let search = |query: &str, filter: CodexHistoryFilterQuery| {
        block_on(service.search_contents(
            &scope,
            &CodexHistorySearchQuery {
                query: query.to_owned(),
                filter,
            },
            BUDGET,
        ))
    };

    let twice = search("same text", CodexHistoryFilterQuery::default()).expect("same text");
    assert_eq!(twice.item_count, 2, "identical text in two positions");
    assert_eq!(twice.items[0].item_id, "msg_a1");
    assert_eq!(twice.items[1].item_id, "msg_a2");

    let case_mismatch = search("Same text", CodexHistoryFilterQuery::default()).expect("case");
    assert_eq!(case_mismatch.item_count, 0);

    let literal = search("%", CodexHistoryFilterQuery::default()).expect("literal wildcard");
    assert_eq!(literal.item_count, 0);

    let unicode = search("明文 needle", CodexHistoryFilterQuery::default()).expect("unicode");
    assert_eq!(unicode.item_count, 1);
    assert_eq!(unicode.items[0].item_id, "msg_u2");

    let role_mismatch = search(
        "same text",
        CodexHistoryFilterQuery {
            role: Some(CodexHistoryRole::User),
            ..CodexHistoryFilterQuery::default()
        },
    )
    .expect("role scoped search");
    assert_eq!(role_mismatch.item_count, 0);

    let worker_scope = search(
        "worker",
        CodexHistoryFilterQuery {
            agent_name: Some("/root/worker".to_owned()),
            ..CodexHistoryFilterQuery::default()
        },
    )
    .expect("worker search");
    assert_eq!(worker_scope.item_count, 1);
    assert_eq!(worker_scope.items[0].item_id, "msg_w1");
    assert_eq!(worker_scope.items[0].agent_name, "/root/worker");

    let error = search("", CodexHistoryFilterQuery::default()).expect_err("empty query");
    assert_invalid(error, "query");
}

#[test]
fn search_should_report_storage_failures_instead_of_empty_matches() {
    let key = key();
    let session = session_key();
    let mut history = history();
    history.unavailable = true;
    let service = service(history, Some(CodexSessionOrigin::Managed));
    let scope = CodexHistoryScope {
        key_id: &key,
        session_key: &session,
        current_agent_name: "/root",
    };
    let error = block_on(service.search_contents(
        &scope,
        &CodexHistorySearchQuery {
            query: "same text".to_owned(),
            filter: CodexHistoryFilterQuery::default(),
        },
        BUDGET,
    ))
    .expect_err("store failure is an error");
    assert_store_unavailable(error);
}

#[test]
fn read_item_should_read_unicode_character_ranges() {
    let key = key();
    let session = session_key();
    let service = service(history(), Some(CodexSessionOrigin::Managed));
    let scope = CodexHistoryScope {
        key_id: &key,
        session_key: &session,
        current_agent_name: "/root",
    };
    let read = |window_id: &str, item_id: &str, offset: Option<u64>, limit: Option<u64>| {
        block_on(service.read_item(
            &scope,
            &CodexHistoryReadQuery {
                agent_name: None,
                window_id: window_id.to_owned(),
                item_id: item_id.to_owned(),
                offset_chars: offset,
                limit_chars: limit,
            },
            BUDGET,
        ))
        .expect("read item")
    };

    let CodexHistoryReadItemOutcome::Read(first) = read("thread-a:0", "msg_u1", Some(0), Some(2))
    else {
        panic!("first range");
    };
    assert_eq!(first.content.as_deref(), Some("定价"));
    assert_eq!(first.offset_chars, 0);
    assert_eq!(first.next_offset_chars, 2);
    assert_eq!(first.n_chars, UNICODE_TEXT.chars().count() as u64);
    assert!(first.readable);

    let CodexHistoryReadItemOutcome::Read(second) = read(
        "thread-a:0",
        "msg_u1",
        Some(first.next_offset_chars),
        Some(5),
    ) else {
        panic!("second range");
    };
    assert_eq!(second.content.as_deref(), Some("规则：单位"));
    assert_eq!(second.next_offset_chars, 7);

    let CodexHistoryReadItemOutcome::Read(past_end) =
        read("thread-a:0", "msg_u1", Some(10_000), Some(4))
    else {
        panic!("past end");
    };
    assert_eq!(past_end.content.as_deref(), Some(""));
    assert_eq!(past_end.next_offset_chars, past_end.n_chars);

    let CodexHistoryReadItemOutcome::Read(opaque) = read("thread-a:0", "cmp_1", None, None) else {
        panic!("opaque item");
    };
    assert!(!opaque.readable);
    assert_eq!(opaque.content, None);
    assert_eq!(opaque.n_chars, 0);

    assert_eq!(
        read("thread-a:1", "msg_u1", None, None),
        CodexHistoryReadItemOutcome::NotFound { window_found: true },
        "no cross-window fallback"
    );
    assert_eq!(
        read("thread-a:9", "msg_u1", None, None),
        CodexHistoryReadItemOutcome::NotFound {
            window_found: false
        }
    );
    assert_eq!(
        read("thread-b:0", "msg_w1", None, None),
        CodexHistoryReadItemOutcome::NotFound {
            window_found: false
        },
        "another agent's window is outside this scope"
    );

    let error = block_on(service.read_item(
        &scope,
        &CodexHistoryReadQuery {
            agent_name: None,
            window_id: "thread-a:0".to_owned(),
            item_id: "msg_u1".to_owned(),
            offset_chars: None,
            limit_chars: Some(0),
        },
        BUDGET,
    ))
    .expect_err("zero limit chars");
    assert_invalid(error, "limit_chars");
}

#[test]
fn output_budget_should_bound_page_size_and_content_bytes() {
    let key = key();
    let session = session_key();
    let mut history = history();
    let long_text = "a".repeat(400);
    history.items.push(item(
        "thread-a:1",
        8,
        "msg_long",
        Some(CodexHistoryRole::Assistant),
        "message",
        Some(&long_text),
        None,
        None,
    ));
    history
        .windows
        .retain(|window| window.window.window_id.expose_to_store() != "thread-a:1");
    history.windows.push(window("thread-a:1", Some("/root"), 3));
    let service = service(history, Some(CodexSessionOrigin::Managed));
    let scope = CodexHistoryScope {
        key_id: &key,
        session_key: &session,
        current_agent_name: "/root",
    };

    // 内容预算 = 600 - 外壳预留 512 = 88 字节
    let page = block_on(service.list_items(
        &scope,
        &CodexHistoryListItemsQuery {
            filter: CodexHistoryFilterQuery {
                window_id: Some("thread-a:1".to_owned()),
                recent_first: true,
                ..CodexHistoryFilterQuery::default()
            },
            max_chars_per_item: Some(400),
        },
        CodexHistoryQueryBudget::new(600),
    ))
    .expect("budgeted page");
    let long = page
        .items
        .iter()
        .find(|item| item.item_id == "msg_long")
        .expect("long item");
    assert_eq!(long.n_chars, 400);
    assert!(long.truncated_content.as_ref().map_or(0, String::len) <= 88);

    let CodexHistoryReadItemOutcome::Read(read) = block_on(service.read_item(
        &scope,
        &CodexHistoryReadQuery {
            agent_name: None,
            window_id: "thread-a:1".to_owned(),
            item_id: "msg_long".to_owned(),
            offset_chars: None,
            limit_chars: Some(400),
        },
        CodexHistoryQueryBudget::new(600),
    ))
    .expect("budgeted read") else {
        panic!("readable item");
    };
    let content = read.content.expect("content");
    assert_eq!(content.len(), 88);
    assert_eq!(read.next_offset_chars, 88);
    assert_eq!(read.n_chars, 400);
}

#[test]
fn route_should_only_serve_managed_sessions_locally() {
    let key = key();
    let session = session_key();
    let scope = CodexHistoryScope {
        key_id: &key,
        session_key: &session,
        current_agent_name: "/root",
    };
    for (origin, expected) in [
        (Some(CodexSessionOrigin::Managed), CodexHistoryRoute::Local),
        (
            Some(CodexSessionOrigin::Legacy),
            CodexHistoryRoute::Upstream,
        ),
        (
            Some(CodexSessionOrigin::Unclassified),
            CodexHistoryRoute::Upstream,
        ),
        (None, CodexHistoryRoute::Upstream),
    ] {
        let service = service(history(), origin);
        assert_eq!(
            block_on(service.route(&scope)).expect("route"),
            expected,
            "origin {origin:?}"
        );
    }
}

#[test]
fn agent_names_should_normalize_once_against_the_current_agent() {
    assert_eq!(
        resolve_agent_name("/root", None).expect("current agent"),
        "/root"
    );
    assert_eq!(
        resolve_agent_name("/root/worker/", Some(".")).expect("current agent"),
        "/root/worker"
    );
    assert_eq!(
        resolve_agent_name("/root/worker", Some("../sibling")).expect("sibling"),
        "/root/sibling"
    );
    assert_eq!(
        resolve_agent_name("/root", Some("..")).expect("root stays root"),
        "/root"
    );
    assert_eq!(
        resolve_agent_name("/root", Some("/root//worker/")).expect("absolute"),
        "/root/worker"
    );
    let error = resolve_agent_name("", None).expect_err("empty current agent");
    assert_invalid(error, "agent_name");
}

#[test]
fn execute_should_decide_the_origin_before_running_the_tool() {
    let key = key();
    let session = session_key();
    let scope = CodexHistoryScope {
        key_id: &key,
        session_key: &session,
        current_agent_name: "/root",
    };
    let query = CodexHistoryToolQuery::ListWindows(CodexHistoryWindowQuery::default());

    let managed = service(history(), Some(CodexSessionOrigin::Managed));
    let outcome = block_on(managed.execute(&scope, &query, BUDGET)).expect("local execution");
    let CodexHistoryToolOutcome::Completed(CodexHistoryToolResult::Windows(list)) = outcome else {
        panic!("local window list");
    };
    assert_eq!(list.window_count, 2);

    // legacy 会话直接走上游，不读取本地 history
    let mut failed = history();
    failed.unavailable = true;
    let legacy = service(failed, Some(CodexSessionOrigin::Legacy));
    assert_eq!(
        block_on(legacy.execute(&scope, &query, BUDGET)).expect("upstream"),
        CodexHistoryToolOutcome::Upstream
    );

    let unclassified = service(history(), Some(CodexSessionOrigin::Unclassified));
    assert_eq!(
        block_on(unclassified.execute(&scope, &query, BUDGET)).expect("upstream"),
        CodexHistoryToolOutcome::Upstream
    );

    let invalid = CodexHistoryToolQuery::SearchContents(CodexHistorySearchQuery {
        query: String::new(),
        filter: CodexHistoryFilterQuery::default(),
    });
    let error = block_on(managed.execute(&scope, &invalid, BUDGET)).expect_err("empty query");
    assert_invalid(error, "query");
}

#[test]
fn item_identity_should_prefer_declared_ids_and_keep_generated_ids_positional() {
    let window = CodexNativeWindowId::try_new("thread-a:0").expect("window");
    assert_eq!(codex_history_item_id(Some("msg_1"), &window, 3), "msg_1");
    assert_eq!(
        codex_history_item_id(Some("  "), &window, 3),
        "cprs:thread-a:0:3",
        "empty declared ids fall back to the positional identity"
    );
    assert_eq!(
        codex_history_item_id(None, &window, 3),
        codex_history_item_id(None, &window, 3)
    );
    assert_ne!(
        codex_history_item_id(None, &window, 3),
        codex_history_item_id(None, &window, 4),
        "identical text in another position keeps a distinct identity"
    );
}

#[test]
fn role_filter_should_reject_unknown_wire_values_instead_of_ignoring_them() {
    let mut filter = CodexHistoryFilterQuery::default();
    filter.set_role(Some("assistant")).expect("official role");
    assert_eq!(filter.role, Some(CodexHistoryRole::Assistant));

    filter.set_role(None).expect("no filter");
    assert_eq!(filter.role, None);

    let error = filter.set_role(Some("narrator")).expect_err("unknown role");
    assert_invalid(error, "role");
}
