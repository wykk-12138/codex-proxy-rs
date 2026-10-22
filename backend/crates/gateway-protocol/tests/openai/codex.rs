//! 验证 Codex 会话身份优先级、轮次元数据与多代理模式解析

use gateway_protocol::openai::{
    CodexSessionIdentity, CodexSessionSources, codex_responses_request_semantics, codex_session_id,
    codex_session_identity, codex_thread_id,
};
use serde_json::{Map, Value, json};

#[test]
fn reasoning_effort_should_observe_unsigned_numbers_without_changing_the_body() {
    for (effort, expected) in [
        (json!(0), Some("0")),
        (json!(64), Some("64")),
        (json!(u64::MAX), Some("18446744073709551615")),
        (json!(" high "), Some("high")),
        (json!(-1), None),
        (json!(1.5), None),
        (json!(true), None),
        (json!(null), None),
    ] {
        let body = object(json!({"reasoning":{"effort":effort}}));
        let semantics = codex_responses_request_semantics(&body, &Map::new());
        assert_eq!(semantics.reasoning_effort.as_deref(), expected);
        assert_eq!(body["reasoning"]["effort"], effort);
    }
}

#[test]
fn codex_session_should_prefer_the_connection_identity_over_body_metadata() {
    let body = object(
        json!({"session_id":"body-session", "client_metadata":{"session_id":"metadata-session"}}),
    );
    let context = object(
        json!({"session_id":" root-session ", "turn_metadata":"{\"session_id\":\"turn-session\"}"}),
    );
    assert_eq!(
        codex_session_id(&body, &context).as_deref(),
        Some("root-session")
    );
}

#[test]
fn codex_session_should_accept_explicit_metadata_and_ignore_unrelated_ids() {
    for (body, context) in [
        (json!({"session_id":"root"}), json!({})),
        (json!({"client_metadata":{"session_id":"root"}}), json!({})),
        (
            json!({"client_metadata":{"x-codex-turn-metadata":"{\"session_id\":\"root\"}"}}),
            json!({}),
        ),
        (
            json!({}),
            json!({"turn_metadata":"{\"session_id\":\"root\"}"}),
        ),
        (json!({"context":{"session_id":"root"}}), json!({})),
    ] {
        assert_eq!(
            codex_session_id(&object(body), &object(context)).as_deref(),
            Some("root")
        );
    }
    for body in [
        json!({"session_id":" ", "thread_id":"child", "turn_id":"turn", "id":"request"}),
        json!({"client_metadata":{"x-codex-turn-metadata":"invalid JSON"}}),
    ] {
        assert!(codex_session_id(&object(body), &Map::new()).is_none());
    }
}

#[test]
fn codex_thread_should_use_explicit_identity_and_never_guess_from_other_ids() {
    for (body, context, expected) in [
        (
            json!({"thread_id":"body", "client_metadata":{"thread_id":"metadata"}}),
            json!({"thread_id":" child "}),
            Some("child"),
        ),
        (
            json!({"thread_id":"body", "client_metadata":{"thread_id":"metadata"}}),
            json!({}),
            Some("body"),
        ),
        (
            json!({"client_metadata":{"thread_id":"metadata"}}),
            json!({}),
            Some("metadata"),
        ),
        (
            json!({"client_metadata":{"x-codex-turn-metadata":"{\"thread_id\":\"metadata-thread\"}"}}),
            json!({}),
            Some("metadata-thread"),
        ),
        (
            json!({"turnMetadata":"{\"thread_id\":\"body-turn\"}"}),
            json!({"turn_metadata":"{\"thread_id\":\"header-turn\"}"}),
            Some("header-turn"),
        ),
        (
            json!({"context":{"thread_id":"context-thread"}}),
            json!({}),
            Some("context-thread"),
        ),
        (
            json!({"thread_id":" ", "session_id":"root", "turn_id":"turn", "agent_name":"agent"}),
            json!({}),
            None,
        ),
        (
            json!({"client_metadata":{"x-codex-turn-metadata":"invalid JSON"}}),
            json!({}),
            None,
        ),
    ] {
        assert_eq!(
            codex_thread_id(&object(body), &object(context)).as_deref(),
            expected
        );
    }
}

#[test]
fn codex_semantics_should_use_protocol_context_before_body_metadata() {
    let body = json!({
        "model": "grok-4.5",
        "reasoning": {"effort": " xhigh "},
        "client_metadata": {
            "x-codex-turn-metadata": "{\"request_kind\":\"review\"}"
        },
        "input": [
            {"type": "message", "role": "user", "content": "history"},
            {"type": "compaction_trigger"}
        ]
    });
    let body = object(body);
    let context = Map::from_iter([(
        "turn_metadata".to_owned(),
        json!("{\"request_kind\":\"compaction\",\"subagent_kind\":\"compact\"}"),
    )]);

    let semantics = codex_responses_request_semantics(&body, &context);

    assert_eq!(semantics.reasoning_effort.as_deref(), Some("xhigh"));
    assert_eq!(semantics.request_kind.as_deref(), Some("compaction"));
    assert_eq!(semantics.subagent_kind.as_deref(), Some("compact"));
    assert_eq!(semantics.reasoning_preset, None);
    assert!(semantics.compact);
}

#[test]
fn codex_semantics_should_detect_ultra_preset_from_proactive_multi_agent() {
    let body = object(json!({
        "model": "gpt-test",
        "reasoning": {"effort": "max"},
        "input": [
            {
                "type": "message",
                "role": "developer",
                "content": [{
                    "type": "input_text",
                    "text": "<multi_agent_mode>Proactive multi-agent delegation is active.</multi_agent_mode>"
                }]
            }
        ]
    }));

    let semantics = codex_responses_request_semantics(&body, &Map::new());

    assert_eq!(semantics.reasoning_effort.as_deref(), Some("max"));
    assert_eq!(semantics.reasoning_preset, Some("ultra"));
    assert!(!semantics.compact);
}

fn object(value: Value) -> Map<String, Value> {
    let Value::Object(object) = value else {
        panic!("test value must be an object");
    };
    object
}

#[test]
fn account_identity_uses_logical_metadata_instead_of_parent_cache_routing() {
    use gateway_protocol::openai::codex_account_session_id;
    let context = object(
        json!({"session_id":"parent-cache", "turn_metadata": "{\"session_id\":\"handshake-session\"}"}),
    );
    let body = object(
        json!({"client_metadata":{"x-codex-turn-metadata":"{\"session_id\":\"actual-session\",\"thread_id\":\"child\"}"}}),
    );
    assert_eq!(
        codex_account_session_id(&body, &context).as_deref(),
        Some("actual-session")
    );
    assert_eq!(
        codex_session_id(&body, &context).as_deref(),
        Some("parent-cache")
    );
}

#[test]
fn account_identity_does_not_infer_a_session_from_turn_or_cache_keys() {
    use gateway_protocol::openai::codex_account_session_id;
    for body in [
        json!({"prompt_cache_key":"shared-prompt"}),
        json!({"turn_id":"turn", "thread_id":"child"}),
        json!({"input":"hello"}),
    ] {
        assert_eq!(codex_account_session_id(&object(body), &Map::new()), None);
    }
}

fn session_sources<'a>(
    body: &'a Map<String, Value>,
    context: &'a Map<String, Value>,
) -> CodexSessionSources<'a> {
    CodexSessionSources {
        body,
        context,
        turn_metadata: None,
        session_id: None,
        endpoint_session: None,
    }
}

#[test]
fn pool_session_identity_resolves_each_single_source() {
    for (body, context, turn_metadata, session_id, endpoint, expected) in [
        // 官方 history/notes 把会话放在正文 context
        (
            json!({"context":{"session_id":"body-context"}}),
            json!({}),
            None,
            None,
            None,
            "body-context",
        ),
        // 官方 client_metadata.session_id
        (
            json!({"client_metadata":{"session_id":"metadata"}}),
            json!({}),
            None,
            None,
            None,
            "metadata",
        ),
        // Search 的正文 id 属端点级逻辑会话
        (
            json!({"id":"endpoint"}),
            json!({}),
            None,
            None,
            Some("endpoint"),
            "endpoint",
        ),
        // 请求头的 turn metadata 是逻辑来源
        (
            json!({}),
            json!({}),
            Some("{\"session_id\":\"header-turn\"}"),
            None,
            None,
            "header-turn",
        ),
        // 没有逻辑声明时兜底到 session-id 请求头
        (
            json!({}),
            json!({}),
            None,
            Some("header-session"),
            None,
            "header-session",
        ),
        // 再兜底到正文顶层 session_id
        (
            json!({"session_id":"body-session"}),
            json!({}),
            None,
            None,
            None,
            "body-session",
        ),
    ] {
        let body = object(body);
        let context = object(context);
        let sources = CodexSessionSources {
            turn_metadata,
            session_id,
            endpoint_session: endpoint,
            ..session_sources(&body, &context)
        };
        assert_eq!(
            codex_session_identity(sources),
            CodexSessionIdentity::Resolved(expected.to_owned()),
            "会话来源应解析出 {expected}"
        );
    }
}

#[test]
fn pool_session_identity_deduplicates_same_logical_declaration() {
    // 模型请求常同时带 turn metadata 与 client_metadata，同值不算冲突
    let body = object(json!({"client_metadata":{"session_id":"shared"}}));
    let context = Map::new();
    let sources = CodexSessionSources {
        turn_metadata: Some("{\"session_id\":\"shared\"}"),
        ..session_sources(&body, &context)
    };
    assert_eq!(
        codex_session_identity(sources),
        CodexSessionIdentity::Resolved("shared".to_owned())
    );
}

#[test]
fn pool_session_identity_prefers_logical_declaration_over_transport_fallback() {
    let body = object(json!({"turnMetadata":"{\"session_id\":\"logical\"}","session_id":"body"}));
    let context = Map::new();
    let sources = CodexSessionSources {
        session_id: Some("header"),
        ..session_sources(&body, &context)
    };
    assert_eq!(
        codex_session_identity(sources),
        CodexSessionIdentity::Resolved("logical".to_owned()),
        "逻辑声明与不同兜底值并存不算冲突"
    );
    // 没有逻辑声明时，兜底头先于正文顶层 session_id
    let body = object(json!({"session_id":"body"}));
    let sources = CodexSessionSources {
        session_id: Some("header"),
        ..session_sources(&body, &context)
    };
    assert_eq!(
        codex_session_identity(sources),
        CodexSessionIdentity::Resolved("header".to_owned())
    );
}

#[test]
fn pool_session_identity_reports_conflict_between_logical_declarations() {
    for (body, context, turn_metadata, endpoint) in [
        // client_metadata 与请求上下文 turn metadata 不同
        (
            json!({"client_metadata":{"session_id":"session-a"}}),
            json!({"turn_metadata":"{\"session_id\":\"session-b\"}"}),
            None,
            None,
        ),
        // Search 的 id 与 turn metadata 不同
        (
            json!({"id":"session-a"}),
            json!({}),
            Some("{\"session_id\":\"session-b\"}"),
            Some("session-a"),
        ),
    ] {
        let body = object(body);
        let context = object(context);
        let sources = CodexSessionSources {
            turn_metadata,
            endpoint_session: endpoint,
            ..session_sources(&body, &context)
        };
        assert_eq!(
            codex_session_identity(sources),
            CodexSessionIdentity::Conflict
        );
    }
}

#[test]
fn pool_session_identity_reports_missing_without_any_declaration() {
    let body = object(json!({"session_id":" ","client_metadata":{"thread_id":"child"}}));
    let context = object(json!({"turn_metadata":"invalid JSON"}));
    assert_eq!(
        codex_session_identity(session_sources(&body, &context)),
        CodexSessionIdentity::Missing
    );
    let sources = CodexSessionSources {
        turn_metadata: Some("{\"session_id\":\" \"}"),
        session_id: Some(" "),
        endpoint_session: Some(" "),
        ..session_sources(&body, &context)
    };
    assert_eq!(
        codex_session_identity(sources),
        CodexSessionIdentity::Missing
    );
}

#[test]
fn pool_session_identity_reads_official_history_and_notes_body() {
    // 官方 history/notes 请求体只把会话放在 context，带 current_agent_name 也不影响
    for body in [
        json!({"context":{"session_id":"thread-notes","current_agent_name":"child"},"opaque":"encrypted"}),
        json!({"context":{"session_id":"thread-notes","current_agent_name":"child"},"commands":{"open":[]}}),
    ] {
        let body = object(body);
        assert_eq!(
            codex_session_identity(session_sources(&body, &Map::new())),
            CodexSessionIdentity::Resolved("thread-notes".to_owned())
        );
        // 同样的正文与模型请求解析出同一会话标识
        assert_eq!(
            codex_session_id(&body, &Map::new()).as_deref(),
            Some("thread-notes")
        );
    }
    // 附带不同 session-id 头时按计划以传输兜底头为准，不当成第二个逻辑来源
    let body = object(json!({"context":{"session_id":"thread-notes"}}));
    let context = Map::new();
    let sources = CodexSessionSources {
        session_id: Some("cache-routing"),
        ..session_sources(&body, &context)
    };
    assert_eq!(
        codex_session_identity(sources),
        CodexSessionIdentity::Resolved("cache-routing".to_owned())
    );
}
