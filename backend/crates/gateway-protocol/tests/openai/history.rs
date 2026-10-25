//! 验证 history 四工具参数解析、输出预算、结果编码与条目明文投影

use gateway_protocol::openai::{
    CodexHistoryItemEntry, CodexHistoryItemListResult, CodexHistoryItemNotFoundResult,
    CodexHistoryReadItemResult, CodexHistoryTool, CodexHistoryToolArguments,
    CodexHistoryWindowEntry, CodexHistoryWindowListResult, CodexHistoryWireError,
    CodexToolOutputTruncationPolicy, OPAQUE_ITEM_NOTE, codex_history_declared_item_id,
    codex_history_item_attachments, codex_history_item_projection, codex_history_item_semantics,
    parse_codex_history_tool_request, project_local_history_search_query,
};
use serde_json::{Value, json};

fn parse(tool: CodexHistoryTool, body: Value) -> CodexHistoryToolArguments {
    let body = serde_json::to_vec(&body).expect("serialize arguments");
    parse_codex_history_tool_request(tool, &body)
        .expect("valid arguments")
        .arguments
}

#[test]
fn list_windows_should_accept_the_official_optional_fields() {
    let arguments = parse(
        CodexHistoryTool::ListWindows,
        json!({"agent_name": "/root/worker", "limit": 5, "recent_first": true}),
    );
    let CodexHistoryToolArguments::ListWindows(arguments) = arguments else {
        panic!("list_windows arguments");
    };
    assert_eq!(arguments.agent_name.as_deref(), Some("/root/worker"));
    assert_eq!(arguments.limit, Some(5));
    assert_eq!(arguments.recent_first, Some(true));
}

#[test]
fn list_items_should_keep_every_official_filter() {
    let arguments = parse(
        CodexHistoryTool::ListItems,
        json!({
            "agent_name": "worker",
            "limit": 10,
            "recent_first": false,
            "role": "assistant",
            "tool_name": "write_file",
            "tool_namespace": "notes",
            "window_id": "thread:0",
            "max_chars_per_item": 4000,
        }),
    );
    let CodexHistoryToolArguments::ListItems(arguments) = arguments else {
        panic!("list_items arguments");
    };
    assert_eq!(arguments.agent_name.as_deref(), Some("worker"));
    assert_eq!(arguments.role.as_deref(), Some("assistant"));
    assert_eq!(arguments.tool_name.as_deref(), Some("write_file"));
    assert_eq!(arguments.tool_namespace.as_deref(), Some("notes"));
    assert_eq!(arguments.window_id.as_deref(), Some("thread:0"));
    assert_eq!(arguments.max_chars_per_item, Some(4000));
    assert_eq!(arguments.recent_first, Some(false));
}

#[test]
fn read_item_and_search_should_require_their_official_fields() {
    let arguments = parse(
        CodexHistoryTool::ReadItem,
        json!({"window_id": "thread:0", "item_id": "msg_1", "offset_chars": 3, "limit_chars": 7}),
    );
    let CodexHistoryToolArguments::ReadItem(arguments) = arguments else {
        panic!("read_item arguments");
    };
    assert_eq!(arguments.window_id, "thread:0");
    assert_eq!(arguments.item_id, "msg_1");
    assert_eq!(arguments.offset_chars, Some(3));
    assert_eq!(arguments.limit_chars, Some(7));

    let body = serde_json::to_vec(&json!({"window_id": "thread:0"})).expect("body");
    let error = parse_codex_history_tool_request(CodexHistoryTool::ReadItem, &body)
        .expect_err("item_id is required");
    assert_eq!(error, CodexHistoryWireError::InvalidArguments("read_item"));

    let body = serde_json::to_vec(&json!({"limit": 3})).expect("body");
    let error = parse_codex_history_tool_request(CodexHistoryTool::SearchContents, &body)
        .expect_err("query is required");
    assert_eq!(
        error,
        CodexHistoryWireError::InvalidArguments("search_contents")
    );
}

#[test]
fn empty_body_should_mean_empty_arguments_like_the_official_client() {
    let request =
        parse_codex_history_tool_request(CodexHistoryTool::ListWindows, b"").expect("empty body");
    assert_eq!(
        request.arguments,
        CodexHistoryToolArguments::ListWindows(Default::default())
    );

    let error =
        parse_codex_history_tool_request(CodexHistoryTool::ListItems, b"[]").expect_err("array");
    assert_eq!(error, CodexHistoryWireError::NotAnObject);
    let error =
        parse_codex_history_tool_request(CodexHistoryTool::ListItems, b"{").expect_err("json");
    assert_eq!(error, CodexHistoryWireError::InvalidJson);
}

#[test]
fn request_should_read_the_client_injected_current_agent() {
    let body = serde_json::to_vec(&json!({
        "query": "needle",
        "context": {"session_id": "session", "current_agent_name": "/root/worker"},
    }))
    .expect("body");
    let request =
        parse_codex_history_tool_request(CodexHistoryTool::SearchContents, &body).expect("request");
    assert_eq!(request.current_agent_name.as_deref(), Some("/root/worker"));
    assert_eq!(request.tool.name(), "search_contents");
    assert_eq!(
        CodexHistoryTool::from_name("read_item"),
        Some(CodexHistoryTool::ReadItem)
    );
    assert_eq!(CodexHistoryTool::from_name("write_file"), None);
}

#[test]
fn truncation_policy_should_follow_the_official_bytes_and_tokens_modes() {
    let bytes = CodexToolOutputTruncationPolicy::parse(r#"{"mode":"bytes","limit":1024}"#)
        .expect("bytes policy");
    assert_eq!(bytes.byte_budget(), 1024);
    assert_eq!(bytes.limit(), 1024);

    let tokens = CodexToolOutputTruncationPolicy::parse(r#"{"mode":"tokens","limit":250}"#)
        .expect("tokens policy");
    assert_eq!(tokens.byte_budget(), 1000);

    assert_eq!(
        CodexToolOutputTruncationPolicy::parse(r#"{"mode":"lines","limit":10}"#),
        None
    );
    assert_eq!(
        CodexToolOutputTruncationPolicy::parse(r#"{"mode":"bytes","limit":-1}"#),
        None
    );
    assert_eq!(CodexToolOutputTruncationPolicy::parse("not-json"), None);
}

#[test]
fn message_projection_should_keep_plaintext_and_official_roles() {
    let projection = codex_history_item_projection(
        &json!({
            "type": "message",
            "role": "user",
            "content": [
                {"type": "input_text", "text": "first line"},
                {"type": "input_image", "image_url": "data:image/png;base64,cG5n"},
                {"type": "input_text", "text": "second line"},
            ],
        })
        .to_string(),
    );
    assert_eq!(projection.content_type, "message");
    assert_eq!(projection.role, Some("user"));
    assert_eq!(projection.text.as_deref(), Some("first line\nsecond line"));
    assert_eq!(projection.tool_name, None);

    // 官方 input 允许把消息内容直接写成字符串
    let projection = codex_history_item_projection(
        &json!({"role": "developer", "content": "policy"}).to_string(),
    );
    assert_eq!(projection.content_type, "message");
    assert_eq!(projection.role, Some("developer"));
    assert_eq!(projection.text.as_deref(), Some("policy"));
}

#[test]
fn tool_projection_should_exclude_encrypted_arguments_and_keep_the_rest() {
    let projection = codex_history_item_projection(
        &json!({
            "type": "function_call",
            "name": "write_file",
            "namespace": "notes",
            "arguments": "{\"path\":\"/root/notes/a.txt\",\"text\":\"cipher\"}",
            "encrypted_function_args": ["text"],
        })
        .to_string(),
    );
    assert_eq!(projection.content_type, "function_call");
    assert_eq!(projection.role, Some("assistant"));
    assert_eq!(projection.tool_namespace.as_deref(), Some("notes"));
    assert_eq!(projection.tool_name.as_deref(), Some("write_file"));
    let text = projection.text.expect("plaintext arguments");
    assert!(text.contains("/root/notes/a.txt"));
    assert!(!text.contains("cipher"));

    let projection = codex_history_item_projection(
        &json!({
            "type": "function_call",
            "name": "write_file",
            "namespace": "notes",
            "arguments": "{\"text\":\"cipher\"}",
            "encrypted_function_args": ["text"],
        })
        .to_string(),
    );
    assert_eq!(projection.text, None, "entirely encrypted arguments");
}

#[test]
fn known_notes_encrypted_arguments_are_not_indexed_without_a_declaration() {
    // 官方 notes 写工具 schema 把 text 声明为 encrypted；真实调用可能不重复写进
    // encrypted_function_args，按已知 namespace/工具名排除该字段
    let write = codex_history_item_projection(
        &json!({
            "type": "function_call",
            "name": "write_file",
            "namespace": "notes",
            "arguments": "{\"path\":\"/root/notes/a.txt\",\"text\":\"enc_cipher\"}",
        })
        .to_string(),
    );
    let text = write.text.expect("path remains readable");
    assert!(text.contains("/root/notes/a.txt"));
    assert!(!text.contains("enc_cipher"));

    let append = codex_history_item_projection(
        &json!({
            "type": "function_call",
            "name": "append_to_file",
            "namespace": "notes",
            "arguments": "{\"path\":\"/root/notes/a.txt\",\"text\":\"enc_cipher\"}",
        })
        .to_string(),
    );
    let text = append.text.expect("path remains readable");
    assert!(text.contains("/root/notes/a.txt"));
    assert!(!text.contains("enc_cipher"));

    let search = codex_history_item_projection(
        &json!({
            "type": "function_call",
            "name": "search_contents",
            "namespace": "notes",
            "arguments": "{\"query\":\"enc_cipher\"}",
        })
        .to_string(),
    );
    assert_eq!(search.text, None, "encrypted note query has no plaintext");
}

#[test]
fn plaintext_queries_and_unknown_tools_keep_their_projection() {
    // managed 会话的 history 检索查询已由网关明文化，可以继续索引
    let history = codex_history_item_projection(
        &json!({
            "type": "function_call",
            "name": "search_contents",
            "namespace": "history",
            "arguments": "{\"query\":\"recover me\"}",
        })
        .to_string(),
    );
    assert_eq!(history.text.as_deref(), Some("{\"query\":\"recover me\"}"));

    // 未知工具不凭参数名或长度猜密文
    let unknown = codex_history_item_projection(
        &json!({
            "type": "function_call",
            "name": "custom_tool",
            "arguments": "{\"text\":\"visible\"}",
        })
        .to_string(),
    );
    assert_eq!(unknown.text.as_deref(), Some("{\"text\":\"visible\"}"));
}

#[test]
fn tool_output_projection_should_index_only_plaintext_parts() {
    let projection = codex_history_item_projection(
        &json!({
            "type": "function_call_output",
            "call_id": "call_1",
            "output": [
                {"type": "input_text", "text": "visible"},
                {"type": "encrypted_content", "encrypted_content": "opaque"},
            ],
        })
        .to_string(),
    );
    assert_eq!(projection.content_type, "function_call_output");
    assert_eq!(projection.role, Some("tool"));
    assert_eq!(projection.text.as_deref(), Some("visible"));

    let projection = codex_history_item_projection(
        &json!({
            "type": "function_call_output",
            "call_id": "call_1",
            "output": [{"type": "encrypted_content", "encrypted_content": "opaque"}],
        })
        .to_string(),
    );
    assert_eq!(projection.text, None);

    let projection = codex_history_item_projection(
        &json!({
            "type": "function_call_output",
            "call_id": "call_1",
            "output": "shell output",
        })
        .to_string(),
    );
    assert_eq!(projection.text.as_deref(), Some("shell output"));
}

#[test]
fn opaque_items_should_not_receive_a_fabricated_projection() {
    let reasoning = codex_history_item_projection(
        &json!({
            "type": "reasoning",
            "summary": [{"type": "summary_text", "text": "kept summary"}],
            "encrypted_content": "opaque",
        })
        .to_string(),
    );
    assert_eq!(reasoning.role, Some("assistant"));
    assert_eq!(
        reasoning.text.as_deref(),
        Some("kept summary"),
        "official reasoning summaries use summary_text parts"
    );

    let compaction = codex_history_item_projection(
        &json!({"type": "compaction", "encrypted_content": "opaque"}).to_string(),
    );
    assert_eq!(compaction.content_type, "compaction");
    assert_eq!(compaction.role, None);
    assert_eq!(compaction.text, None);

    let agent_message = codex_history_item_projection(
        &json!({
            "type": "agent_message",
            "content": [
                {"type": "input_text", "text": "plain"},
                {"type": "encrypted_content", "encrypted_content": "opaque"},
            ],
        })
        .to_string(),
    );
    assert_eq!(agent_message.role, Some("assistant"));
    assert_eq!(agent_message.text, None);

    let custom_call = codex_history_item_projection(
        &json!({"type": "custom_tool_call", "name": "shell", "input": "ls"}).to_string(),
    );
    assert_eq!(custom_call.tool_name.as_deref(), Some("shell"));
    assert_eq!(custom_call.text.as_deref(), Some("ls"));
}

#[test]
fn item_attachments_should_extract_inline_images_only() {
    let raw = json!({
        "type": "message",
        "role": "user",
        "content": [
            {"type": "input_text", "text": "look"},
            {
                "type": "input_image",
                "image_url": "data:image/png;base64,cG5n",
                "detail": "original",
            },
            {"type": "input_image", "file_id": "file-123"},
            {
                "type": "input_image",
                "image_url": "data:image/jpeg;base64,anBlZw==",
                "detail": "unexpected",
            },
        ],
    })
    .to_string();
    let attachments = codex_history_item_attachments(&raw);
    assert_eq!(attachments.omitted, 1);
    assert_eq!(attachments.images.len(), 2);
    assert_eq!(attachments.images[0].data, "cG5n");
    assert_eq!(attachments.images[0].mime_type, "image/png");
    assert_eq!(attachments.images[0].detail.as_deref(), Some("original"));
    assert_eq!(attachments.images[1].data, "anBlZw==");
    assert_eq!(attachments.images[1].detail, None);

    let projection = codex_history_item_projection(&raw);
    assert_eq!(
        projection.text.as_deref(),
        Some("look"),
        "Base64 attachments never enter the text projection"
    );
}

fn item_entry(content: Option<&str>) -> CodexHistoryItemEntry {
    CodexHistoryItemEntry::new(
        "msg_1".to_owned(),
        "thread:0".to_owned(),
        "/root".to_owned(),
        Some("user".to_owned()),
        "message".to_owned(),
        None,
        None,
        4,
        content.is_some(),
        content.map_or(0, |content| content.chars().count() as u64),
        content.map(ToOwned::to_owned),
    )
}

#[test]
fn list_results_should_keep_official_shapes_and_item_markers() {
    let windows = CodexHistoryWindowListResult {
        agent_name: "/root".to_owned(),
        window_count: 1,
        total_windows: 2,
        truncated: true,
        windows: vec![CodexHistoryWindowEntry {
            window_id: "thread:0".to_owned(),
            agent_name: "/root".to_owned(),
            item_count: 3,
        }],
    };
    assert_eq!(
        serde_json::to_value(&windows).expect("window list"),
        json!({
            "agent_name": "/root",
            "window_count": 1,
            "total_windows": 2,
            "truncated": true,
            "windows": [{"window_id": "thread:0", "agent_name": "/root", "item_count": 3}],
        })
    );

    let list = CodexHistoryItemListResult {
        window_id: None,
        item_count: 2,
        returned: 2,
        truncated: false,
        max_chars_per_item: Some(4000),
        items: vec![item_entry(Some("body")), item_entry(None)],
    };
    let value = serde_json::to_value(&list).expect("item list");
    assert_eq!(value["max_chars_per_item"], json!(4000));
    assert_eq!(
        value["items"][0]["truncated_content"],
        json!("body\n[id: msg_1]")
    );
    assert_eq!(value["items"][1]["truncated_content"], Value::Null);

    let search = CodexHistoryItemListResult {
        max_chars_per_item: None,
        items: Vec::new(),
        window_id: Some("thread:0".to_owned()),
        item_count: 0,
        returned: 0,
        truncated: false,
    };
    let value = serde_json::to_value(&search).expect("search list");
    assert!(value.get("max_chars_per_item").is_none());
}

#[test]
fn read_item_result_should_separate_content_images_and_opaque_notes() {
    let attachments = codex_history_item_attachments(
        &json!({
            "type": "message",
            "role": "user",
            "content": [{"type": "input_image", "image_url": "data:image/png;base64,cG5n"}],
        })
        .to_string(),
    );
    let result = CodexHistoryReadItemResult::readable(
        "thread:0".to_owned(),
        "msg_1".to_owned(),
        "message".to_owned(),
        12,
        0,
        4,
        "text".to_owned(),
        attachments,
    );
    assert_eq!(
        serde_json::to_value(&result).expect("read result"),
        json!({
            "window_id": "thread:0",
            "item_id": "msg_1",
            "content_type": "message",
            "readable": true,
            "n_chars": 12,
            "offset_chars": 0,
            "next_offset_chars": 4,
            "content": "text",
            "images": [{"data": "cG5n", "mime_type": "image/png"}],
        })
    );

    let opaque = CodexHistoryReadItemResult::opaque(
        "thread:0".to_owned(),
        "cmp_1".to_owned(),
        "compaction".to_owned(),
        Default::default(),
    );
    let value = serde_json::to_value(&opaque).expect("opaque result");
    assert_eq!(value["readable"], json!(false));
    assert_eq!(value["note"], json!(OPAQUE_ITEM_NOTE));
    assert!(value.get("content").is_none());
    assert!(value.get("images").is_none());

    let not_found =
        CodexHistoryItemNotFoundResult::new("thread:0".to_owned(), "msg_missing".to_owned(), true);
    assert_eq!(
        serde_json::to_value(&not_found).expect("not found"),
        json!({
            "error": "item_not_found",
            "window_id": "thread:0",
            "item_id": "msg_missing",
            "window_found": true,
            "item_found": false,
        })
    );
}

#[test]
fn managed_history_search_declaration_should_keep_notes_encryption() {
    let mut body = json!({
        "model": "gpt-5.4",
        "tools": [
            {
                "type": "namespace",
                "name": "history",
                "description": "private history",
                "tools": [
                    {
                        "type": "function",
                        "name": "search_contents",
                        "description": "search",
                        "strict": false,
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "query": {"type": "string", "encrypted": true, "description": "literal"},
                                "window_id": {"type": ["string", "null"]}
                            },
                            "required": ["query"]
                        }
                    },
                    {
                        "type": "function",
                        "name": "list_items",
                        "parameters": {"type": "object", "properties": {}}
                    }
                ]
            },
            {
                "type": "namespace",
                "name": "notes",
                "tools": [
                    {
                        "type": "function",
                        "name": "search_contents",
                        "parameters": {
                            "type": "object",
                            "properties": {"query": {"type": "string", "encrypted": true}}
                        }
                    },
                    {
                        "type": "function",
                        "name": "write_file",
                        "parameters": {
                            "type": "object",
                            "properties": {"text": {"type": "string", "encrypted": true}}
                        }
                    }
                ]
            }
        ]
    });
    let object = body.as_object_mut().expect("request body object");
    assert!(project_local_history_search_query(object));
    let history = &body["tools"][0]["tools"][0]["parameters"]["properties"]["query"];
    assert_eq!(
        history,
        &json!({"type": "string", "description": "literal"})
    );
    assert!(
        body["tools"][0]["tools"][0]["parameters"]["properties"]
            .get("window_id")
            .is_some(),
        "其它字段保持原样"
    );
    assert_eq!(
        body["tools"][0]["tools"][1]["parameters"],
        json!({"type": "object", "properties": {}}),
        "其它 history 工具不受影响"
    );
    assert_eq!(
        body["tools"][1]["tools"][0]["parameters"]["properties"]["query"]["encrypted"],
        json!(true),
        "notes 检索仍由上游加密"
    );
    assert_eq!(
        body["tools"][1]["tools"][1]["parameters"]["properties"]["text"]["encrypted"],
        json!(true)
    );
    // 已适配的正文再次投影是幂等 no-op
    assert!(!project_local_history_search_query(
        body.as_object_mut().expect("request body object")
    ));
    assert_eq!(
        body["tools"][1]["tools"][0]["parameters"]["properties"]["query"]["encrypted"],
        json!(true)
    );
}

#[test]
fn requests_without_history_search_contents_are_left_untouched() {
    let mut body = json!({"tools": [{"type": "function", "name": "shell"}]});
    assert!(!project_local_history_search_query(
        body.as_object_mut().expect("request body object")
    ));
    let mut without_tools = json!({"model": "gpt-5.4"});
    assert!(!project_local_history_search_query(
        without_tools.as_object_mut().expect("request body object")
    ));
}

#[test]
fn declared_item_id_should_only_come_from_the_official_field() {
    assert_eq!(
        codex_history_declared_item_id(r#"{"type":"message","id":"msg_1"}"#),
        Some("msg_1".to_owned())
    );
    assert_eq!(
        codex_history_declared_item_id(r#"{"type":"compaction","encrypted_content":"opaque"}"#),
        None
    );
    assert_eq!(
        codex_history_declared_item_id(r#"{"type":"message","id":"  "}"#),
        None
    );
    assert_eq!(codex_history_declared_item_id("not json"), None);
}

#[test]
fn item_semantics_should_ignore_only_known_transport_differences() {
    let stored = json!({
        "type": "message",
        "id": "msg_1",
        "status": "completed",
        "role": "assistant",
        "content": [{"type": "output_text", "text": "answer", "annotations": [], "logprobs": null}],
    });
    let replayed = json!({
        "type": "message",
        "role": "assistant",
        "content": [{"type": "input_text", "text": "answer"}],
    });
    // 客户端回填时去掉 id/status 并切换文本分段类型，仍是同一语义对象
    assert_eq!(
        codex_history_item_semantics(&stored.to_string()),
        codex_history_item_semantics(&replayed.to_string())
    );
    // JSON 字段顺序不是语义，规范化后仍相等
    let reordered = r#"{"content":[{"text":"answer","type":"output_text"}],"role":"assistant","type":"message"}"#;
    assert_eq!(
        codex_history_item_semantics(&stored.to_string()),
        codex_history_item_semantics(reordered)
    );
    // 正文变化必须改变身份
    let changed = json!({
        "type": "message",
        "role": "assistant",
        "content": [{"type": "input_text", "text": "different"}],
    });
    assert_ne!(
        codex_history_item_semantics(&stored.to_string()),
        codex_history_item_semantics(&changed.to_string())
    );
}

#[test]
fn item_semantics_should_include_ciphertext_and_image_payloads() {
    let old_cipher = json!({"type": "reasoning", "id": "rs_1", "encrypted_content": "cipher-old"});
    let new_cipher = json!({"type": "reasoning", "id": "rs_1", "encrypted_content": "cipher-new"});
    // 不可读对象的明文投影都是 None，但密文不同就是不同对象
    assert_ne!(
        codex_history_item_semantics(&old_cipher.to_string()),
        codex_history_item_semantics(&new_cipher.to_string())
    );

    let first_image = json!({
        "type": "message",
        "role": "user",
        "content": [
            {"type": "input_text", "text": "see the image"},
            {"type": "input_image", "image_url": "data:image/png;base64,AAAA"},
        ],
    });
    let second_image = json!({
        "type": "message",
        "role": "user",
        "content": [
            {"type": "input_text", "text": "see the image"},
            {"type": "input_image", "image_url": "data:image/png;base64,BBBB"},
        ],
    });
    // 正文相同、附件不同：图片 data URL 参与比较
    assert_ne!(
        codex_history_item_semantics(&first_image.to_string()),
        codex_history_item_semantics(&second_image.to_string())
    );
}
