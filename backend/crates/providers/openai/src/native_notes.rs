//! 原生 notes 的 Provider 侧接线：已发布域映射与官方重放请求
//!
//! 本模块只解释 Codex 自己的 notes wire，会话顺序、日志与重建决策由 Core 负责
//! 重建域把出站 `context.session_id` 换成已发布域代次，其它业务参数与密文原样保留
//! 重放按保存的操作与参数重建官方请求，包含官方写调用固定携带的加密参数标记与截断预算

use bytes::Bytes;
use gateway_core::{
    engine::{
        codex::{CodexAuxiliaryOperation, CodexAuxiliaryRequest},
        codex_history::DEFAULT_HISTORY_OUTPUT_BYTES,
        middleware::MiddlewareHeader,
    },
    error::{ProviderError, ProviderErrorKind},
    provider_ports::{CodexNotesGeneration, CodexNotesOperation, CodexNotesOperationKind},
    upstream::UpstreamSendState,
};
use gateway_protocol::openai::TOOL_OUTPUT_TRUNCATION_POLICY_HEADER;
use serde_json::Value;

/// 官方 notes 写调用固定声明的加密参数标记
const ENCRYPTED_TOOL_ARGUMENTS_HEADER: &str = "x-openai-encrypted-tool-arguments";

/// notes 六端点是否属于本模块处理的会话辅助调用
pub(crate) const fn is_notes_operation(operation: CodexAuxiliaryOperation) -> bool {
    matches!(
        operation,
        CodexAuxiliaryOperation::NotesThreadHint
            | CodexAuxiliaryOperation::NotesListFilesByPrefix
            | CodexAuxiliaryOperation::NotesReadFile
            | CodexAuxiliaryOperation::NotesSearchContents
            | CodexAuxiliaryOperation::NotesAppendToFile
            | CodexAuxiliaryOperation::NotesWriteFile
    )
}

/// 写类端点对应的日志操作类型；读类返回 None
pub(crate) const fn write_kind(
    operation: CodexAuxiliaryOperation,
) -> Option<CodexNotesOperationKind> {
    match operation {
        CodexAuxiliaryOperation::NotesWriteFile => Some(CodexNotesOperationKind::WriteFile),
        CodexAuxiliaryOperation::NotesAppendToFile => Some(CodexNotesOperationKind::AppendToFile),
        _ => None,
    }
}

/// 官方请求正文声明的当前 agent
///
/// 未声明时由调用方按根 agent 解释；这里不解析任何业务参数或密文
pub(crate) fn declared_agent_name(body: &[u8]) -> Option<String> {
    serde_json::from_slice::<Value>(body)
        .ok()?
        .pointer("/context/current_agent_name")?
        .as_str()
        .map(ToOwned::to_owned)
}

/// 出站正文的域映射
///
/// 原生域保持客户端原始字节；重建域只替换 `context.session_id`，
/// 加密参数、agent 路径与行范围等业务参数逐字段保留
pub(crate) fn map_outbound_domain(
    body: &[u8],
    generation: Option<&CodexNotesGeneration>,
) -> Result<Bytes, ProviderError> {
    let Some(generation) = generation else {
        return Ok(Bytes::copy_from_slice(body));
    };
    let mut document = serde_json::from_slice::<Value>(body).map_err(|_| notes_body_error())?;
    let context = document
        .get_mut("context")
        .and_then(Value::as_object_mut)
        .ok_or_else(notes_body_error)?;
    context.insert(
        "session_id".to_owned(),
        Value::String(generation.expose_to_store().to_owned()),
    );
    serde_json::to_vec(&document)
        .map(Bytes::from)
        .map_err(|_| notes_body_error())
}

/// 按保存的操作重建一次重放请求
///
/// 目标域代次写入新的 `context.session_id`；官方 notes 写调用总带加密参数标记与
/// 截断预算，重放不能只有保存的正文而漏掉这两个头
///
/// # Errors
///
/// 保存的正文不是 JSON 对象或缺少 `context`
pub(crate) fn replay_request(
    generation: &CodexNotesGeneration,
    operation: &CodexNotesOperation,
) -> Result<CodexAuxiliaryRequest, ProviderError> {
    Ok(CodexAuxiliaryRequest {
        operation: replay_endpoint(operation.kind),
        query: None,
        headers: replay_headers(),
        body: map_outbound_domain(operation.request.as_bytes(), Some(generation))?,
    })
}

const fn replay_endpoint(kind: CodexNotesOperationKind) -> CodexAuxiliaryOperation {
    match kind {
        CodexNotesOperationKind::WriteFile => CodexAuxiliaryOperation::NotesWriteFile,
        CodexNotesOperationKind::AppendToFile => CodexAuxiliaryOperation::NotesAppendToFile,
    }
}

/// 官方写调用固定携带的两个头
///
/// 写响应只回写入结果，截断预算不改变业务参数；缺省取官方模型默认预算
fn replay_headers() -> Vec<MiddlewareHeader> {
    // 官方 wire 值把模式与上限放在同一 JSON 对象里
    let policy = format!(r#"{{"mode":"bytes","limit":{DEFAULT_HISTORY_OUTPUT_BYTES}}}"#);
    vec![
        MiddlewareHeader::new(ENCRYPTED_TOOL_ARGUMENTS_HEADER, Bytes::from_static(b"true")),
        MiddlewareHeader::new(TOOL_OUTPUT_TRUNCATION_POLICY_HEADER, Bytes::from(policy)),
    ]
}

/// 重建域要求正文能按官方形状改写；不能改写时本次请求未发送
pub(crate) fn notes_body_error() -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::InvalidRequest,
        UpstreamSendState::NotSent,
    )
    .with_retry_prohibited()
}
