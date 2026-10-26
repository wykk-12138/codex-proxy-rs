//! managed notes 写交付的受监督 owner
//!
//! 调用方只负责准备：取得容量、取得会话互斥、解析落点与账号、构造可发送请求。
//! 交接成功后 append、start、发送、完整读取响应与落库全部由本 owner 持有，
//! 调用方取消或 future 被丢弃只停止等待结果，不再丢失真实发送与持久观察
//!
//! 本模块不提供恢复扫描、重试队列或未知副作用重放：结果是 `Unknown` 时如实保存，
//! 并继续阻止自动迁移

use std::{
    future::Future,
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, SystemTime},
};

use futures::{StreamExt as _, future::BoxFuture, lock::OwnedMutexGuard};
use gateway_core::{
    engine::{codex_notes::CodexNotesService, middleware::MiddlewareResponse},
    error::{ProviderDiagnostic, ProviderError, ProviderErrorKind},
    lifecycle::{CancellationToken, Deadline},
    policy::ClientApiKeyId,
    provider_ports::{
        CodexNotesCompletion, CodexNotesOperationKind, CodexNotesOperationStatus,
        CodexNotesReplica, ProviderSessionAffinityKey,
    },
    task::{
        DaemonRestartPolicy, DaemonTask, WorkerDaemonContext, WorkerDefinitionError,
        WorkerTaskError,
    },
    upstream::UpstreamSendState,
};
use tokio::sync::{Semaphore, mpsc, oneshot};

use super::{
    failure::map_client_error,
    native::{buffered_response, forwardable_headers, notes_coordination_error},
};
use crate::{
    credential::{CodexCredentialLease, NotesLeaseLifecycle},
    transport::{CodexClientError, native::PreparedNativeAuxiliary},
};

/// 同时在途的 managed notes 写工作上限
///
/// 容量覆盖准备、交接和执行，没有独立的额外排队额度；数量上限不等于正文内存上限
pub(crate) const NOTES_DELIVERY_CAPACITY: usize = 32;

/// 官方 notes 工具请求的 35 秒总时限，覆盖响应头与完整正文读取
const NOTES_UPSTREAM_TIMEOUT_SECS: u64 = 35;
const NOTES_UPSTREAM_TIMEOUT: Duration = Duration::from_secs(NOTES_UPSTREAM_TIMEOUT_SECS);

/// 单次持久等待的硬上限
///
/// 与 Store 的 statement 预算一致；超时只说明提交结果不明，不能据此判定未提交
const NOTES_PERSIST_TIMEOUT_SECS: u64 = 30;
const NOTES_PERSIST_TIMEOUT: Duration = Duration::from_secs(NOTES_PERSIST_TIMEOUT_SECS);

/// 交接后交付本身需要的净时限：一次有界上游发送加上四次有界持久等待
///
/// 四次对应 append、start、complete 与一次读回或结果不明补记。它是交接前复验的
/// 覆盖下限，也是交付内部各步骤预算之和，不再作为租约申请的完整期限
pub const NOTES_DELIVERY_LEASE_BUDGET: Duration =
    Duration::from_secs(NOTES_UPSTREAM_TIMEOUT_SECS + NOTES_PERSIST_TIMEOUT_SECS * 4);

/// 取得账号租约之后、交接之前允许消耗的准备窗口
///
/// 覆盖该窗口内全部异步步骤：租约取得、认领、身份投影（PAT 会真实等待 whoami）
/// 与落点复读。它被 `tokio::time::timeout` 真实强制，超时在写意图与发送之前失败，
/// 因此不会把准备耗时转嫁给交付时限
pub(crate) const NOTES_PREPARE_TIMEOUT: Duration =
    Duration::from_secs(NOTES_PERSIST_TIMEOUT_SECS * 2);

/// 申请租约时使用的完整期限：有界准备窗口 + 交接后完整交付预算
///
/// 两个分量都有独立强制上限：准备由本模块的 timeout 强制，交付由 35s/30s 预算强制。
/// 因此“准备 + 交付”不会超过申请期限，交接前还会用同一期限复验剩余量
pub(crate) const NOTES_LEASE_REQUEST_BUDGET: Duration =
    NOTES_PREPARE_TIMEOUT.saturating_add(NOTES_DELIVERY_LEASE_BUDGET);

/// 一次交接前取得的容量与交付生命周期
///
/// `permit` 一直持有到工作结束；`lifecycle` 的取消与截止属于交付，而不是调用方
pub(crate) struct NotesDeliveryEntry {
    pub(crate) permit: tokio::sync::OwnedSemaphorePermit,
    pub(crate) lifecycle: NotesDeliveryLifecycle,
}

impl NotesDeliveryEntry {
    /// 账号租约续期必须跟随交付生命周期
    pub(crate) fn lease(&self) -> NotesLeaseLifecycle {
        self.lifecycle.lease()
    }
}

/// 交付工作的生命周期
///
/// 只携带交付自己的取消信号；租约期限由 `lease()` 现场申请，并由交接前复验确认
/// 仍覆盖完整交付
pub(crate) struct NotesDeliveryLifecycle {
    cancellation: CancellationToken,
}

impl NotesDeliveryLifecycle {
    /// 只有原本取得账号租约的 notes 写路径使用这里的事实
    ///
    /// 申请期限 = 有界准备窗口 + 完整交付预算，计时点就是取得租约这件事本身
    pub(crate) fn lease(&self) -> NotesLeaseLifecycle {
        NotesLeaseLifecycle {
            deadline: Deadline::from(SystemTime::now() + NOTES_LEASE_REQUEST_BUDGET),
            cancellation: self.cancellation.clone(),
        }
    }
}

/// 交接前复验：同一租约期限的剩余量必须覆盖完整交付预算
///
/// 剩余不足时不写意图、不发送——宁可让本次写保护性失败，也不在租约已过期后交付
pub(crate) fn lease_covers_delivery(lease: &NotesLeaseLifecycle) -> bool {
    lease
        .deadline
        .remaining()
        .is_some_and(|remaining| remaining >= NOTES_DELIVERY_LEASE_BUDGET)
}

/// 已准备完成、所有权已交给 owner 的一次写入
pub(crate) struct NotesDeliveryWork {
    pub(crate) completion: oneshot::Sender<Result<MiddlewareResponse, ProviderError>>,
    pub(crate) permit: tokio::sync::OwnedSemaphorePermit,
    pub(crate) lifecycle: NotesDeliveryLifecycle,
    pub(crate) service: Arc<CodexNotesService>,
    /// 会话互斥在交给 owner 后仍必须覆盖发送与落库
    pub(crate) guard: OwnedMutexGuard<()>,
    pub(crate) key_id: ClientApiKeyId,
    pub(crate) session_key: ProviderSessionAffinityKey,
    pub(crate) replica: CodexNotesReplica,
    pub(crate) kind: CodexNotesOperationKind,
    pub(crate) agent_name: String,
    pub(crate) stored: String,
    pub(crate) prepared: PreparedNativeAuxiliary,
    /// 首绑取得的账号租约随交付释放，不随调用方提前结束
    pub(crate) lease: Option<Box<CodexCredentialLease>>,
}

/// 受 Host 监督的 notes 交付 owner
pub(crate) struct CodexNotesDelivery {
    handoff: StdMutex<Handoff>,
    permits: Arc<Semaphore>,
    receiver: StdMutex<Option<mpsc::Receiver<NotesDeliveryWork>>>,
    /// owner 生命周期：Host 关闭时取消，调用方取消不影响它
    lifecycle: CancellationToken,
}

enum Handoff {
    Open(mpsc::Sender<NotesDeliveryWork>),
    Closed,
}

impl CodexNotesDelivery {
    pub(crate) fn new() -> Arc<Self> {
        let (sender, receiver) = mpsc::channel(NOTES_DELIVERY_CAPACITY);
        Arc::new(Self {
            handoff: StdMutex::new(Handoff::Open(sender)),
            permits: Arc::new(Semaphore::new(NOTES_DELIVERY_CAPACITY)),
            receiver: StdMutex::new(Some(receiver)),
            lifecycle: CancellationToken::new(),
        })
    }

    /// 在准备之前取得容量与交付生命周期
    ///
    /// 没有容量时明确拒绝：不写日志、不发送，也不创建后台等待者
    pub(crate) fn begin(&self) -> Result<NotesDeliveryEntry, ProviderError> {
        if matches!(
            *self
                .handoff
                .lock()
                .unwrap_or_else(|error| error.into_inner()),
            Handoff::Closed
        ) {
            return Err(delivery_closed());
        }
        let permit = Arc::clone(&self.permits)
            .try_acquire_owned()
            .map_err(|_| delivery_capacity_error())?;
        Ok(NotesDeliveryEntry {
            permit,
            lifecycle: NotesDeliveryLifecycle {
                cancellation: self.lifecycle.child_token(),
            },
        })
    }

    /// 交接已准备的工作；成功即所有权转移到 owner
    ///
    /// 本方法是同步的：接收与关闭在同一互斥内裁决，不存在先执行后登记的空隙
    pub(crate) fn submit(&self, work: NotesDeliveryWork) -> Result<(), ProviderError> {
        let mut handoff = self
            .handoff
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Handoff::Open(sender) = &*handoff {
            // 通道容量不大于 permit 总数，持有 permit 的交接一定能立即入队；
            // 队满只拒绝本次，不牵连已在途的工作
            match sender.try_send(work) {
                Ok(()) => return Ok(()),
                Err(mpsc::error::TrySendError::Full(_)) => return Err(delivery_capacity_error()),
                Err(mpsc::error::TrySendError::Closed(_)) => {}
            }
        }
        // 接收端已经结束：关闭 gate，后续交接都在准备阶段明确失败
        *handoff = Handoff::Closed;
        self.lifecycle.cancel();
        Err(delivery_closed())
    }

    /// 关闭接收 gate 并结束交付生命周期
    fn close(&self) {
        *self
            .handoff
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Handoff::Closed;
        self.lifecycle.cancel();
    }

    /// Host 长驻任务入口：接收、执行并观察全部已交接工作
    async fn run(&self, context: WorkerDaemonContext) {
        let Some(mut receiver) = self
            .receiver
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        else {
            // 接收端已被上一次运行取走：关闭 gate，工作不会进入无人读取的通道
            // 这里不重建接收端、不重放已交接工作；managed notes 写要到进程重启才能恢复
            tracing::error!(
                "Codex notes delivery receiver is already gone; managed notes writes stay \
                 closed until the gateway process restarts"
            );
            self.close();
            context.cancellation().cancelled().await;
            return;
        };
        let mut running = futures::stream::FuturesUnordered::new();
        let closed = loop {
            tokio::select! {
                biased;
                () = context.cancellation().cancelled() => break true,
                item = receiver.recv() => match item {
                    Some(work) => {
                        running.push(work.deliver());
                    }
                    None => break false,
                },
                Some(()) = running.next(), if !running.is_empty() => {}
            }
        };
        // 关闭后不再接收新工作，也不再产生新的持久意图或上游发送
        self.close();
        if closed {
            receiver.close();
            // 通道里尚未开始的工作直接拒绝：丢弃即释放容量、互斥与租约
            while receiver.try_recv().is_ok() {}
        }
        // 已在途工作继续观察真实结果，受各自 35 秒上限与 Host 剩余关闭时间限制
        while running.next().await.is_some() {}
    }
}

/// 交付 owner 的 Host 任务
///
/// 只由组装根放进任务计划；它接管真实 notes 写，不做恢复扫描或重放
pub struct CodexNotesDeliveryTask {
    delivery: Arc<CodexNotesDelivery>,
}

impl CodexNotesDeliveryTask {
    pub(crate) const fn new(delivery: Arc<CodexNotesDelivery>) -> Self {
        Self { delivery }
    }

    /// 当前可用的交付容量
    ///
    /// 只用于装配自检与诊断观测，不改变准入语义
    #[doc(hidden)]
    #[must_use]
    pub fn available_capacity(&self) -> usize {
        self.delivery.permits.available_permits()
    }
}

impl DaemonTask for CodexNotesDeliveryTask {
    fn run(&self, context: WorkerDaemonContext) -> BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            self.delivery.run(context).await;
            Ok(())
        })
    }
}

impl super::CodexProvider {
    /// 交付 owner 的 Host 任务；未装配 notes 时为 None
    #[must_use]
    pub fn notes_delivery_task(&self) -> Option<CodexNotesDeliveryTask> {
        self.notes_delivery
            .as_ref()
            .map(|delivery| CodexNotesDeliveryTask::new(Arc::clone(delivery)))
    }
}

/// 交付任务的固定身份与重启退避
pub(super) const NOTES_DELIVERY_WORKER_OWNER: &str = "openai-notes-delivery";
const NOTES_DELIVERY_RESTART_INITIAL: Duration = Duration::from_secs(1);
const NOTES_DELIVERY_RESTART_MAXIMUM: Duration = Duration::from_secs(60);

#[doc(hidden)]
pub fn notes_delivery_registration(
    task: CodexNotesDeliveryTask,
) -> Result<gateway_core::task::WorkerRegistration, WorkerDefinitionError> {
    gateway_core::task::WorkerRegistration::try_new(
        gateway_core::task::WorkerId::try_new(
            gateway_core::task::WorkerKind::CodexNotesDelivery,
            NOTES_DELIVERY_WORKER_OWNER,
        )?,
        gateway_core::task::WorkerRunnable::Daemon {
            restart: DaemonRestartPolicy::try_new(
                NOTES_DELIVERY_RESTART_INITIAL,
                NOTES_DELIVERY_RESTART_MAXIMUM,
            )?,
            task: Box::new(task),
        },
    )
}

impl NotesDeliveryWork {
    /// 交付一次已准备好的写入，并把结果交回仍存在的调用方
    async fn deliver(self) {
        let Self {
            completion,
            permit: _permit,
            lifecycle,
            service,
            guard: _guard,
            key_id,
            session_key,
            replica,
            kind,
            agent_name,
            stored,
            prepared,
            lease: _lease,
        } = self;
        let result = deliver_prepared(
            &service,
            &lifecycle,
            &key_id,
            &session_key,
            replica,
            kind,
            agent_name,
            stored,
            prepared,
        )
        .await;
        // 接收端已经消失时丢弃响应；观察本身已经落库
        let _ = completion.send(result);
    }
}

/// 一次写入的状态机：先落意图，再开始，再发送一次并保存完整响应
#[allow(clippy::too_many_arguments)]
async fn deliver_prepared(
    service: &CodexNotesService,
    lifecycle: &NotesDeliveryLifecycle,
    key_id: &ClientApiKeyId,
    session_key: &ProviderSessionAffinityKey,
    replica: CodexNotesReplica,
    kind: CodexNotesOperationKind,
    agent_name: String,
    stored: String,
    prepared: PreparedNativeAuxiliary,
) -> Result<MiddlewareResponse, ProviderError> {
    // 关闭先到时不产生新意图：工作已受监督，但字节与日志都还没有产生
    if lifecycle.cancellation.is_cancelled() {
        return Err(delivery_closed());
    }
    let operation = persist(
        service.append_operation(key_id, session_key, replica, kind, agent_name, stored),
        UpstreamSendState::NotSent,
    )
    .await?;
    let seq = operation.seq;
    // 没有开始事实就不允许字节离开网关
    if let Err(error) = persist(
        service.start_operation(key_id, session_key, seq),
        UpstreamSendState::NotSent,
    )
    .await
    {
        return Err(settle_after_start_failure(service, key_id, session_key, seq, error).await);
    }
    // 已开始但取消先到：字节从未离开网关，按可证明状态收尾
    if lifecycle.cancellation.is_cancelled() {
        settle_proven_unsent(service, key_id, session_key, seq).await;
        return Err(delivery_closed());
    }
    let upstream_deadline = tokio::time::Instant::now() + NOTES_UPSTREAM_TIMEOUT;
    let response = match tokio::time::timeout_at(upstream_deadline, prepared.execute()).await {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => {
            // 字节可能已经离开网关；顺序位置保留到显式处理为止
            record_unknown(service, key_id, session_key, seq, None).await;
            return Err(map_client_error(error, UpstreamSendState::Ambiguous, false).error);
        }
        Err(_) => {
            record_unknown(service, key_id, session_key, seq, None).await;
            return Err(upstream_timeout());
        }
    };
    let status = response.status();
    let headers = forwardable_headers(&response);
    let bytes = match tokio::time::timeout_at(upstream_deadline, response.bytes()).await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(error)) => {
            record_unknown(service, key_id, session_key, seq, Some(status.as_u16())).await;
            return Err(map_client_error(
                CodexClientError::HttpJson(error),
                UpstreamSendState::Ambiguous,
                false,
            )
            .error);
        }
        Err(_) => {
            record_unknown(service, key_id, session_key, seq, Some(status.as_u16())).await;
            return Err(upstream_timeout());
        }
    };
    let Ok(response_body) = std::str::from_utf8(&bytes) else {
        record_unknown(service, key_id, session_key, seq, Some(status.as_u16())).await;
        return Err(notes_body_error());
    };
    // 完整响应先落库再交给客户端；非 2xx 记录真实状态，不解读加密业务结果
    let recorded = if status.is_success() {
        CodexNotesOperationStatus::Responded
    } else {
        CodexNotesOperationStatus::Failed
    };
    let completion = CodexNotesCompletion {
        key_id: key_id.clone(),
        session_key: session_key.clone(),
        seq,
        status: recorded,
        response: Some(response_body.to_owned()),
        response_status: Some(status.as_u16()),
    };
    if let Err(error) = persist(
        service.complete_operation(&completion),
        UpstreamSendState::Sent,
    )
    .await
    {
        // 提交结果可能已经落库；不补写 Failed/Unknown 覆盖可能已提交的 Responded
        tracing::warn!(
            seq,
            error = %error,
            "Codex notes operation completion could not be persisted"
        );
        return Err(error);
    }
    Ok(buffered_response(status.as_u16(), headers, bytes))
}

/// `start` 失败后锁内读回：区分本 owner 尚未发送的开始事实与结果不明的历史记录
///
/// 该调用点上 seq 由本次 append 刚分配、会话互斥由本 owner 持有到交付结束、
/// 且 `prepared.execute` 尚未调用，因此这里的 `Unsent` 与 `InFlight` 都证明字节
/// 没有离开网关；按已有的 `Unsent`/`InFlight → Failed` 转换收尾，不新建状态
/// 或恢复机制。这一证明只适用于本调用点：进程重启遗留的 `InFlight` 与已经
/// 开始发送后的结果不明都不能据此改判
///
/// `Unknown`、终态与读回失败保持原状：前者可能来自真实发送或已提交的其它观察，
/// 后者无法确认日志记录与本次意图一致；三者都不改写，读回失败时返回原 `start` 失败
async fn settle_after_start_failure(
    service: &CodexNotesService,
    key_id: &ClientApiKeyId,
    session_key: &ProviderSessionAffinityKey,
    seq: u64,
    error: ProviderError,
) -> ProviderError {
    let Ok(operations) = persist(
        service.operations(key_id, session_key),
        UpstreamSendState::NotSent,
    )
    .await
    else {
        // 读回本身失败：日志记录不可核对，保留原状并如实返回原 start 失败
        return error;
    };
    let observed = operations
        .iter()
        .find(|operation| operation.seq == seq)
        .map(|operation| operation.status);
    if matches!(
        observed,
        Some(CodexNotesOperationStatus::Unsent | CodexNotesOperationStatus::InFlight)
    ) {
        settle_proven_unsent(service, key_id, session_key, seq).await;
        return error;
    }
    notes_state_conflict()
}

/// 已确认字节未离开网关时按 `Unsent`/`InFlight → Failed` 收尾
///
/// 只用于本 owner 能证明尚未发送的调用点；收尾失败只记日志，不宣称已终结
async fn settle_proven_unsent(
    service: &CodexNotesService,
    key_id: &ClientApiKeyId,
    session_key: &ProviderSessionAffinityKey,
    seq: u64,
) {
    let completion = CodexNotesCompletion {
        key_id: key_id.clone(),
        session_key: session_key.clone(),
        seq,
        status: CodexNotesOperationStatus::Failed,
        response: None,
        response_status: None,
    };
    if let Err(error) = persist(
        service.complete_operation(&completion),
        UpstreamSendState::NotSent,
    )
    .await
    {
        tracing::warn!(
            seq,
            error = %error,
            "Codex notes unsent operation could not be settled"
        );
    }
}

/// 发送异常时如实记录结果不明
async fn record_unknown(
    service: &CodexNotesService,
    key_id: &ClientApiKeyId,
    session_key: &ProviderSessionAffinityKey,
    seq: u64,
    response_status: Option<u16>,
) {
    let completion = CodexNotesCompletion {
        key_id: key_id.clone(),
        session_key: session_key.clone(),
        seq,
        status: CodexNotesOperationStatus::Unknown,
        response: None,
        response_status,
    };
    if let Err(error) = persist(
        service.complete_operation(&completion),
        UpstreamSendState::Ambiguous,
    )
    .await
    {
        // 记录失败只补日志，不覆盖原发送错误；该 seq 随后会阻止发布
        tracing::warn!(
            seq,
            error = %error,
            "Codex notes operation outcome could not be persisted"
        );
    }
}

/// 单次持久等待的硬上限
///
/// 超时只说明提交结果不明；调用方不得据此改写操作状态或重放
async fn persist<T>(
    future: impl Future<Output = Result<T, gateway_core::engine::codex_notes::CodexNotesError>>,
    send_state: UpstreamSendState,
) -> Result<T, ProviderError> {
    match tokio::time::timeout(NOTES_PERSIST_TIMEOUT, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(notes_coordination_error(error, send_state)),
        Err(_) => Err(provider_error(ProviderErrorKind::Unavailable, send_state)
            .with_retry_prohibited()
            .with_diagnostic(
                ProviderDiagnostic::new("Codex notes persistence did not complete in time")
                    .with_classification("notes", "persist_timeout"),
            )),
    }
}

fn provider_error(kind: ProviderErrorKind, send_state: UpstreamSendState) -> ProviderError {
    ProviderError::new(kind, send_state)
}

/// 容量耗尽时在发送与意图之前明确拒绝
fn delivery_capacity_error() -> ProviderError {
    ProviderError::new(ProviderErrorKind::Unavailable, UpstreamSendState::NotSent)
        .with_retry_prohibited()
        .with_diagnostic(
            ProviderDiagnostic::new("Codex notes delivery capacity is exhausted")
                .with_classification("notes", "delivery_capacity"),
        )
}

/// owner 已关闭或工作未能交接
fn delivery_closed() -> ProviderError {
    ProviderError::new(ProviderErrorKind::Unavailable, UpstreamSendState::NotSent)
        .with_retry_prohibited()
        .with_diagnostic(
            ProviderDiagnostic::new("Codex notes delivery owner is closed")
                .with_classification("notes", "delivery_closed"),
        )
}

/// 上游整体预算耗尽：可能已经收到响应头，结果不明
fn upstream_timeout() -> ProviderError {
    ProviderError::new(ProviderErrorKind::Timeout, UpstreamSendState::Ambiguous)
        .with_retry_prohibited()
        .with_diagnostic(
            ProviderDiagnostic::new("Codex notes request did not complete in time")
                .with_classification("notes", "upstream_timeout"),
        )
}

/// 读回的事实不是“未发送”，不能覆盖
fn notes_state_conflict() -> ProviderError {
    ProviderError::new(ProviderErrorKind::Unavailable, UpstreamSendState::Ambiguous)
        .with_retry_prohibited()
        .with_diagnostic(
            ProviderDiagnostic::new("Codex notes operation state conflicts with this call")
                .with_classification("notes", "state_conflict"),
        )
        .with_source(std::io::Error::other(
            "Codex notes operation has no Unsent state",
        ))
}

fn notes_body_error() -> ProviderError {
    crate::native_notes::notes_body_error()
}
