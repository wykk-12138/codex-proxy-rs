//! 请求准入租约及费用结算的释放边界

use super::failure::record_resource_failure;
use crate::{
    engine::{
        ModelRequestId,
        admission::ClientAdmissionPort,
        budget::{ClientBudgetCharge, ClientBudgetPort},
    },
    lifecycle::LeaseGuard,
    policy::ClientApiKeyId,
};
use std::sync::Arc;

/// 辅助 API 共享 RPM/并发和排队，但不进入模型金额门禁
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum AdmissionPurpose {
    Model,
    Auxiliary,
}

pub(super) struct AdmissionLease {
    pub(super) renewal: Option<Box<dyn LeaseGuard>>,
    pub(super) armed: bool,
    pub(super) port: Arc<dyn ClientAdmissionPort>,
    pub(super) diagnostics: Arc<dyn crate::diagnostics::OperationalDiagnostics>,
    pub(super) client_api_key_id: ClientApiKeyId,
    pub(super) model_request_id: ModelRequestId,
}

pub(super) async fn settle_budget(
    port: &dyn ClientBudgetPort,
    charge: ClientBudgetCharge,
    diagnostics: &dyn crate::diagnostics::OperationalDiagnostics,
) {
    let request_id = charge.request_id.clone();
    if let Err(error) = port.settle(charge).await {
        record_resource_failure(
            diagnostics,
            &request_id,
            "settle_client_budget",
            "Client budget settlement failed; storage will retry on the next request",
            error,
        )
        .await;
    }
}

impl AdmissionLease {
    pub(super) async fn release(mut self) {
        self.renewal.take();
        if let Err(error) = self
            .port
            .release(&self.client_api_key_id, &self.model_request_id)
            .await
        {
            record_resource_failure(
                self.diagnostics.as_ref(),
                &self.model_request_id,
                "release_client_admission",
                "Client admission release failed; lease TTL remains active",
                error,
            )
            .await;
        }
        self.armed = false;
    }
}

impl Drop for AdmissionLease {
    fn drop(&mut self) {
        self.renewal.take();
        if self.armed {
            self.port
                .abandon(&self.client_api_key_id, &self.model_request_id);
        }
    }
}
