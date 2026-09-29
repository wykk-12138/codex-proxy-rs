use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;

use crate::{
    model::{AdminErrorKind, Revision},
    ports::{
        plugins::{PluginPackageInspector, PluginStore},
        store::AdminStoreErrorKind,
        system::{
            SystemOperationError, SystemOperationErrorKind, SystemUpdateCandidate,
            SystemUpdatePreflight,
        },
    },
};

use super::plugins::official::update_compatibility;

/// 只读取启用实例及其固定包体，检查目标宿主合同；不准备或执行插件。
pub(crate) struct PluginSystemUpdatePreflight {
    store: Arc<dyn PluginStore>,
    inspector: Arc<dyn PluginPackageInspector>,
}

impl PluginSystemUpdatePreflight {
    pub(crate) fn new(
        store: Arc<dyn PluginStore>,
        inspector: Arc<dyn PluginPackageInspector>,
    ) -> Self {
        Self { store, inspector }
    }
}

#[async_trait]
impl SystemUpdatePreflight for PluginSystemUpdatePreflight {
    async fn validate(
        &self,
        candidate: SystemUpdateCandidate,
    ) -> Result<Revision, SystemOperationError> {
        let target = semver::Version::parse(candidate.target_version.trim_start_matches('v'))
            .map_err(|_| invalid("目标网关版本不合法"))?;
        let compatibility = update_compatibility(
            candidate.release_manifest.as_ref(),
            candidate.target_version.trim_start_matches('v'),
        )
        .map_err(|_| invalid("目标发行物缺少有效的插件兼容声明"))?;
        let snapshot = self.store.load_instances().await.map_err(map_store_error)?;
        let mut inspected = BTreeMap::new();
        for instance in snapshot
            .instances
            .iter()
            .filter(|instance| instance.enabled)
        {
            let requirements = if let Some(requirements) = inspected.get(&instance.artifact_sha256)
            {
                requirements
            } else {
                let artifact = self
                    .store
                    .load_artifact(&instance.artifact_sha256)
                    .await
                    .map_err(map_store_error)?;
                let requirements = self
                    .inspector
                    .compatibility(artifact.archive, instance.artifact_sha256.clone())
                    .await
                    .map_err(map_inspection_error)?;
                inspected.insert(instance.artifact_sha256.clone(), requirements);
                inspected
                    .get(&instance.artifact_sha256)
                    .ok_or_else(|| internal("插件兼容性检查结果不可用"))?
            };
            let host_versions = semver::VersionReq::parse(&requirements.host_version)
                .map_err(|_| incompatible(&instance.id))?;
            if !host_versions.matches(&target)
                || !compatibility
                    .manifest_schema_versions
                    .contains(&requirements.manifest_schema_version)
                || !compatibility
                    .protocol_versions
                    .contains(&requirements.protocol_version)
                || requirements
                    .capabilities
                    .iter()
                    .any(|(capability, version)| {
                        !compatibility.supports_capability(capability, *version)
                    })
            {
                return Err(incompatible(&instance.id));
            }
        }
        Ok(snapshot.config_revision)
    }

    async fn confirm_revision(&self, expected: Revision) -> Result<(), SystemOperationError> {
        let current = self
            .store
            .load_instances()
            .await
            .map_err(map_store_error)?
            .config_revision;
        if current != expected {
            return Err(conflict("插件配置已变化，请重新执行系统更新"));
        }
        Ok(())
    }
}

fn map_store_error(error: crate::ports::store::AdminStoreError) -> SystemOperationError {
    match error.kind() {
        AdminStoreErrorKind::StaleRevision
        | AdminStoreErrorKind::DuplicateName
        | AdminStoreErrorKind::Conflict => conflict("插件配置已变化，请重新执行系统更新"),
        AdminStoreErrorKind::Invalid | AdminStoreErrorKind::NotFound => {
            conflict("启用插件的制品不完整，无法执行系统更新")
        }
        AdminStoreErrorKind::Unavailable => internal("插件配置暂不可读取"),
    }
}

fn map_inspection_error(error: crate::model::AdminError) -> SystemOperationError {
    match error.kind() {
        AdminErrorKind::Invalid | AdminErrorKind::Conflict | AdminErrorKind::NotFound => {
            conflict("启用插件的制品无法通过兼容性检查")
        }
        _ => internal("插件兼容性检查暂不可用"),
    }
}

fn incompatible(instance_id: &str) -> SystemOperationError {
    conflict(format!("启用插件实例 {instance_id} 与目标网关版本不兼容"))
}

fn invalid(message: impl Into<String>) -> SystemOperationError {
    SystemOperationError::new(SystemOperationErrorKind::Invalid, message)
}

fn conflict(message: impl Into<String>) -> SystemOperationError {
    SystemOperationError::new(SystemOperationErrorKind::Conflict, message)
}

fn internal(message: impl Into<String>) -> SystemOperationError {
    SystemOperationError::new(SystemOperationErrorKind::Internal, message)
}
