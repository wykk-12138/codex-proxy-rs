use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use gateway_admin::{
    model::{
        AdminError, MutationContext, Revision,
        plugins::{
            InspectedPluginArtifact, InstalledPluginArtifact, PluginArtifactMetadata,
            PluginArtifactMutation, PluginCompatibilityRequirements, PluginSource,
            distribution::{PluginSourceBinding, SourceCredential, SourceCredentialInfo},
            instances::{PluginInstance, PluginInstanceMutation, PluginInstanceSnapshot},
        },
        system::{
            SystemOperationAccepted, SystemOperationState, SystemOperationStatus,
            SystemUpdateDetail, SystemUpdateStatus, SystemVersion,
        },
    },
    ports::{
        plugins::{PluginPackageInspector, PluginStore},
        store::AdminStoreResult,
        system::{
            SystemOperationError, SystemOperations, SystemUpdateCandidate, SystemUpdateEventStream,
            SystemUpdatePreflight,
        },
    },
};

struct Fixture {
    revisions: std::sync::Mutex<Vec<Revision>>,
    requirements: PluginCompatibilityRequirements,
}

impl Fixture {
    fn new(requirements: PluginCompatibilityRequirements, revisions: &[u64]) -> Arc<Self> {
        Arc::new(Self {
            revisions: std::sync::Mutex::new(
                revisions
                    .iter()
                    .map(|revision| Revision::new(*revision).expect("revision"))
                    .collect(),
            ),
            requirements,
        })
    }

    fn next_revision(&self) -> Revision {
        let mut revisions = self.revisions.lock().expect("revisions");
        if revisions.len() > 1 {
            revisions.remove(0)
        } else {
            *revisions.first().expect("fixture revision")
        }
    }
}

#[async_trait]
impl PluginPackageInspector for Fixture {
    async fn inspect(
        &self,
        _: Arc<[u8]>,
        _: Option<String>,
    ) -> Result<InspectedPluginArtifact, AdminError> {
        unreachable!()
    }

    async fn compatibility(
        &self,
        _: Arc<[u8]>,
        expected_sha256: String,
    ) -> Result<PluginCompatibilityRequirements, AdminError> {
        assert_eq!(expected_sha256, "a".repeat(64));
        Ok(self.requirements.clone())
    }
}

#[async_trait]
impl PluginStore for Fixture {
    async fn management_target_is_current(
        &self,
        target: &gateway_admin::model::plugins::management::PluginManagementTarget,
    ) -> AdminStoreResult<bool> {
        Ok(self
            .load_instances()
            .await?
            .instances
            .iter()
            .any(|instance| {
                instance.enabled
                    && instance.trusted_process
                    && instance.id == target.instance_id
                    && instance.artifact_sha256 == target.artifact_sha256
                    && instance.revision.get() == target.revision
            }))
    }
    async fn load_instances(&self) -> AdminStoreResult<PluginInstanceSnapshot> {
        Ok(PluginInstanceSnapshot {
            config_revision: self.next_revision(),
            instances: vec![PluginInstance {
                id: "enabled-fixture".into(),
                name: "Enabled fixture".into(),
                artifact_sha256: "a".repeat(64),
                enabled: true,
                trusted_process: true,
                configuration: serde_json::json!({}),
                secrets: BTreeMap::new(),

                bindings: Vec::new(),
                revision: Revision::new(1).expect("revision"),
            }],
        })
    }

    async fn load_artifact(&self, digest: &str) -> AdminStoreResult<InspectedPluginArtifact> {
        assert_eq!(digest, "a".repeat(64));
        Ok(InspectedPluginArtifact {
            metadata: PluginArtifactMetadata {
                plugin_id: "test.fixture".into(),
                version: "1.0.0".into(),
                name: "fixture".into(),
                display_name: "Fixture".into(),
                publisher: "test".into(),
                author: Some("project".into()),
                description: "fixture".into(),
                license: "MIT".into(),
                sha256: digest.into(),
                platforms: Vec::new(),
                icon: None,
                contributes: BTreeMap::new(),

                configuration_schema: serde_json::json!({}),
                secret_fields: Vec::new(),
                state_namespaces: Vec::new(),
            },
            archive: Arc::from([1_u8]),
        })
    }

    async fn save_instance(
        &self,
        _: PluginInstance,
        _: Revision,
        _: &MutationContext,
    ) -> AdminStoreResult<PluginInstanceMutation> {
        unreachable!()
    }

    async fn delete_instance(
        &self,
        _: &str,
        _: Revision,
        _: &MutationContext,
    ) -> AdminStoreResult<Revision> {
        unreachable!()
    }

    async fn list_update_sources(&self) -> AdminStoreResult<Vec<PluginSourceBinding>> {
        unreachable!()
    }

    async fn change_update_source(
        &self,
        _: PluginSourceBinding,
        _: &MutationContext,
    ) -> AdminStoreResult<Revision> {
        unreachable!()
    }

    async fn list_source_credentials(&self) -> AdminStoreResult<Vec<SourceCredentialInfo>> {
        unreachable!()
    }

    async fn load_source_credential(&self, _: &str) -> AdminStoreResult<SourceCredential> {
        unreachable!()
    }

    async fn save_source_credential(
        &self,
        _: SourceCredential,
        _: &MutationContext,
    ) -> AdminStoreResult<Revision> {
        unreachable!()
    }

    async fn delete_source_credential(
        &self,
        _: &str,
        _: &MutationContext,
    ) -> AdminStoreResult<Revision> {
        unreachable!()
    }

    async fn list_artifacts(&self) -> AdminStoreResult<Vec<InstalledPluginArtifact>> {
        unreachable!()
    }

    async fn install_artifact(
        &self,
        _: InspectedPluginArtifact,
        _: PluginSource,
        _: &MutationContext,
    ) -> AdminStoreResult<PluginArtifactMutation> {
        unreachable!()
    }

    async fn accept_artifact(
        &self,
        _: &str,
        _: &MutationContext,
    ) -> AdminStoreResult<PluginArtifactMutation> {
        unreachable!()
    }

    async fn delete_artifact(&self, _: &str, _: &MutationContext) -> AdminStoreResult<Revision> {
        unreachable!()
    }
}

struct PreflightingSystem {
    candidate: SystemUpdateCandidate,
}

#[async_trait]
impl SystemOperations for PreflightingSystem {
    async fn version(&self) -> Result<SystemVersion, SystemOperationError> {
        unreachable!()
    }

    async fn update_detail(
        &self,
        _: bool,
        _: Option<gateway_admin::model::system::SystemUpdateChannel>,
    ) -> Result<SystemUpdateDetail, SystemOperationError> {
        unreachable!()
    }

    fn update_events(&self) -> SystemUpdateEventStream {
        Box::pin(futures::stream::empty())
    }

    async fn perform_update(
        &self,
        _: Option<String>,
        _: Option<gateway_admin::model::system::SystemUpdateChannel>,
        preflight: Arc<dyn SystemUpdatePreflight>,
    ) -> Result<SystemOperationAccepted, SystemOperationError> {
        let revision = preflight.validate(self.candidate.clone()).await?;
        preflight.confirm_revision(revision).await?;
        Ok(SystemOperationAccepted::Update {
            operation_id: "fixture".into(),
            deployment_mode: "fixture".into(),
            message: "accepted".into(),
            target_version: self.candidate.target_version.clone(),
        })
    }

    async fn update_status(&self) -> Result<SystemUpdateStatus, SystemOperationError> {
        Ok(SystemUpdateStatus {
            previous_version: None,
            current_version: None,
            need_restart: false,
            operation: SystemOperationState {
                operation_id: None,
                kind: None,
                status: SystemOperationStatus::Idle,
                target_version: None,
                message: None,
                error: None,
                started_at: None,
                finished_at: None,
            },
        })
    }

    async fn rollback(
        &self,
        _: Arc<dyn SystemUpdatePreflight>,
    ) -> Result<SystemOperationAccepted, SystemOperationError> {
        unreachable!()
    }

    async fn restart(&self) -> Result<SystemOperationAccepted, SystemOperationError> {
        unreachable!()
    }
}

#[tokio::test]
async fn system_update_accepts_compatible_enabled_plugins_at_the_same_revision() {
    let fixture = Fixture::new(requirements("^1.0", "executor"), &[7]);
    let services = super::AdminHarness::new()
        .plugins(fixture.clone(), fixture)
        .system(Arc::new(PreflightingSystem {
            candidate: candidate("executor"),
        }))
        .build()
        .await;

    services
        .system()
        .perform_update(Some("1.2.0".into()), None)
        .await
        .expect("compatible update");
}

#[tokio::test]
async fn system_update_rejects_missing_target_capability_and_revision_changes() {
    let incompatible = Fixture::new(requirements("^1.0", "executor"), &[7]);
    let services = super::AdminHarness::new()
        .plugins(incompatible.clone(), incompatible)
        .system(Arc::new(PreflightingSystem {
            candidate: candidate("models"),
        }))
        .build()
        .await;
    assert!(
        services
            .system()
            .perform_update(Some("1.2.0".into()), None)
            .await
            .is_err()
    );

    let stale = Fixture::new(requirements("^1.0", "executor"), &[7, 8]);
    let services = super::AdminHarness::new()
        .plugins(stale.clone(), stale)
        .system(Arc::new(PreflightingSystem {
            candidate: candidate("executor"),
        }))
        .build()
        .await;
    assert!(
        services
            .system()
            .perform_update(Some("1.2.0".into()), None)
            .await
            .is_err()
    );
}

fn requirements(host_version: &str, capability: &str) -> PluginCompatibilityRequirements {
    PluginCompatibilityRequirements {
        host_version: host_version.into(),
        manifest_schema_version: 2,
        protocol_version: 3,
        capabilities: vec![(capability.into(), 1)],
    }
}

fn candidate(capability: &str) -> SystemUpdateCandidate {
    let manifest = serde_json::to_vec(&serde_json::json!({
        "schema_version": 1,
        "sealed": true,
        "gateway_version": "1.2.0",
        "gateway_git_sha": "a".repeat(40),
        "plugin_host": {
            "schema_version": 2,
            "manifest_schema_versions": [2],
            "protocol_versions": [3],
            "capabilities": [{ "capability": capability, "versions": [1] }],
        },
        "plugins": [],
    }))
    .expect("manifest");
    SystemUpdateCandidate {
        target_version: "1.2.0".into(),
        release_manifest: manifest.into(),
    }
}
