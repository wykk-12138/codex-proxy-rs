//! 验证导出进程的成功、错误、取消与暂存清理生命周期

use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::postgres::{TestDatabase, drain_analytics};
use gateway_admin::model::backup::code;
use gateway_admin::ports::backup::{DatabaseDumpPort, DumpRequest};
use gateway_core::lifecycle::CancellationToken;
use gateway_store::backup::{pg_dump::PgDumpAdapter, staging::StagingArea};
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncReadExt as _;

#[tokio::test]
async fn dump_process_lifecycle() {
    const PROBE_DIR: &str = "CPR_TEST_PG_DUMP_PROBE_DIR";
    let Ok(directory) = std::env::var(PROBE_DIR) else {
        // PATH 只在独立测试进程中改变，不污染同一 harness 的其他测试
        let directory = tempfile::tempdir().unwrap();
        let script = directory.path().join("pg_dump");
        std::fs::write(
            &script,
            "#!/bin/sh\nprintf '%s' $$ > \"$CPR_TEST_PG_DUMP_PROBE_DIR/pid\"\ncase \"$5\" in\n  wait) exec 1>&-; exec sleep 30 ;;\n  stream) printf archive; exec sleep 30 ;;\n  fail) printf partial; exit 1 ;;\n  *) printf archive ;;\nesac\n",
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut paths = vec![directory.path().to_path_buf()];
        paths.extend(std::env::split_paths(
            &std::env::var_os("PATH").unwrap_or_default(),
        ));
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "backup::pg_dump::dump_process_lifecycle",
                "--nocapture",
            ])
            .env(PROBE_DIR, directory.path())
            .env("PATH", std::env::join_paths(paths).unwrap())
            .status()
            .unwrap();
        assert!(status.success());
        return;
    };
    let Some(database) = TestDatabase::create("dump_lifecycle").await else {
        return;
    };
    drain_analytics(&database.pool).await;
    let directory = Path::new(&directory);
    let staging = Arc::new(StagingArea::open(directory.join("staging"), 1024 * 1024).unwrap());
    let dump = |mode: &str| {
        PgDumpAdapter::new(
            staging.clone(),
            mode,
            "test-password",
            database.pool.clone(),
        )
    };

    let artifact = dump("success").dump(request("success")).await.unwrap();
    let compressed = std::fs::read(&artifact.path).unwrap();
    let mut decoded = String::new();
    async_compression::tokio::bufread::GzipDecoder::new(compressed.as_slice())
        .read_to_string(&mut decoded)
        .await
        .unwrap();
    assert!(decoded.contains("\narchive\n"));
    assert_eq!(artifact.size_bytes, compressed.len() as u64);
    assert_eq!(artifact.sha256, hex::encode(Sha256::digest(&compressed)));
    assert!(!staging.partial_path("success").exists());
    assert_eq!(
        dump("success")
            .inspect_staging("success")
            .await
            .unwrap()
            .unwrap()
            .sha256,
        artifact.sha256
    );

    let error = dump("fail").dump(request("fail")).await.unwrap_err();
    assert_eq!(error.code(), code::PG_DUMP_FAILED);
    assert!(!staging.partial_path("fail").exists());

    let small = Arc::new(StagingArea::open(directory.join("small"), 1).unwrap());
    let error = PgDumpAdapter::new(
        small.clone(),
        "success",
        "test-password",
        database.pool.clone(),
    )
    .dump(request("oversized"))
    .await
    .unwrap_err();
    assert_eq!(error.code(), code::STAGING_SPACE_EXHAUSTED);
    assert!(!small.partial_path("oversized").exists());

    std::fs::create_dir(staging.final_path("rename")).unwrap();
    assert!(dump("success").dump(request("rename")).await.is_err());
    assert!(!staging.partial_path("rename").exists());

    std::fs::remove_file(directory.join("pid")).unwrap();
    std::fs::create_dir(staging.partial_path("open")).unwrap();
    assert!(dump("success").dump(request("open")).await.is_err());
    assert!(
        !directory.join("pid").exists(),
        "open failure must not spawn a process"
    );

    for (mode, abort) in [("wait", false), ("stream", true)] {
        let adapter = dump(mode);
        let request = request(mode);
        let cancellation = request.cancellation.clone();
        let task = tokio::spawn(async move { adapter.dump(request).await });
        wait_until(|| {
            std::fs::read_to_string(directory.join("pid")).is_ok_and(|pid| !pid.is_empty())
        })
        .await;
        let pid = std::fs::read_to_string(directory.join("pid")).unwrap();
        if mode == "wait" {
            // stdout 已关闭，但子进程仍存活：取消必须覆盖 wait 阶段
            wait_until(|| std::fs::read_link(format!("/proc/{pid}/fd/1")).is_err()).await;
        }
        if abort {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            cancellation.cancel();
            let error = tokio::time::timeout(Duration::from_secs(3), task)
                .await
                .expect("cancel must not wait for natural process exit")
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code(), code::CANCELLED);
        }
        wait_until(|| !Path::new(&format!("/proc/{pid}")).exists()).await;
        assert!(!staging.partial_path(mode).exists());
        assert!(!staging.final_path(mode).exists());
        std::fs::remove_file(directory.join("pid")).unwrap();
    }
    assert!(
        artifact.path.exists(),
        "completed archives remain available for recovery"
    );
    database.close().await;
}

#[tokio::test]
async fn cleanup_reports_failure_and_still_removes_the_other_archive() {
    let directory = tempfile::tempdir().unwrap();
    let staging = Arc::new(StagingArea::open(directory.path().to_path_buf(), 64).unwrap());
    let adapter = PgDumpAdapter::new(
        staging.clone(),
        "unused",
        "unused",
        sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .unwrap(),
    );
    adapter.cleanup_staging("missing").await.unwrap();
    std::fs::create_dir(staging.partial_path("blocked")).unwrap();
    std::fs::write(staging.final_path("blocked"), b"archive").unwrap();
    assert!(adapter.cleanup_staging("blocked").await.is_err());
    assert!(!staging.final_path("blocked").exists());
    assert!(staging.partial_path("blocked").is_dir());
}

fn request(backup_id: &str) -> DumpRequest {
    DumpRequest {
        backup_id: backup_id.to_owned(),
        cancellation: CancellationToken::new(),
    }
}

async fn wait_until(condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("process lifecycle condition");
}
