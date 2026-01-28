use std::process::Stdio;
use std::sync::Arc;

use log::info;
use tokio::sync::Mutex;

#[derive(Clone)]
pub(super) struct ProcessManager {
    cmd: String,
    child: Arc<Mutex<Option<tokio::process::Child>>>,
    blockers: Arc<std::sync::atomic::AtomicUsize>,
}

impl ProcessManager {
    pub(super) fn new(cmd: String) -> Self {
        Self {
            cmd,
            child: Arc::new(Mutex::new(None)),
            blockers: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    pub(super) async fn start(&self) {
        let mut child_lock = self.child.lock().await;
        if child_lock.is_some() {
            return;
        }

        info!("ProcessManager: Starting child process: {}", self.cmd);
        let parts: Vec<&str> = self.cmd.split_whitespace().collect();
        if parts.is_empty() {
            return;
        }

        let mut command = tokio::process::Command::new(parts[0]);
        if parts.len() > 1 {
            command.args(&parts[1..]);
        }

        // Inherit stdout/stderr allowing logs to show up in replited output.
        command.stdout(Stdio::inherit());
        command.stderr(Stdio::inherit());

        match command.spawn() {
            Ok(child) => {
                *child_lock = Some(child);
                info!("ProcessManager: Child process started.");
            }
            Err(e) => {
                log::error!("ProcessManager: Failed to spawn child process: {e}");
            }
        }
    }

    pub(super) async fn stop(&self) {
        let mut child_lock = self.child.lock().await;
        if let Some(mut child) = child_lock.take() {
            info!("ProcessManager: Stopping child process...");
            // TODO: Graceful shutdown if needed.
            let _ = child.kill().await;
            let _ = child.wait().await;
            info!("ProcessManager: Child process stopped.");
        }
    }

    pub(super) async fn add_blocker(&self) {
        let prev = self
            .blockers
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if prev == 0 {
            // First blocker, stop the process.
            self.stop().await;
        }
    }

    pub(super) async fn remove_blocker(&self) {
        let prev = self
            .blockers
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        if prev == 1 {
            // Last blocker removed, start the process.
            self.start().await;
        }
    }

    /// Restart the child process (for schema changes, etc.)
    /// Only restarts if there are no active blockers.
    pub(super) async fn restart(&self) {
        let blockers = self.blockers.load(std::sync::atomic::Ordering::SeqCst);
        if blockers > 0 {
            log::warn!(
                "ProcessManager: Restart requested but blockers active ({blockers}), skipping."
            );
            return;
        }
        info!("ProcessManager: Restarting child process...");
        self.stop().await;
        self.start().await;
    }
}
