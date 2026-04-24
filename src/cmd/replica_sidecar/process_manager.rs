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

pub(super) struct ReaderBlocker {
    process_manager: Option<ProcessManager>,
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
        if self.cmd.trim().is_empty() {
            return;
        }

        let mut command = tokio::process::Command::new("sh");
        command.arg("-c").arg(&self.cmd);

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

    pub(super) async fn block_reader(&self) -> ReaderBlocker {
        self.add_blocker().await;
        ReaderBlocker {
            process_manager: Some(self.clone()),
        }
    }

    pub(super) async fn remove_blocker(&self) {
        if self.release_blocker_count() {
            // Last blocker removed, start the process.
            self.start().await;
        }
    }

    fn release_blocker_count(&self) -> bool {
        let mut current = self.blockers.load(std::sync::atomic::Ordering::SeqCst);
        loop {
            if current == 0 {
                return false;
            }
            match self.blockers.compare_exchange(
                current,
                current - 1,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return current == 1;
                }
                Err(actual) => current = actual,
            }
        }
    }

    #[cfg(test)]
    pub(super) fn blocker_count(&self) -> usize {
        self.blockers.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl ReaderBlocker {
    pub(super) async fn release(mut self) {
        if let Some(pm) = self.process_manager.take() {
            pm.remove_blocker().await;
        }
    }
}

impl Drop for ReaderBlocker {
    fn drop(&mut self) {
        let Some(pm) = self.process_manager.take() else {
            return;
        };
        if !pm.release_blocker_count() {
            return;
        }
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                pm.start().await;
            });
        }
    }
}
