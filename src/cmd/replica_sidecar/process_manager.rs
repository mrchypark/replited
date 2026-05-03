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
        #[cfg(unix)]
        {
            command.process_group(0);
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
            #[cfg(unix)]
            if let Some(pid) = child.id() {
                let pgid = -(pid as libc::pid_t);
                // The exec command is shell-owned, so kill the whole process group to avoid
                // leaving the managed reader bound to ports or holding SQLite files open.
                unsafe {
                    libc::kill(pgid, libc::SIGTERM);
                }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                unsafe {
                    libc::kill(pgid, libc::SIGKILL);
                }
            }

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
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    pm.start().await;
                });
            }
            Err(err) => {
                log::warn!(
                    "ProcessManager: reader blocker dropped without a Tokio runtime; child restart skipped: {err}"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::process::Command;
    use std::time::{Duration, Instant};

    use tempfile::tempdir;

    use super::ProcessManager;

    fn process_is_alive(pid: &str) -> bool {
        Command::new("kill")
            .arg("-0")
            .arg(pid)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    #[tokio::test]
    async fn stop_terminates_descendant_processes_spawned_by_shell_command() {
        let dir = tempdir().expect("tempdir");
        let pid_file = dir.path().join("child.pid");
        let cmd = format!("sleep 60 & echo $! > {}; wait", pid_file.display());
        let pm = ProcessManager::new(cmd);

        pm.start().await;

        let deadline = Instant::now() + Duration::from_secs(2);
        while !pid_file.exists() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let child_pid = fs::read_to_string(&pid_file).expect("child pid file");
        let child_pid = child_pid.trim();
        assert!(
            process_is_alive(child_pid),
            "test child should be running before stop"
        );

        pm.stop().await;

        let deadline = Instant::now() + Duration::from_secs(2);
        while process_is_alive(child_pid) && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            !process_is_alive(child_pid),
            "stop should terminate shell descendants, pid={child_pid}"
        );
    }
}
