use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use log::info;
use rusqlite::Connection;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use super::managed_reader::{ManagedChildTemplate, ManagedGenerationLayout};
use super::reader_proxy::{ReaderProxy, ReaderProxyHandle};

const CHILD_PROCESS_STARTING_LOG: &str = "ProcessManager: Starting child process.";
const CHILD_TERMINATION_GRACE: Duration = Duration::from_secs(5);
const MANAGED_PROXY_HEALTH_TIMEOUT: Duration = Duration::from_secs(30);
const AUXILIARY_DB_FILE: &str = "auxiliary.db";

#[derive(Clone)]
pub(super) struct ProcessManager {
    mode: ProcessManagerMode,
    blockers: Arc<std::sync::atomic::AtomicUsize>,
}

pub(super) struct ReaderBlocker {
    process_manager: Option<ProcessManager>,
}

#[derive(Clone)]
enum ProcessManagerMode {
    Legacy {
        cmd: String,
        child: Arc<Mutex<Option<tokio::process::Child>>>,
    },
    ManagedProxy {
        template: ManagedChildTemplate,
        layout: Arc<ManagedGenerationLayout>,
        db_path: PathBuf,
        proxy: ReaderProxyHandle,
        active_child: Arc<Mutex<Option<ManagedChild>>>,
        generation_counter: Arc<std::sync::atomic::AtomicU64>,
        proxy_task: Arc<tokio::task::JoinHandle<std::io::Result<()>>>,
    },
}

struct ManagedChild {
    child: tokio::process::Child,
    generation_id: String,
    port: u16,
}

impl ProcessManager {
    pub(super) fn new(cmd: String) -> Self {
        Self {
            mode: ProcessManagerMode::Legacy {
                cmd,
                child: Arc::new(Mutex::new(None)),
            },
            blockers: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    pub(super) async fn new_managed_proxy(
        proxy_addr: SocketAddr,
        template: ManagedChildTemplate,
        generation_root: impl Into<PathBuf>,
        db_path: impl Into<PathBuf>,
    ) -> std::io::Result<Self> {
        let (proxy, handle) = ReaderProxy::bind(proxy_addr).await?;
        let proxy_task = tokio::spawn(proxy.serve());
        Ok(Self {
            mode: ProcessManagerMode::ManagedProxy {
                template,
                layout: Arc::new(ManagedGenerationLayout::new(generation_root)),
                db_path: db_path.into(),
                proxy: handle,
                active_child: Arc::new(Mutex::new(None)),
                generation_counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                proxy_task: Arc::new(proxy_task),
            },
            blockers: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        })
    }

    pub(super) async fn start(&self) {
        match &self.mode {
            ProcessManagerMode::Legacy { cmd, child } => start_legacy_child(cmd, child).await,
            ProcessManagerMode::ManagedProxy { .. } => {
                if let Err(err) = self.promote_managed_generation().await {
                    log::error!("ProcessManager: managed proxy handoff failed: {err}");
                }
            }
        }
    }

    pub(super) async fn stop(&self) {
        match &self.mode {
            ProcessManagerMode::Legacy { child, .. } => stop_child(child).await,
            ProcessManagerMode::ManagedProxy {
                active_child,
                proxy_task,
                ..
            } => {
                stop_managed_child(active_child).await;
                proxy_task.abort();
            }
        }
    }

    pub(super) async fn add_blocker(&self) {
        let prev = self
            .blockers
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if prev == 0 && matches!(self.mode, ProcessManagerMode::Legacy { .. }) {
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

    async fn promote_managed_generation(&self) -> std::io::Result<()> {
        let ProcessManagerMode::ManagedProxy {
            template,
            layout,
            db_path,
            proxy,
            active_child,
            generation_counter,
            ..
        } = &self.mode
        else {
            return Ok(());
        };

        let generation_number =
            generation_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        let generation_id = format!("reader-{generation_number:020}");
        let generation_dir = layout.generation_dir(&generation_id);
        prepare_generation_dir(db_path, &generation_dir).await?;
        layout.promote_active_generation(&generation_id)?;

        let port = reserve_loopback_port()?;
        let command = template.render(port, &generation_dir);
        let child = spawn_shell_child(&command)?;
        wait_for_http_health(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)).await?;

        let mut active = active_child.lock().await;
        let old = active.replace(ManagedChild {
            child,
            generation_id,
            port,
        });
        proxy.set_active_target(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port));
        drop(active);

        if let Some(old) = old {
            info!(
                "ProcessManager: retiring managed reader generation {} on port {}",
                old.generation_id, old.port
            );
            stop_process_child(old.child).await;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn blocker_count(&self) -> usize {
        self.blockers.load(std::sync::atomic::Ordering::SeqCst)
    }
}

async fn start_legacy_child(cmd: &str, child: &Arc<Mutex<Option<tokio::process::Child>>>) {
    let mut child_lock = child.lock().await;
    if child_lock.is_some() {
        return;
    }

    info!("{CHILD_PROCESS_STARTING_LOG}");
    if cmd.trim().is_empty() {
        return;
    }

    match spawn_shell_child(cmd) {
        Ok(child) => {
            *child_lock = Some(child);
            info!("ProcessManager: Child process started.");
        }
        Err(e) => {
            log::error!("ProcessManager: Failed to spawn child process: {e}");
        }
    }
}

fn spawn_shell_child(cmd: &str) -> std::io::Result<tokio::process::Child> {
    let mut command = tokio::process::Command::new("sh");
    command.arg("-c").arg(cmd);
    #[cfg(unix)]
    {
        command.process_group(0);
    }

    // Inherit stdout/stderr allowing logs to show up in replited output.
    command.stdout(Stdio::inherit());
    command.stderr(Stdio::inherit());
    command.spawn()
}

async fn stop_child(child: &Arc<Mutex<Option<tokio::process::Child>>>) {
    let mut child_lock = child.lock().await;
    if let Some(child) = child_lock.take() {
        stop_process_child(child).await;
    }
}

async fn stop_managed_child(active_child: &Arc<Mutex<Option<ManagedChild>>>) {
    let mut active = active_child.lock().await;
    if let Some(child) = active.take() {
        stop_process_child(child.child).await;
    }
}

async fn stop_process_child(mut child: tokio::process::Child) {
    info!("ProcessManager: Stopping child process...");
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        let pgid = -(pid as libc::pid_t);
        // The exec command is shell-owned, so kill the whole process group to avoid
        // leaving managed reader descendants bound to ports or holding SQLite files open.
        unsafe {
            libc::kill(pgid, libc::SIGTERM);
        }

        match tokio::time::timeout(CHILD_TERMINATION_GRACE, child.wait()).await {
            Ok(_) => {
                info!("ProcessManager: Child process stopped.");
                return;
            }
            Err(_) => unsafe {
                libc::kill(pgid, libc::SIGKILL);
            },
        }
    }

    let _ = child.kill().await;
    let _ = child.wait().await;
    info!("ProcessManager: Child process stopped.");
}

async fn prepare_generation_dir(db_path: &Path, generation_dir: &Path) -> std::io::Result<()> {
    let db_path = db_path.to_path_buf();
    let generation_dir = generation_dir.to_path_buf();
    tokio::task::spawn_blocking(move || prepare_generation_dir_sync(&db_path, &generation_dir))
        .await
        .map_err(|err| std::io::Error::other(err.to_string()))?
}

fn prepare_generation_dir_sync(db_path: &Path, generation_dir: &Path) -> std::io::Result<()> {
    let tmp_dir = generation_dir.with_extension("tmp");
    match std::fs::remove_dir_all(&tmp_dir) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    std::fs::create_dir_all(&tmp_dir)?;

    let tmp_db = tmp_dir.join("data.db");
    let conn = Connection::open(db_path).map_err(std::io::Error::other)?;
    conn.backup(rusqlite::MAIN_DB, &tmp_db, None)
        .map_err(std::io::Error::other)?;
    drop(conn);

    let source_aux = db_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(AUXILIARY_DB_FILE);
    if source_aux.exists() {
        std::fs::copy(&source_aux, tmp_dir.join(AUXILIARY_DB_FILE))?;
    }

    match std::fs::remove_dir_all(generation_dir) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    std::fs::rename(&tmp_dir, generation_dir)?;
    Ok(())
}

fn reserve_loopback_port() -> std::io::Result<u16> {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    Ok(listener.local_addr()?.port())
}

async fn wait_for_http_health(addr: SocketAddr) -> std::io::Result<()> {
    let deadline = tokio::time::Instant::now() + MANAGED_PROXY_HEALTH_TIMEOUT;
    let request = b"GET /health HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
    loop {
        match TcpStream::connect(addr).await {
            Ok(mut stream) => {
                if tokio::io::AsyncWriteExt::write_all(&mut stream, request)
                    .await
                    .is_ok()
                {
                    let mut buf = vec![0; 256];
                    match tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await {
                        Ok(n) if String::from_utf8_lossy(&buf[..n]).contains("200 OK") => {
                            return Ok(());
                        }
                        _ => {}
                    }
                }
            }
            Err(_) => {}
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("managed child did not become healthy at {addr}"),
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
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

    use super::{CHILD_PROCESS_STARTING_LOG, ProcessManager};

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

    #[test]
    fn start_log_message_does_not_include_exec_command() {
        let secret_bearing_command =
            "EDGE_COMMAND_AUTH_KEY=secret-token REGISTRY_CREDENTIAL_PW=secret /app serve";

        assert!(!CHILD_PROCESS_STARTING_LOG.contains(secret_bearing_command));
        assert!(!CHILD_PROCESS_STARTING_LOG.contains("secret-token"));
        assert!(!CHILD_PROCESS_STARTING_LOG.contains("REGISTRY_CREDENTIAL_PW"));
    }

    #[tokio::test]
    async fn stop_terminates_descendant_processes_spawned_by_shell_command() {
        let dir = tempdir().expect("tempdir");
        let pid_file = dir.path().join("child.pid");
        let cmd = format!(
            "sleep 60 & child=$!; printf '%s\\n' \"$child\" > '{}'; wait \"$child\"",
            pid_file.display()
        );
        let pm = ProcessManager::new(cmd);

        pm.start().await;

        let deadline = Instant::now() + Duration::from_secs(2);
        let mut child_pid = String::new();
        while Instant::now() < deadline {
            if let Ok(pid) = fs::read_to_string(&pid_file) {
                let pid = pid.trim();
                if !pid.is_empty() && process_is_alive(pid) {
                    child_pid = pid.to_string();
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            !child_pid.is_empty(),
            "test child should be running before stop"
        );

        pm.stop().await;

        let deadline = Instant::now() + Duration::from_secs(2);
        while process_is_alive(&child_pid) && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            !process_is_alive(&child_pid),
            "stop should terminate shell descendants, pid={child_pid}"
        );
    }

    #[tokio::test]
    async fn stop_allows_shell_wrapper_to_finish_term_cleanup_before_kill() {
        let dir = tempdir().expect("tempdir");
        let ready_file = dir.path().join("ready");
        let cleanup_file = dir.path().join("cleaned");
        let cmd = format!(
            "trap 'sleep 1; printf cleaned > \"{}\"; exit 0' TERM; printf ready > \"{}\"; while :; do sleep 60 & wait $!; done",
            cleanup_file.display(),
            ready_file.display()
        );
        let pm = ProcessManager::new(cmd);

        pm.start().await;

        let deadline = Instant::now() + Duration::from_secs(2);
        while !ready_file.exists() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(ready_file.exists(), "shell wrapper should start");

        pm.stop().await;

        assert_eq!(
            fs::read_to_string(cleanup_file).expect("TERM cleanup marker"),
            "cleaned"
        );
    }
}
