use std::net::SocketAddr;
use std::sync::Arc;

use log::{error, warn};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

#[derive(Clone)]
pub(super) struct ReaderProxyHandle {
    active_target: watch::Sender<Option<SocketAddr>>,
}

pub(super) struct ReaderProxy {
    listener: TcpListener,
    active_target: watch::Receiver<Option<SocketAddr>>,
}

impl ReaderProxyHandle {
    pub(super) fn set_active_target(&self, target: SocketAddr) {
        let _ = self.active_target.send(Some(target));
    }

    #[cfg(test)]
    fn clear_active_target(&self) {
        let _ = self.active_target.send(None);
    }
}

impl ReaderProxy {
    pub(super) async fn bind(addr: SocketAddr) -> std::io::Result<(Self, ReaderProxyHandle)> {
        let listener = TcpListener::bind(addr).await?;
        let (active_tx, active_rx) = watch::channel(None);
        Ok((
            Self {
                listener,
                active_target: active_rx,
            },
            ReaderProxyHandle {
                active_target: active_tx,
            },
        ))
    }

    pub(super) fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub(super) async fn serve(self) -> std::io::Result<()> {
        let active_target = Arc::new(self.active_target);
        loop {
            let (client, _) = self.listener.accept().await?;
            let target = *active_target.borrow();
            tokio::spawn(async move {
                if let Err(err) = proxy_connection(client, target).await {
                    warn!("ReaderProxy: connection failed: {err}");
                }
            });
        }
    }
}

async fn proxy_connection(
    mut client: TcpStream,
    target: Option<SocketAddr>,
) -> std::io::Result<()> {
    let Some(target) = target else {
        client
            .write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
            .await?;
        return Ok(());
    };

    let mut upstream = match TcpStream::connect(target).await {
        Ok(upstream) => upstream,
        Err(err) => {
            let _ = client
                .write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
                .await;
            return Err(err);
        }
    };

    match tokio::io::copy_bidirectional(&mut client, &mut upstream).await {
        Ok(_) => Ok(()),
        Err(err) => {
            error!("ReaderProxy: bidirectional copy failed: {err}");
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::ReaderProxy;

    fn loopback_any_port() -> std::net::SocketAddr {
        (IpAddr::V4(Ipv4Addr::LOCALHOST), 0).into()
    }

    async fn read_http_response(addr: std::net::SocketAddr) -> String {
        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("connect proxy");
        stream
            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .expect("write request");
        let mut buf = vec![0; 256];
        let n = stream.read(&mut buf).await.expect("read response");
        String::from_utf8_lossy(&buf[..n]).into_owned()
    }

    async fn start_static_http_child(body: &'static str) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind(loopback_any_port())
            .await
            .expect("bind child");
        let addr = listener.local_addr().expect("child addr");
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut request = [0; 256];
                    let _ = socket.read(&mut request).await;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn proxy_returns_unavailable_before_active_target() {
        let (proxy, _handle) = ReaderProxy::bind(loopback_any_port())
            .await
            .expect("bind proxy");
        let proxy_addr = proxy.local_addr().expect("proxy addr");
        let proxy_task = tokio::spawn(proxy.serve());

        let response = read_http_response(proxy_addr).await;

        proxy_task.abort();
        assert!(response.starts_with("HTTP/1.1 503 Service Unavailable"));
    }

    #[tokio::test]
    async fn proxy_routes_new_connections_to_latest_active_target() {
        let first = start_static_http_child("first-generation").await;
        let second = start_static_http_child("second-generation").await;
        let (proxy, handle) = ReaderProxy::bind(loopback_any_port())
            .await
            .expect("bind proxy");
        let proxy_addr = proxy.local_addr().expect("proxy addr");
        let proxy_task = tokio::spawn(proxy.serve());

        handle.set_active_target(first);
        let first_response = read_http_response(proxy_addr).await;
        handle.set_active_target(second);
        let second_response = read_http_response(proxy_addr).await;

        proxy_task.abort();
        assert!(first_response.ends_with("first-generation"));
        assert!(second_response.ends_with("second-generation"));
    }

    #[tokio::test]
    async fn proxy_returns_unavailable_after_active_target_is_cleared() {
        let first = start_static_http_child("first-generation").await;
        let (proxy, handle) = ReaderProxy::bind(loopback_any_port())
            .await
            .expect("bind proxy");
        let proxy_addr = proxy.local_addr().expect("proxy addr");
        let proxy_task = tokio::spawn(proxy.serve());

        handle.set_active_target(first);
        assert!(
            read_http_response(proxy_addr)
                .await
                .ends_with("first-generation")
        );
        handle.clear_active_target();
        tokio::time::sleep(Duration::from_millis(10)).await;
        let response = read_http_response(proxy_addr).await;

        proxy_task.abort();
        assert!(response.starts_with("HTTP/1.1 503 Service Unavailable"));
    }
}
