//! Metrics: what each replica of the node is doing, as a Prometheus page
//! served over HTTP at `/metrics`, when `run --metrics <addr>` asks for
//! one.
//!
//! The page is [`Store::metrics`], built when it is asked for.

use std::future;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use kurator_net::Store;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};

/// How long a scraper may take to send its request and read the page.
const PATIENCE: Duration = Duration::from_secs(5);

/// The largest request head read; a longer one is answered as it stands.
const HEAD: usize = 8 << 10;

/// Where scrapers connect, when the node serves metrics.
#[derive(Debug)]
pub struct Endpoint {
    listener: Option<TcpListener>,
}

impl Endpoint {
    /// Listens at `addr`, or nowhere when it is `None`.
    ///
    /// # Errors
    ///
    /// When `addr` cannot be bound.
    pub async fn bind(addr: Option<SocketAddr>) -> io::Result<Self> {
        let listener = match addr {
            Some(addr) => Some(TcpListener::bind(addr).await?),
            None => None,
        };
        Ok(Self { listener })
    }

    /// Where it listens, if anywhere.
    ///
    /// # Errors
    ///
    /// When the socket cannot say.
    pub fn local_addr(&self) -> io::Result<Option<SocketAddr>> {
        self.listener
            .as_ref()
            .map(TcpListener::local_addr)
            .transpose()
    }

    /// The next scraper; never, when it listens nowhere.
    ///
    /// # Errors
    ///
    /// When accepting fails.
    pub async fn accept(&self) -> io::Result<TcpStream> {
        match &self.listener {
            Some(listener) => Ok(listener.accept().await?.0),
            None => future::pending().await,
        }
    }
}

/// Answers one request on `stream`: the page of `store` for `GET
/// /metrics`, and 404 for anything else. A scraper slower than
/// five seconds is dropped.
///
/// # Errors
///
/// When the stream fails or the scraper is too slow.
pub async fn answer(store: &Store, stream: TcpStream) -> io::Result<()> {
    tokio::time::timeout(PATIENCE, respond(store, stream))
        .await
        .map_err(io::Error::other)?
}

async fn respond(store: &Store, mut stream: TcpStream) -> io::Result<()> {
    let head = read_head(&mut stream).await?;
    let (status, body) = if head.starts_with(b"GET /metrics ") {
        ("200 OK", store.metrics())
    } else {
        ("404 Not Found", "not found; try /metrics\n".to_owned())
    };
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: text/plain; version=0.0.4\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

/// Reads up to the blank line that ends a request's head, or [`HEAD`]
/// bytes, whichever comes first.
async fn read_head(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut head = Vec::new();
    let mut chunk = [0_u8; 1024];
    while head.len() < HEAD && !head.windows(4).any(|end| end == b"\r\n\r\n") {
        let read = stream.read(&mut chunk).await?;
        let Some(bytes) = chunk.get(..read).filter(|bytes| !bytes.is_empty()) else {
            break;
        };
        head.extend_from_slice(bytes);
    }
    Ok(head)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::AsyncWriteExt as _;
    use tokio::net::{TcpListener, TcpStream};

    use super::{HEAD, read_head};

    /// What `read_head` takes from a client that sends `parts`, a pause
    /// between each.
    async fn head_of(parts: Vec<Vec<u8>>) -> Vec<u8> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::spawn(async move {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            for part in parts {
                stream.write_all(&part).await.unwrap();
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });
        let (mut stream, _) = listener.accept().await.unwrap();
        let head = read_head(&mut stream).await.unwrap();
        client.await.unwrap();
        head
    }

    #[tokio::test]
    async fn a_head_is_read_to_its_blank_line_or_its_limit() {
        let split = vec![b"GET /met".to_vec(), b"rics HTTP/1.1\r\n\r\n".to_vec()];
        assert_eq!(
            head_of(split).await,
            b"GET /metrics HTTP/1.1\r\n\r\n",
            "across two segments"
        );
        let endless = vec![vec![b'x'; HEAD + 4096]];
        assert_eq!(
            head_of(endless).await.len(),
            HEAD,
            "no further than the limit"
        );
        assert!(
            head_of(Vec::new()).await.is_empty(),
            "a client that hangs up"
        );
    }
}
