//! A client's session: one transaction across every shard its operations
//! touch, run from the command line.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use oleaph_client::{Across, Client, Coordinator, Transacted};
use oleaph_net::{Endpoint, Quic, Wall};

use crate::NodeError;
use crate::args::{Op, Session};
use crate::secret;

/// Runs `session` as one transaction and says what it read and how it
/// came out.
///
/// # Errors
///
/// When the secret cannot be read or the endpoint cannot start, no node
/// tells where the shards are, or an operation fails.
pub async fn run(session: &Session) -> Result<String, NodeError> {
    let (secret, _) = secret::load_or_create(session.dir())?;
    let first = session.peers().first().map(|(_, peer)| peer.addr());
    let endpoint = Endpoint::bind(local(first), &secret, BTreeMap::new())?;
    let entry = session.peers().iter().cloned().collect();
    let client = Client::new(endpoint.transport(), Wall::default(), entry, origin()?);
    if !client.learn().await {
        return Err(NodeError::Unreached);
    }
    let mut txn = Coordinator::new(&client).begin();
    let mut shown = apply(&mut txn, session.ops()).await?;
    let outcome = match txn.commit().await {
        Transacted::Committed(version) => format!("committed at {version}"),
        Transacted::Aborted => "aborted: try again".to_owned(),
        Transacted::Rejected => "rejected".to_owned(),
        Transacted::Unknown | _ => "unknown: it may or may not have taken effect".to_owned(),
    };
    shown.push_str(&outcome);
    Ok(shown)
}

/// Applies `ops` to `txn`, and shows each value read, one per line.
async fn apply(txn: &mut Across<'_, Client<Quic, Wall>>, ops: &[Op]) -> Result<String, NodeError> {
    let mut shown = String::new();
    for op in ops {
        match op {
            Op::Get(key) => {
                let value = txn.get(key).await?;
                let value = value.map_or_else(
                    || "(none)".to_owned(),
                    |value| String::from_utf8_lossy(&value).into_owned(),
                );
                let _infallible = writeln!(shown, "{} = {value}", String::from_utf8_lossy(key));
            }
            Op::Put(key, value) => txn.put(key, value.clone()).await?,
            Op::Delete(key) => txn.delete(key).await?,
        }
    }
    Ok(shown)
}

/// Where a client binds to reach `peer`: any port, on any address of the
/// peer's family.
fn local(peer: Option<SocketAddr>) -> SocketAddr {
    let any = match peer {
        Some(SocketAddr::V6(_)) => Ipv6Addr::UNSPECIFIED.into(),
        Some(SocketAddr::V4(_)) | None => Ipv4Addr::UNSPECIFIED.into(),
    };
    SocketAddr::new(any, 0)
}

/// A number no other client of the cluster picks, but by chance.
fn origin() -> Result<u64, NodeError> {
    let mut bytes = [0_u8; 8];
    getrandom::fill(&mut bytes).map_err(std::io::Error::other)?;
    Ok(u64::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::{local, origin};

    #[test]
    fn a_client_binds_in_the_family_of_its_peer() {
        let v6: SocketAddr = "[::1]:7000".parse().unwrap();
        let v4: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        assert_eq!(local(Some(v6)), "[::]:0".parse().unwrap(), "v6");
        assert_eq!(local(Some(v4)), "0.0.0.0:0".parse().unwrap(), "v4");
        assert_eq!(local(None), "0.0.0.0:0".parse().unwrap(), "none");
    }

    #[test]
    fn origins_differ() {
        assert_ne!(origin().unwrap(), origin().unwrap(), "random");
    }
}
