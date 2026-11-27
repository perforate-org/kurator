//! TLS 1.3 with raw public keys (RFC 7250) in both directions.
//!
//! There are no certificates and no certificate authorities. As on ICP,
//! where a node's TLS key is registered for it, a replica is known by its
//! transport key: the client checks that the server holds the key the
//! membership lists for it, and the server checks that the client holds a
//! key its [`Roster`] trusts.
//!
//! A handshake is signed with the algorithm of the signer's transport key,
//! [`KeyPurpose::Transport`]'s, by the TLS signature scheme that
//! [`Algorithm::tls_scheme`] names; a peer must sign with the scheme of
//! the key it presents.
//!
//! The key exchange is post-quantum: X25519MLKEM768, which combines X25519
//! with ML-KEM-768 (FIPS 203) so that the traffic stays secret unless both
//! are broken. A replica offers and accepts nothing else, since every peer
//! is a Kurator replica: no one can push a handshake down to a classical
//! exchange, and traffic recorded today cannot be read once a quantum
//! computer breaks X25519.

use std::fmt;
use std::sync::Arc;

use kurator_crypto::{Algorithm, KeyPurpose, NodeSecret, VerifyingKey};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::{AlwaysResolvesClientRawPublicKeys, ResolvesClientCert};
use rustls::crypto::{CryptoProvider, verify_tls13_signature_with_raw_key};
use rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, SubjectPublicKeyInfoDer,
    UnixTime,
};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{AlwaysResolvesServerRawPublicKeys, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use rustls::{
    CertificateError, DigitallySignedStruct, DistinguishedName, Error, PeerIncompatible,
    SignatureScheme,
};

use crate::roster::Roster;

/// The ALPN protocol id: the version of the message encoding.
pub(crate) const ALPN: &[u8] = b"kurator-raft/1";

/// The TLS signature scheme of `algorithm`.
fn scheme(algorithm: Algorithm) -> SignatureScheme {
    SignatureScheme::from(algorithm.tls_scheme())
}

/// The signature schemes a replica accepts: those of every algorithm a
/// transport key may use.
fn schemes() -> Vec<SignatureScheme> {
    Algorithm::ALL.into_iter().map(scheme).collect()
}

/// The cryptography every replica's TLS uses: aws-lc-rs, limited to the
/// post-quantum key exchange.
fn provider() -> CryptoProvider {
    CryptoProvider {
        kx_groups: vec![rustls::crypto::aws_lc_rs::kx_group::X25519MLKEM768],
        ..rustls::crypto::aws_lc_rs::default_provider()
    }
}

/// This replica's transport key, ready for TLS.
#[derive(Debug, Clone)]
pub(crate) struct Identity {
    provider: Arc<CryptoProvider>,
    key: Arc<CertifiedKey>,
}

impl Identity {
    /// The transport identity derived from `secret`.
    pub(crate) fn new(secret: &NodeSecret) -> Result<Self, Error> {
        let provider = Arc::new(provider());
        let signing = secret.signing_key(KeyPurpose::Transport);
        let signer = provider
            .key_provider
            .load_private_key(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                signing.to_pkcs8(),
            )))?;
        let spki = signing.verifying_key().to_spki();
        let key = CertifiedKey::new(vec![CertificateDer::from(spki)], signer);
        Ok(Self {
            provider,
            key: Arc::new(key),
        })
    }

    /// The configuration for accepting connections from replicas `roster`
    /// trusts.
    pub(crate) fn server(&self, roster: Arc<Roster>) -> Result<rustls::ServerConfig, Error> {
        let verifier = Arc::new(Trusted {
            roster,
            provider: Arc::clone(&self.provider),
        });
        let resolver: Arc<dyn ResolvesServerCert> = Arc::new(
            AlwaysResolvesServerRawPublicKeys::new(Arc::clone(&self.key)),
        );
        let mut config = rustls::ServerConfig::builder_with_provider(Arc::clone(&self.provider))
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_client_cert_verifier(verifier)
            .with_cert_resolver(resolver);
        config.alpn_protocols = vec![ALPN.to_vec()];
        Ok(config)
    }

    /// The configuration for connecting to the replica that holds `expected`.
    pub(crate) fn client(&self, expected: &VerifyingKey) -> Result<rustls::ClientConfig, Error> {
        let verifier = Arc::new(Expected {
            spki: expected.to_spki(),
            provider: Arc::clone(&self.provider),
        });
        let resolver: Arc<dyn ResolvesClientCert> = Arc::new(
            AlwaysResolvesClientRawPublicKeys::new(Arc::clone(&self.key)),
        );
        let mut config = rustls::ClientConfig::builder_with_provider(Arc::clone(&self.provider))
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_client_cert_resolver(resolver);
        config.alpn_protocols = vec![ALPN.to_vec()];
        Ok(config)
    }
}

/// The public key a peer presented in the handshake.
pub(crate) fn presented_key(spki: &[u8]) -> Option<VerifyingKey> {
    VerifyingKey::from_spki(spki)
}

/// Checks the handshake signature of `spki` over `message`, which must be
/// by the scheme of the key's algorithm.
fn verify_signature(
    provider: &CryptoProvider,
    message: &[u8],
    spki: &CertificateDer<'_>,
    dss: &DigitallySignedStruct,
) -> Result<HandshakeSignatureValid, Error> {
    let key = presented_key(spki).ok_or(CertificateError::BadEncoding)?;
    if dss.scheme != scheme(key.algorithm()) {
        return Err(PeerIncompatible::NoSignatureSchemesInCommon.into());
    }
    verify_tls13_signature_with_raw_key(
        message,
        &SubjectPublicKeyInfoDer::from(spki.as_ref()),
        dss,
        &provider.signature_verification_algorithms,
    )
}

/// Accepts the server that holds one expected key.
struct Expected {
    spki: Vec<u8>,
    provider: Arc<CryptoProvider>,
}

impl fmt::Debug for Expected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Expected")
            .field("key", &presented_key(&self.spki))
            .finish_non_exhaustive()
    }
}

impl ServerCertVerifier for Expected {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        if end_entity.as_ref() == self.spki.as_slice() {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(CertificateError::ApplicationVerificationFailure.into())
        }
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Err(PeerIncompatible::Tls13RequiredForQuic.into())
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_signature(&self.provider, message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}

/// Accepts clients whose key the roster trusts.
#[derive(Debug)]
struct Trusted {
    roster: Arc<Roster>,
    provider: Arc<CryptoProvider>,
}

impl ClientCertVerifier for Trusted {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, Error> {
        presented_key(end_entity)
            .and_then(|key| self.roster.identify(&key))
            .map(|_| ClientCertVerified::assertion())
            .ok_or_else(|| CertificateError::ApplicationVerificationFailure.into())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Err(PeerIncompatible::Tls13RequiredForQuic.into())
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_signature(&self.provider, message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kurator_crypto::{KeyPurpose, NodeSecret};
    use rustls::crypto::{CryptoProvider, SupportedKxGroup};
    use rustls::pki_types::ServerName;
    use rustls::server::AlwaysResolvesServerRawPublicKeys;
    use rustls::{ClientConnection, IoState, NamedGroup, ServerConnection, SignatureScheme};

    use super::{ALPN, Expected, Identity, schemes};

    /// A server that holds `identity`'s key and asks clients for none.
    fn server(identity: &Identity) -> rustls::ServerConfig {
        let mut config =
            rustls::ServerConfig::builder_with_provider(Arc::clone(&identity.provider))
                .with_protocol_versions(&[&rustls::version::TLS13])
                .unwrap()
                .with_no_client_auth()
                .with_cert_resolver(Arc::new(AlwaysResolvesServerRawPublicKeys::new(
                    Arc::clone(&identity.key),
                )));
        config.alpn_protocols = vec![ALPN.to_vec()];
        config
    }

    /// Runs a handshake in memory, and returns the client's side of it.
    fn handshake(
        client: rustls::ClientConfig,
        server: rustls::ServerConfig,
    ) -> Result<ClientConnection, Box<dyn std::error::Error>> {
        let mut client = ClientConnection::new(Arc::new(client), ServerName::try_from("kurator")?)?;
        let mut server = ServerConnection::new(Arc::new(server))?;
        while client.is_handshaking() || server.is_handshaking() {
            let mut bytes = Vec::new();
            while client.wants_write() {
                let _: usize = client.write_tls(&mut bytes)?;
            }
            let _: usize = server.read_tls(&mut bytes.as_slice())?;
            let _: IoState = server.process_new_packets()?;
            bytes.clear();
            while server.wants_write() {
                let _: usize = server.write_tls(&mut bytes)?;
            }
            let _: usize = client.read_tls(&mut bytes.as_slice())?;
            let _: IoState = client.process_new_packets()?;
        }
        Ok(client)
    }

    /// A client with `provider` that expects `secret`'s server and
    /// presents no key of its own.
    fn client(provider: Arc<CryptoProvider>, secret: &NodeSecret) -> rustls::ClientConfig {
        let verifier = Arc::new(Expected {
            spki: secret.public_key(KeyPurpose::Transport).to_spki(),
            provider: Arc::clone(&provider),
        });
        let mut config = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth();
        config.alpn_protocols = vec![ALPN.to_vec()];
        config
    }

    #[test]
    fn replicas_agree_on_the_post_quantum_key_exchange() {
        let secret = NodeSecret::from_seed([5; 32]);
        let identity = Identity::new(&secret).unwrap();
        let replica = client(Arc::clone(&identity.provider), &secret);
        let done = handshake(replica, server(&identity)).unwrap();
        assert_eq!(
            done.negotiated_key_exchange_group()
                .map(SupportedKxGroup::name),
            Some(NamedGroup::X25519MLKEM768),
            "hybrid X25519 and ML-KEM-768"
        );
    }

    #[test]
    fn a_classical_key_exchange_is_refused() {
        let secret = NodeSecret::from_seed([5; 32]);
        let identity = Identity::new(&secret).unwrap();
        let classical = Arc::new(CryptoProvider {
            kx_groups: vec![rustls::crypto::aws_lc_rs::kx_group::X25519],
            ..rustls::crypto::aws_lc_rs::default_provider()
        });
        assert!(
            handshake(client(classical, &secret), server(&identity)).is_err(),
            "X25519 alone is not enough"
        );
    }

    #[test]
    fn an_expectation_shows_the_key_it_expects() {
        let key = NodeSecret::from_seed([5; 32]).public_key(KeyPurpose::Transport);
        let expected = Expected {
            spki: key.to_spki(),
            provider: Arc::new(super::provider()),
        };
        assert_eq!(
            format!("{expected:?}"),
            format!("Expected {{ key: Some(VerifyingKey({key})), .. }}"),
            "debug"
        );
    }

    #[test]
    fn transport_keys_sign_with_a_scheme_replicas_accept() {
        assert_eq!(schemes(), vec![SignatureScheme::ED25519], "Ed25519");
        let scheme = SignatureScheme::from(KeyPurpose::Transport.algorithm().tls_scheme());
        assert!(schemes().contains(&scheme), "{scheme:?}");
    }
}
