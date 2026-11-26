//! TLS 1.3 with raw Ed25519 public keys (RFC 7250) in both directions.
//!
//! There are no certificates and no certificate authorities. As on ICP,
//! where a node's TLS key is registered for it, a replica is known by its
//! transport key: the client checks that the server holds the key the
//! membership lists for it, and the server checks that the client holds a
//! key its [`Roster`] trusts.

use std::fmt;
use std::sync::Arc;

use kurator_raft::{KeyPurpose, NodeSecret};
use kurator_wal::VerifyingKey;
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

use crate::der;
use crate::roster::Roster;

/// The ALPN protocol id: the version of the message encoding.
pub(crate) const ALPN: &[u8] = b"kurator-raft/1";

/// The one signature scheme a replica accepts.
const SCHEME: SignatureScheme = SignatureScheme::ED25519;

/// This replica's transport key, ready for TLS.
#[derive(Debug, Clone)]
pub(crate) struct Identity {
    provider: Arc<CryptoProvider>,
    key: Arc<CertifiedKey>,
}

impl Identity {
    /// The transport identity derived from `secret`.
    pub(crate) fn new(secret: &NodeSecret) -> Result<Self, Error> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let pkcs8 = der::pkcs8(secret.derive(KeyPurpose::Transport));
        let signer = provider
            .key_provider
            .load_private_key(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(pkcs8)))?;
        let public = secret.public_key(KeyPurpose::Transport).to_bytes();
        let key = CertifiedKey::new(vec![CertificateDer::from(der::spki(public))], signer);
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
            spki: der::spki(expected.to_bytes()),
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
    VerifyingKey::from_bytes(&der::spki_key(spki)?)
}

/// Checks the handshake signature of `spki` over `message`.
fn verify_signature(
    provider: &CryptoProvider,
    message: &[u8],
    spki: &CertificateDer<'_>,
    dss: &DigitallySignedStruct,
) -> Result<HandshakeSignatureValid, Error> {
    if dss.scheme != SCHEME {
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
        vec![SCHEME]
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
        vec![SCHEME]
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}
