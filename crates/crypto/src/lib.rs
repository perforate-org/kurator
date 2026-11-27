//! Signature keys for Kurator, each tagged with its [`Algorithm`].
//!
//! Every key and signature knows which algorithm made it, and every
//! encoding of one names that algorithm first, so a cluster can move to
//! another algorithm, such as the post-quantum ML-DSA (FIPS 204), one
//! purpose at a time: a log signed with one algorithm stays readable after
//! new records are signed with another, and a membership can list peers
//! whose transport keys use different algorithms while they migrate.
//!
//! Today Ed25519 is the only algorithm. Adding one means a new
//! [`Algorithm`] variant, its key types behind [`SigningKey`] and
//! [`VerifyingKey`], its DER encodings, and a derivation context per
//! [`KeyPurpose`]; nothing that stores or sends keys changes shape.
//!
//! A node's keys all come from one [`NodeSecret`], one key per
//! [`KeyPurpose`].
//!
//! ```
//! use kurator_crypto::{KeyPurpose, NodeSecret, VerifyingKey};
//!
//! let secret = NodeSecret::from_seed([7; 32]);
//! let key = secret.signing_key(KeyPurpose::WalSigning);
//! let signature = key.sign(b"record");
//! let public: VerifyingKey = key.verifying_key().to_string().parse()?;
//! assert!(public.verify(b"record", &signature));
//! # Ok::<(), kurator_crypto::InvalidKey>(())
//! ```

mod algorithm;
mod der;
mod key;
mod secret;

pub use algorithm::Algorithm;
pub use key::{InvalidKey, Signature, SigningKey, VerifyingKey};
pub use secret::{KeyPurpose, NodeSecret};
