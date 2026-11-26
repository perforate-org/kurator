//! Writes, and how they are encoded as log records.

use std::io::Write as _;

use kurator_codec::{DecodeError, Reader, put_bytes};

use crate::error::{Error, Result};

/// One change to the key-value model.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum KvOp {
    /// Sets `key` to `value`.
    Put {
        /// The key.
        key: Vec<u8>,
        /// The new value.
        value: Vec<u8>,
    },
    /// Removes `key`, if it is there.
    Delete {
        /// The key.
        key: Vec<u8>,
    },
}

/// Changes that are applied together or not at all, in order.
///
/// ```
/// use kurator_storage::WriteBatch;
///
/// let batch = WriteBatch::new().put("a", "1").delete("b");
/// assert_eq!(batch.ops().len(), 2);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteBatch {
    ops: Vec<KvOp>,
}

impl WriteBatch {
    /// An empty batch.
    #[must_use]
    pub const fn new() -> Self {
        Self { ops: Vec::new() }
    }

    /// Adds a write of `value` to `key`.
    #[must_use]
    pub fn put(mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        self.ops.push(KvOp::Put {
            key: key.into(),
            value: value.into(),
        });
        self
    }

    /// Adds a removal of `key`.
    #[must_use]
    pub fn delete(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.ops.push(KvOp::Delete { key: key.into() });
        self
    }

    /// The changes, in order.
    #[must_use]
    pub fn ops(&self) -> &[KvOp] {
        &self.ops
    }

    /// Consumes the batch, yielding its changes in order.
    pub(crate) fn into_ops(self) -> impl Iterator<Item = KvOp> {
        self.ops.into_iter()
    }
}

/// Everything that can be written to the log.
///
/// Later phases add one variant per data model (documents, graph, ...);
/// the model tag in the encoding keeps their records apart.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Command {
    /// Changes to the key-value model.
    Kv(WriteBatch),
}

/// A [`Command`] together with its log payload, ready to be committed.
///
/// Only [`Command::encode`] and [`Encoded::decode`] make one, so a payload
/// always fits one log record and decodes back to its command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Encoded {
    command: Command,
    payload: Vec<u8>,
}

impl Encoded {
    /// Decodes `payload`, keeping it alongside its command.
    ///
    /// # Errors
    ///
    /// When `payload` is not something [`Command::encode`] wrote.
    pub fn decode(payload: Vec<u8>) -> Result<Self, DecodeError> {
        Ok(Self {
            command: Command::decode(&payload)?,
            payload,
        })
    }

    /// The bytes that go into the log.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// The command.
    #[must_use]
    pub const fn command(&self) -> &Command {
        &self.command
    }

    /// Consumes the encoding, keeping the command.
    #[must_use]
    pub fn into_command(self) -> Command {
        self.command
    }
}

const VERSION: u8 = 1;
const MODEL_KV: u8 = 1;
const OP_PUT: u8 = 1;
const OP_DELETE: u8 = 2;

impl Command {
    /// Encodes the command as `version u8 | model u8 | body`. A key-value
    /// body is `count u32` followed by that many `op u8 | key | value?`,
    /// keys and values length prefixed.
    ///
    /// # Errors
    ///
    /// [`Error::EmptyBatch`] and [`Error::BatchTooLarge`].
    pub fn encode(self) -> Result<Encoded> {
        let Self::Kv(batch) = &self;
        if batch.ops.is_empty() {
            return Err(Error::EmptyBatch);
        }
        let count = u32::try_from(batch.ops.len()).map_err(|_| Error::BatchTooLarge)?;
        let mut payload = vec![VERSION, MODEL_KV];
        payload.extend_from_slice(&count.to_le_bytes());
        let too_large = |_| Error::BatchTooLarge;
        for op in &batch.ops {
            match op {
                KvOp::Put { key, value } => {
                    payload.write_all(&[OP_PUT]).map_err(too_large)?;
                    put_bytes(&mut payload, key).map_err(too_large)?;
                    put_bytes(&mut payload, value).map_err(too_large)?;
                }
                KvOp::Delete { key } => {
                    payload.write_all(&[OP_DELETE]).map_err(too_large)?;
                    put_bytes(&mut payload, key).map_err(too_large)?;
                }
            }
        }
        if payload.len() > kurator_wal::MAX_PAYLOAD_LEN {
            return Err(Error::BatchTooLarge);
        }
        Ok(Encoded {
            command: self,
            payload,
        })
    }

    /// Decodes a payload written by [`Command::encode`].
    ///
    /// # Errors
    ///
    /// When `payload` is truncated, has bytes left over, or names a version,
    /// model or operation this build does not know.
    pub fn decode(payload: &[u8]) -> Result<Self, DecodeError> {
        let mut reader = Reader::new(payload);
        match reader.u8()? {
            VERSION => {}
            other => return Err(DecodeError::Version(other)),
        }
        match reader.u8()? {
            MODEL_KV => {}
            other => return Err(DecodeError::Tag(other)),
        }
        let mut ops = Vec::new();
        for _ in 0..reader.u32()? {
            let tag = reader.u8()?;
            let key = reader.bytes()?.to_vec();
            ops.push(match tag {
                OP_PUT => KvOp::Put {
                    key,
                    value: reader.bytes()?.to_vec(),
                },
                OP_DELETE => KvOp::Delete { key },
                other => return Err(DecodeError::Tag(other)),
            });
        }
        reader.finish()?;
        Ok(Self::Kv(WriteBatch { ops }))
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{Command, Encoded, KvOp, WriteBatch};
    use crate::error::Error;
    use kurator_codec::DecodeError;

    fn op() -> impl Strategy<Value = KvOp> {
        let bytes = || proptest::collection::vec(any::<u8>(), 0..16);
        prop_oneof![
            (bytes(), bytes()).prop_map(|(key, value)| KvOp::Put { key, value }),
            bytes().prop_map(|key| KvOp::Delete { key }),
        ]
    }

    #[test]
    fn empty_batches_are_rejected() {
        assert!(
            matches!(
                Command::Kv(WriteBatch::new()).encode(),
                Err(Error::EmptyBatch)
            ),
            "empty"
        );
    }

    #[test]
    fn unknown_versions_models_and_ops_are_rejected() {
        let payload = Command::Kv(WriteBatch::new().delete("k"))
            .encode()
            .unwrap()
            .payload()
            .to_vec();
        for (at, error) in [
            (0, DecodeError::Version(0xFF)),
            (1, DecodeError::Tag(0xFF)),
            (6, DecodeError::Tag(0xFF)),
        ] {
            let mut bad = payload.clone();
            bad[at] = 0xFF;
            assert_eq!(Command::decode(&bad), Err(error), "byte {at}");
        }
        assert_eq!(
            Command::decode(&payload[..payload.len() - 1]),
            Err(DecodeError::Truncated),
            "short"
        );
    }

    #[test]
    fn a_batch_may_fill_a_whole_record() {
        // version, model, count, op tag, key length, "k", value length
        let overhead = 1 + 1 + 4 + 1 + 4 + 1 + 4;
        let value = vec![0; kurator_wal::MAX_PAYLOAD_LEN - overhead];
        let encoded = Command::Kv(WriteBatch::new().put("k", value))
            .encode()
            .unwrap();
        assert_eq!(
            encoded.payload().len(),
            kurator_wal::MAX_PAYLOAD_LEN,
            "exactly full"
        );
    }

    #[test]
    fn oversized_batches_are_rejected() {
        let value = vec![0; kurator_wal::MAX_PAYLOAD_LEN - 15];
        assert!(
            matches!(
                Command::Kv(WriteBatch::new().put("k", value)).encode(),
                Err(Error::BatchTooLarge)
            ),
            "too large"
        );
    }

    proptest! {
        #[test]
        fn roundtrip(ops in proptest::collection::vec(op(), 1..8)) {
            let command = Command::Kv(WriteBatch { ops });
            let encoded = command.clone().encode().unwrap();
            prop_assert_eq!(Command::decode(encoded.payload()), Ok(command));
            prop_assert_eq!(Encoded::decode(encoded.payload().to_vec()), Ok(encoded));
        }

        #[test]
        fn arbitrary_payloads_never_panic(payload in proptest::collection::vec(any::<u8>(), 0..64)) {
            drop(Command::decode(&payload));
        }
    }
}
