//! Writes: changes to the key-value model, applied together.

use kurator_codec::{DecodeError, Reader, put_bytes};

use crate::error::{Error, Result};
use crate::span::Span;

const OP_PUT: u8 = 1;
const OP_DELETE: u8 = 2;

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

impl KvOp {
    /// The key it changes.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Put { key, .. } | Self::Delete { key } => key,
        }
    }
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

    /// Whether it changes nothing.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Whether every key it changes is in `span`.
    #[must_use]
    pub fn fits(&self, span: &Span) -> bool {
        self.ops.iter().all(|op| span.contains(op.key()))
    }

    /// Consumes the batch, yielding its changes in order.
    pub(crate) fn into_ops(self) -> impl Iterator<Item = KvOp> {
        self.ops.into_iter()
    }

    /// Appends `count u32 | (op u8 | key | value?)*`: put 1 with a value,
    /// delete 2 without; keys and values length prefixed.
    pub(crate) fn encode(&self, out: &mut Vec<u8>) -> Result<()> {
        let count = u32::try_from(self.ops.len()).map_err(|_| Error::BatchTooLarge)?;
        out.extend_from_slice(&count.to_le_bytes());
        for op in &self.ops {
            match op {
                KvOp::Put { key, value } => {
                    out.push(OP_PUT);
                    put_bytes(out, key).map_err(Error::too_large)?;
                    put_bytes(out, value).map_err(Error::too_large)?;
                }
                KvOp::Delete { key } => {
                    out.push(OP_DELETE);
                    put_bytes(out, key).map_err(Error::too_large)?;
                }
            }
        }
        Ok(())
    }

    /// Reads a batch written by [`WriteBatch::encode`].
    pub(crate) fn decode(reader: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let mut ops = Vec::new();
        for _ in 0..reader.read::<u32>()? {
            let tag = reader.read::<u8>()?;
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
        Ok(Self { ops })
    }
}
