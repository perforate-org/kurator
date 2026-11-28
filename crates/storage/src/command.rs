//! Commands, and how they are encoded as log records.

use oleaph_codec::{DecodeError, Reader};

use crate::batch::WriteBatch;
use crate::error::{Error, Result};
use crate::span::Span;
use crate::two_phase::{Collect, Decide, Prepare};
use crate::txn::Transaction;

/// Everything that can be written to the log.
///
/// Later phases add one variant per data model (documents, graph, ...);
/// the kind tag in the encoding keeps their records apart.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Command {
    /// Changes to the key-value model, made whatever the state holds.
    Kv(WriteBatch),
    /// Changes to the key-value model, made only if nothing the
    /// transaction read changed since it read it.
    Txn(Transaction),
    /// The first phase of a transaction across shards, on this shard.
    Prepare(Prepare),
    /// The second phase of a transaction across shards, on this shard.
    Decide(Decide),
    /// A step of forgetting the decisions on transactions across shards.
    Collect(Collect),
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
const KIND_KV: u8 = 1;
const KIND_TXN: u8 = 2;
const KIND_PREPARE: u8 = 3;
const KIND_DECIDE: u8 = 4;
const KIND_COLLECT: u8 = 5;

impl Command {
    /// Whether everything the command reads or writes is in `span`, so a
    /// state that holds only `span` can apply it. A decision touches no
    /// key: the shard that prepared the transaction decides it, and
    /// neither does collecting.
    #[must_use]
    pub fn fits(&self, span: &Span) -> bool {
        match self {
            Self::Kv(batch) => batch.fits(span),
            Self::Txn(transaction) => transaction.fits(span),
            Self::Prepare(prepare) => prepare.fits(span),
            Self::Decide(_) | Self::Collect(_) => true,
        }
    }

    /// Encodes the command as `version u8 | kind u8 | body`: kind 1 is a
    /// write batch, as [`WriteBatch`] encodes it, kind 2 a transaction,
    /// its reads and then its writes, kind 3 a [`Prepare`], kind 4 a
    /// [`Decide`] and kind 5 a step of [`Collect`].
    ///
    /// # Errors
    ///
    /// [`Error::EmptyBatch`] when a write batch or a transaction writes
    /// nothing, and [`Error::BatchTooLarge`] when the payload does not fit
    /// one record.
    pub fn encode(self) -> Result<Encoded> {
        let mut payload = vec![VERSION];
        match &self {
            Self::Kv(batch) => {
                payload.push(KIND_KV);
                written(batch)?.encode(&mut payload)?;
            }
            Self::Txn(transaction) => {
                payload.push(KIND_TXN);
                let _writes: &WriteBatch = written(transaction.writes())?;
                transaction.encode(&mut payload)?;
            }
            Self::Prepare(prepare) => {
                payload.push(KIND_PREPARE);
                prepare.encode(&mut payload)?;
            }
            Self::Decide(decide) => {
                payload.push(KIND_DECIDE);
                decide.encode(&mut payload)?;
            }
            Self::Collect(step) => {
                payload.push(KIND_COLLECT);
                step.encode(&mut payload);
            }
        }
        if payload.len() > oleaph_wal::MAX_PAYLOAD_LEN {
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
    /// kind, operation or bound this build does not know.
    pub fn decode(payload: &[u8]) -> Result<Self, DecodeError> {
        let mut reader = Reader::new(payload);
        match reader.read::<u8>()? {
            VERSION => {}
            other => return Err(DecodeError::Version(other)),
        }
        let command = match reader.read::<u8>()? {
            KIND_KV => Self::Kv(WriteBatch::decode(&mut reader)?),
            KIND_TXN => Self::Txn(Transaction::decode(&mut reader)?),
            KIND_PREPARE => Self::Prepare(Prepare::decode(&mut reader)?),
            KIND_DECIDE => Self::Decide(Decide::decode(&mut reader)?),
            KIND_COLLECT => Self::Collect(Collect::decode(&mut reader)?),
            other => return Err(DecodeError::Tag(other)),
        };
        reader.finish()?;
        Ok(command)
    }
}

/// `batch`, if it writes something.
const fn written(batch: &WriteBatch) -> Result<&WriteBatch> {
    if batch.is_empty() {
        return Err(Error::EmptyBatch);
    }
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use std::collections::BTreeSet;
    use std::ops::Bound;

    use super::{Command, Encoded};
    use crate::across::{Deadline, TxnId};
    use crate::batch::{KvOp, WriteBatch};
    use crate::error::Error;
    use crate::two_phase::{Collect, Decide, Decision, Prepare};
    use crate::txn::{KeyRange, ReadSet, Transaction};
    use crate::version::Version;
    use oleaph_codec::DecodeError;

    fn bytes() -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(any::<u8>(), 0..16)
    }

    fn bound() -> impl Strategy<Value = Bound<Vec<u8>>> {
        prop_oneof![
            Just(Bound::Unbounded),
            bytes().prop_map(Bound::Included),
            bytes().prop_map(Bound::Excluded),
        ]
    }

    fn transaction() -> impl Strategy<Value = Transaction> {
        let range = (bound(), bound()).prop_map(|(start, end)| {
            KeyRange::new(
                start.as_ref().map(Vec::as_slice),
                end.as_ref().map(Vec::as_slice),
            )
        });
        (
            any::<u64>(),
            proptest::collection::btree_set(bytes(), 0..4),
            proptest::collection::vec(range, 0..4),
            proptest::collection::vec(op(), 1..4),
        )
            .prop_map(|(read_at, keys, ranges, ops)| {
                Transaction::new(
                    Version::new(read_at),
                    ReadSet::new(keys, ranges),
                    batch(ops),
                )
            })
    }

    fn batch(ops: Vec<KvOp>) -> WriteBatch {
        ops.into_iter()
            .fold(WriteBatch::new(), |batch, op| match op {
                KvOp::Put { key, value } => batch.put(key, value),
                KvOp::Delete { key } => batch.delete(key),
            })
    }

    fn op() -> impl Strategy<Value = KvOp> {
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
        let reads_only = Transaction::new(Version::ZERO, ReadSet::default(), WriteBatch::new());
        assert!(
            matches!(Command::Txn(reads_only).encode(), Err(Error::EmptyBatch)),
            "a transaction that writes nothing"
        );
    }

    #[test]
    fn transactions_encode_their_reads() {
        let reads = ReadSet::new(
            BTreeSet::from([b"k".to_vec()]),
            vec![KeyRange::new(Bound::Included(b"a"), Bound::Excluded(b"b"))],
        );
        let transaction = Transaction::new(Version::new(7), reads, WriteBatch::new().delete("k"));
        let payload = Command::Txn(transaction)
            .encode()
            .unwrap()
            .payload()
            .to_vec();
        assert_eq!(
            payload,
            [
                &[1, 2][..],
                &7_u64.to_le_bytes(),
                &[1, 0, 0, 0, 1, 0, 0, 0, b'k'],
                &[1, 0, 0, 0, 1, 1, 0, 0, 0, b'a', 2, 1, 0, 0, 0, b'b'],
                &[1, 0, 0, 0, 2, 1, 0, 0, 0, b'k'],
            ]
            .concat(),
            "layout"
        );
        let mut bad = payload;
        bad[23] = 0xFF;
        assert_eq!(Command::decode(&bad), Err(DecodeError::Tag(0xFF)), "bound");
    }

    #[test]
    fn unknown_versions_kinds_and_ops_are_rejected() {
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
        // version, kind, count, op tag, key length, "k", value length
        let overhead = 1 + 1 + 4 + 1 + 4 + 1 + 4;
        let value = vec![0; oleaph_wal::MAX_PAYLOAD_LEN - overhead];
        let encoded = Command::Kv(WriteBatch::new().put("k", value))
            .encode()
            .unwrap();
        assert_eq!(
            encoded.payload().len(),
            oleaph_wal::MAX_PAYLOAD_LEN,
            "exactly full"
        );
    }

    #[test]
    fn oversized_batches_are_rejected() {
        let value = vec![0; oleaph_wal::MAX_PAYLOAD_LEN - 15];
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
            let command = Command::Kv(batch(ops));
            let encoded = command.clone().encode().unwrap();
            prop_assert_eq!(Command::decode(encoded.payload()), Ok(command));
            prop_assert_eq!(Encoded::decode(encoded.payload().to_vec()), Ok(encoded));
        }

        #[test]
        fn transactions_roundtrip(transaction in transaction()) {
            let command = Command::Txn(transaction);
            let encoded = command.clone().encode().unwrap();
            prop_assert_eq!(Command::decode(encoded.payload()), Ok(command));
        }

        #[test]
        fn both_phases_roundtrip(
            id in any::<u128>(),
            primary in bytes(),
            transaction in transaction(),
            deadline in any::<u64>(),
            commits in any::<bool>(),
        ) {
            let decision = if commits { Decision::Commit } else { Decision::Abort };
            for command in [
                Command::Prepare(Prepare::new(TxnId::new(id), primary.clone(), Deadline::new(deadline), transaction)),
                Command::Decide(Decide::new(TxnId::new(id), primary, Deadline::new(deadline), decision)),
                Command::Collect(Collect::Fence(Deadline::new(deadline))),
                Command::Collect(Collect::Forget(Deadline::new(deadline))),
            ] {
                let encoded = command.clone().encode().unwrap();
                prop_assert_eq!(Command::decode(encoded.payload()), Ok(command));
            }
        }

        #[test]
        fn arbitrary_payloads_never_panic(payload in proptest::collection::vec(any::<u8>(), 0..64)) {
            drop(Command::decode(&payload));
        }
    }
}
