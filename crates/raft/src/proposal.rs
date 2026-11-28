//! Proposals: what a client asks a shard's log to hold.

use std::fmt;

use oleaph_codec::{DecodeError, Reader};
use oleaph_shard::Split;
use oleaph_storage::{Command, Encoded};

use crate::error::{Error, Result};

/// Bytes an entry record adds to its proposal: the tag, the log id and the
/// payload kind. The log codec's tests hold it to the encoding.
pub(crate) const ENTRY_OVERHEAD: usize = 1 + 24 + 1;

/// The application data of an entry: a command for the shard's keys, or a
/// split of the shard.
///
/// Only [`Proposal::new`], [`Proposal::split`] and the log decoder make
/// one, so a proposal always fits one log record together with its entry
/// header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal(Body);

/// What a proposal carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Body {
    /// A command, with its encoding.
    Command(Encoded),
    /// A split, with its encoding.
    Split(Split, Vec<u8>),
}

impl Proposal {
    /// Encodes `command` for the log.
    ///
    /// # Errors
    ///
    /// [`Error::Storage`] when the command is empty or too large to encode,
    /// and [`Error::ProposalTooLarge`] when it does not fit one log record
    /// together with its entry header.
    pub fn new(command: Command) -> Result<Self> {
        Self::fitting(Body::Command(command.encode()?))
    }

    /// Encodes `split` for the log.
    ///
    /// # Errors
    ///
    /// [`Error::ProposalTooLarge`] when the split key does not fit one log
    /// record.
    pub fn split(split: Split) -> Result<Self> {
        let mut encoded = Vec::new();
        split.encode(&mut encoded)?;
        Self::fitting(Body::Split(split, encoded))
    }

    fn fitting(body: Body) -> Result<Self> {
        let len = body.payload().len();
        if len.saturating_add(ENTRY_OVERHEAD) > oleaph_wal::MAX_PAYLOAD_LEN {
            return Err(Error::ProposalTooLarge { len });
        }
        Ok(Self(body))
    }

    /// Reads back a command's payload from the log.
    pub(crate) fn decode_command(payload: Vec<u8>) -> Result<Self, DecodeError> {
        Encoded::decode(payload).map(|encoded| Self(Body::Command(encoded)))
    }

    /// Reads back a split's payload from the log.
    pub(crate) fn decode_split(payload: Vec<u8>) -> Result<Self, DecodeError> {
        let mut reader = Reader::new(&payload);
        let split = Split::decode(&mut reader)?;
        reader.finish()?;
        Ok(Self(Body::Split(split, payload)))
    }

    /// What it carries.
    pub(crate) const fn body(&self) -> &Body {
        &self.0
    }

    /// Consumes the proposal, keeping what it carries.
    pub(crate) fn into_body(self) -> Body {
        self.0
    }
}

impl Body {
    /// The bytes that go into the log.
    pub(crate) fn payload(&self) -> &[u8] {
        match self {
            Self::Command(encoded) => encoded.payload(),
            Self::Split(_, encoded) => encoded,
        }
    }
}

impl fmt::Display for Proposal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let len = self.0.payload().len();
        match &self.0 {
            Body::Command(_) => write!(f, "proposal of {len} bytes"),
            Body::Split(split, _) => write!(f, "split of {} into {}", split.epoch(), split.right()),
        }
    }
}

#[cfg(test)]
mod tests {
    use oleaph_shard::{Epoch, ShardId, Split};
    use oleaph_storage::{Command, WriteBatch};

    use super::Proposal;
    use crate::error::Error;

    #[test]
    fn a_proposal_shows_what_it_carries() {
        let proposal = Proposal::new(Command::Kv(WriteBatch::new().put("k", "v"))).unwrap();
        let len = proposal.body().payload().len();
        assert_eq!(
            proposal.to_string(),
            format!("proposal of {len} bytes"),
            "command"
        );
        let split = Proposal::split(Split::new("m", ShardId::new(2), Epoch::ZERO)).unwrap();
        assert_eq!(split.to_string(), "split of e0 into s2", "split");
    }

    #[test]
    fn a_split_key_must_fit_one_record() {
        let at = vec![0; oleaph_wal::MAX_PAYLOAD_LEN];
        assert!(
            matches!(
                Proposal::split(Split::new(at, ShardId::new(2), Epoch::ZERO)),
                Err(Error::ProposalTooLarge { .. })
            ),
            "too large"
        );
    }

    #[test]
    fn a_proposal_that_fills_one_record_exactly_fits() {
        let split =
            |len: usize| Proposal::split(Split::new(vec![0; len], ShardId::new(2), Epoch::ZERO));
        let probe = 1 << 20;
        let header = split(probe).unwrap().body().payload().len() - probe;
        let fills = oleaph_wal::MAX_PAYLOAD_LEN - super::ENTRY_OVERHEAD - header;
        assert!(split(fills).is_ok(), "exactly one record");
        assert!(
            matches!(split(fills + 1), Err(Error::ProposalTooLarge { .. })),
            "one byte over"
        );
    }
}
