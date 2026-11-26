//! Proposals: commands encoded for the Raft log.

use std::fmt;

use kurator_storage::{Command, Encoded};

use crate::error::{Error, Result};

/// Bytes an entry record adds to its command: the tag, the log id and the
/// payload kind. The log codec's tests hold it to the encoding.
pub(crate) const ENTRY_OVERHEAD: usize = 1 + 24 + 1;

/// A command on its way through the Raft log: the application data of an
/// entry.
///
/// Only [`Proposal::new`] and the log decoder make one, so a proposal
/// always fits one log record together with its entry header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal(Encoded);

impl Proposal {
    /// Encodes `command` for the log.
    ///
    /// # Errors
    ///
    /// [`Error::Storage`] when the command is empty or too large to encode,
    /// and [`Error::ProposalTooLarge`] when it does not fit one log record
    /// together with its entry header.
    pub fn new(command: Command) -> Result<Self> {
        let encoded = command.encode()?;
        let len = encoded.payload().len();
        if len.saturating_add(ENTRY_OVERHEAD) > kurator_wal::MAX_PAYLOAD_LEN {
            return Err(Error::ProposalTooLarge { len });
        }
        Ok(Self(encoded))
    }

    /// Wraps a payload read back from the log.
    pub(crate) const fn decoded(encoded: Encoded) -> Self {
        Self(encoded)
    }

    /// The encoded command.
    #[must_use]
    pub const fn encoded(&self) -> &Encoded {
        &self.0
    }

    /// Consumes the proposal, keeping the command.
    #[must_use]
    pub fn into_command(self) -> Command {
        self.0.into_command()
    }
}

impl fmt::Display for Proposal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "proposal of {} bytes", self.0.payload().len())
    }
}
