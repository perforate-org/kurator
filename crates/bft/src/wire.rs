//! Encoding the parts every message shares: node ids, views, block ids and
//! signatures.

use std::io::{self, Write};

use kurator_codec::{DecodeError, Reader, put_bytes};
use kurator_consensus::NodeId;
use kurator_crypto::{Algorithm, Signature};

use crate::view::View;

/// Appends `node` as a `u64`.
pub(crate) fn put_node(out: &mut dyn Write, node: NodeId) -> io::Result<()> {
    out.write_all(&node.get().to_le_bytes())
}

/// Reads a node id written by [`put_node`].
pub(crate) fn read_node(reader: &mut Reader<'_>) -> Result<NodeId, DecodeError> {
    Ok(NodeId::new(reader.read()?))
}

/// Appends `view` as a `u64`.
pub(crate) fn put_view(out: &mut dyn Write, view: View) -> io::Result<()> {
    out.write_all(&view.get().to_le_bytes())
}

/// Reads a view written by [`put_view`].
pub(crate) fn read_view(reader: &mut Reader<'_>) -> Result<View, DecodeError> {
    Ok(View::new(reader.read()?))
}

/// Appends `signature` as `algorithm u8 | len u32 | bytes`.
pub(crate) fn put_signature(out: &mut dyn Write, signature: &Signature) -> io::Result<()> {
    out.write_all(&[signature.algorithm().id()])?;
    put_bytes(out, signature.as_bytes())
}

/// Reads a signature written by [`put_signature`].
pub(crate) fn read_signature(reader: &mut Reader<'_>) -> Result<Signature, DecodeError> {
    let id = reader.read::<u8>()?;
    let algorithm = Algorithm::from_id(id).ok_or(DecodeError::Tag(id))?;
    Signature::from_parts(algorithm, reader.bytes()?).ok_or(DecodeError::Invalid)
}

/// Appends how many items follow, as a `u32`.
pub(crate) fn put_count(out: &mut dyn Write, count: usize) -> io::Result<()> {
    let count = u32::try_from(count).map_err(|_| io::Error::other(TooMany(count)))?;
    out.write_all(&count.to_le_bytes())
}

/// Reads a count written by [`put_count`].
pub(crate) fn read_count(reader: &mut Reader<'_>) -> Result<u32, DecodeError> {
    reader.read()
}

/// Appends `commands`: their count, then each with its length.
pub(crate) fn put_commands(out: &mut dyn Write, commands: &[Vec<u8>]) -> io::Result<()> {
    put_count(out, commands.len())?;
    commands
        .iter()
        .try_for_each(|command| put_bytes(out, command))
}

/// Reads commands written by [`put_commands`].
pub(crate) fn read_commands(reader: &mut Reader<'_>) -> Result<Vec<Vec<u8>>, DecodeError> {
    let count = read_count(reader)?;
    let mut commands = Vec::new();
    for _ in 0..count {
        commands.push(reader.bytes()?.to_vec());
    }
    Ok(commands)
}

/// A list too long for its `u32` count.
#[derive(Debug, thiserror::Error)]
#[error("{0} items are too many to encode")]
struct TooMany(usize);
