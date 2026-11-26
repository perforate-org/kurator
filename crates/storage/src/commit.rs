//! Group commit: one thread turns whatever writes are waiting into one
//! durable log append.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::thread::{self, JoinHandle};

use crate::command::Encoded;
use crate::core::Core;
use crate::error::{Error, Result};
use crate::outcome::Outcome;

type Reply = SyncSender<Result<Outcome>>;

/// The commit thread. Dropping it lets the thread finish the writes already
/// queued and waits for it.
#[derive(Debug)]
pub(crate) struct GroupCommit {
    requests: Option<Sender<(Encoded, Reply)>>,
    worker: Option<JoinHandle<()>>,
}

impl GroupCommit {
    /// Starts the commit thread for `core`, committing up to `max_batch`
    /// writes at a time.
    pub(crate) fn spawn(core: Arc<Core>, max_batch: NonZeroUsize) -> Result<Self> {
        let (requests, queue) = mpsc::channel();
        let worker = thread::Builder::new()
            .name("kurator-commit".to_owned())
            .spawn(move || run(&core, &queue, max_batch))?;
        Ok(Self {
            requests: Some(requests),
            worker: Some(worker),
        })
    }

    /// Queues `entry` and waits until it is durable and applied.
    pub(crate) fn commit(&self, entry: Encoded) -> Result<Outcome> {
        let (reply, answer) = mpsc::sync_channel(1);
        self.requests
            .as_ref()
            .ok_or(Error::Closed)?
            .send((entry, reply))
            .map_err(|_| Error::Closed)?;
        answer.recv().map_err(|_| Error::Closed)?
    }
}

impl Drop for GroupCommit {
    fn drop(&mut self) {
        drop(self.requests.take());
        if let Some(worker) = self.worker.take() {
            // A panic on the commit thread already failed its writers.
            drop(worker.join());
        }
    }
}

fn run(core: &Core, queue: &Receiver<(Encoded, Reply)>, max_batch: NonZeroUsize) {
    let more = max_batch.get().saturating_sub(1);
    while let Ok(first) = queue.recv() {
        let (entries, replies): (Vec<_>, Vec<_>) = std::iter::once(first)
            .chain(queue.try_iter().take(more))
            .unzip();
        match core.commit(entries) {
            Ok(outcomes) => {
                for (outcome, reply) in outcomes.into_iter().zip(replies) {
                    answer(&reply, Ok(outcome));
                }
            }
            Err(error) => {
                for reply in replies {
                    answer(&reply, Err(Error::Commit(Arc::clone(&error))));
                }
            }
        }
    }
}

/// Sends the outcome to a writer; a writer that stopped waiting needs none.
fn answer(reply: &Reply, outcome: Result<Outcome>) {
    drop(reply.send(outcome));
}
