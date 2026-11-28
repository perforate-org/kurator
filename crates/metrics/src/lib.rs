//! Metrics for OleaphDB: [`Histogram`]s of latencies that the hot paths
//! record into without locking, and an [`Exposition`] that writes them,
//! and plain values, in the Prometheus text format (version 0.0.4).
//!
//! ```
//! use std::time::Duration;
//!
//! use oleaph_metrics::{Exposition, Histogram, Histograms, Label, Values};
//!
//! const SYNCS: Histograms = Histograms::new("demo_sync_seconds", "How long a sync took.");
//! const TERM: Values = Values::gauge("demo_term", "The current term.");
//!
//! let syncs = Histogram::default();
//! syncs.record(Duration::from_micros(800));
//! let mut out = Exposition::default();
//! out.value(&TERM, &[Label::new("shard", &1)], 3);
//! out.histogram(&SYNCS, &[Label::new("shard", &1)], &syncs);
//! let text = out.into_text();
//! assert!(text.contains("demo_term{shard=\"1\"} 3\n"));
//! assert!(text.contains("demo_sync_seconds_bucket{shard=\"1\",le=\"0.001\"} 1\n"));
//! assert!(text.contains("demo_sync_seconds_count{shard=\"1\"} 1\n"));
//! ```

mod exposition;
mod histogram;

pub use exposition::{Exposition, Histograms, Label, Values};
pub use histogram::Histogram;
