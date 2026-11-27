//! The Prometheus text format: each family's help and type, then its
//! samples, one a line.
//!
//! ```text
//! # HELP <name> <help>
//! # TYPE <name> gauge|counter|histogram
//! <name>{<label>="<value>",...} <value>
//! ```

use std::fmt::{self, Write as _};

use crate::histogram::Histogram;

/// What a family of plain values counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// A value that goes up and down.
    Gauge,
    /// A value that only goes up, until the process restarts.
    Counter,
}

/// A family of plain values: its name, its help, and whether it is a
/// gauge or a counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Values {
    name: &'static str,
    help: &'static str,
    kind: Kind,
}

impl Values {
    /// A family of values that go up and down.
    #[must_use]
    pub const fn gauge(name: &'static str, help: &'static str) -> Self {
        Self {
            name,
            help,
            kind: Kind::Gauge,
        }
    }

    /// A family of values that only go up.
    #[must_use]
    pub const fn counter(name: &'static str, help: &'static str) -> Self {
        Self {
            name,
            help,
            kind: Kind::Counter,
        }
    }
}

/// A family of [`Histogram`]s of seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Histograms {
    name: &'static str,
    help: &'static str,
}

impl Histograms {
    /// A family named `name`, which should end in `_seconds`.
    #[must_use]
    pub const fn new(name: &'static str, help: &'static str) -> Self {
        Self { name, help }
    }
}

/// One label of a sample: a name and its value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Label {
    name: &'static str,
    value: String,
}

impl Label {
    /// The label `name` with the value `value`.
    #[must_use]
    pub fn new(name: &'static str, value: &impl fmt::Display) -> Self {
        Self {
            name,
            value: value.to_string(),
        }
    }
}

/// A page of metrics in the Prometheus text format.
///
/// Samples may come in any order: each family's are kept together, after
/// its help and type, as the format requires, and families stay in the
/// order they first came.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Exposition {
    /// Each family's name and its lines so far.
    families: Vec<(&'static str, String)>,
}

impl Exposition {
    /// Adds the sample `value`, labelled `labels`, to the family `family`.
    pub fn value(&mut self, family: &Values, labels: &[Label], value: u64) {
        let kind = match family.kind {
            Kind::Gauge => "gauge",
            Kind::Counter => "counter",
        };
        if let Some(lines) = self.family(family.name, family.help, kind) {
            sample(lines, family.name, labels, &value.to_string());
        }
    }

    /// Adds `histogram`, of seconds, labelled `labels`, to the family
    /// `family`.
    pub fn histogram(&mut self, family: &Histograms, labels: &[Label], histogram: &Histogram) {
        let Some(lines) = self.family(family.name, family.help, "histogram") else {
            return;
        };
        let bucket = format!("{}_bucket", family.name);
        for (bound, below) in histogram.cumulative() {
            let le = labelled(labels, Label::new("le", &seconds(bound)));
            sample(lines, &bucket, &le, &below.to_string());
        }
        let count = histogram.count().to_string();
        let every = labelled(labels, Label::new("le", &"+Inf"));
        sample(lines, &bucket, &every, &count);
        let sum = seconds(histogram.sum_micros());
        sample(lines, &format!("{}_sum", family.name), labels, &sum);
        sample(lines, &format!("{}_count", family.name), labels, &count);
    }

    /// The page.
    #[must_use]
    pub fn into_text(self) -> String {
        self.families.into_iter().map(|(_, lines)| lines).collect()
    }

    /// The lines of the family `name`, begun with its help and type the
    /// first time.
    fn family(&mut self, name: &'static str, help: &str, kind: &str) -> Option<&mut String> {
        if !self.families.iter().any(|(known, _)| *known == name) {
            let mut head = String::new();
            let _help = writeln!(head, "# HELP {name} {}", escape(help, Escape::Help));
            let _kind = writeln!(head, "# TYPE {name} {kind}");
            self.families.push((name, head));
        }
        self.families
            .iter_mut()
            .find(|(known, _)| *known == name)
            .map(|(_, lines)| lines)
    }
}

/// Writes one sample of `name` to `lines`.
fn sample(lines: &mut String, name: &str, labels: &[Label], value: &str) {
    lines.push_str(name);
    if !labels.is_empty() {
        let pairs: Vec<String> = labels
            .iter()
            .map(|label| format!("{}=\"{}\"", label.name, escape(&label.value, Escape::Value)))
            .collect();
        let _labels = write!(lines, "{{{}}}", pairs.join(","));
    }
    let _value = writeln!(lines, " {value}");
}

/// `labels` and then `last`.
fn labelled(labels: &[Label], last: Label) -> Vec<Label> {
    labels.iter().cloned().chain([last]).collect()
}

/// `micros` microseconds as seconds, with no more digits than it needs.
fn seconds(micros: u64) -> String {
    let whole = micros.checked_div(1_000_000).unwrap_or_default();
    let fraction = micros.checked_rem(1_000_000).unwrap_or_default();
    if fraction == 0 {
        return whole.to_string();
    }
    let digits = format!("{fraction:06}");
    format!("{whole}.{}", digits.trim_end_matches('0'))
}

/// Where a text goes, which decides what is escaped in it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Escape {
    /// A family's help: backslashes and line breaks.
    Help,
    /// A label's value, between double quotes: those too.
    Value,
}

/// `text` escaped for where it goes.
fn escape(text: &str, place: Escape) -> String {
    let mut escaped = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '"' if place == Escape::Value => escaped.push_str("\\\""),
            other => escaped.push(other),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Escape, Exposition, Histograms, Label, Values, escape, seconds};
    use crate::histogram::Histogram;

    #[test]
    fn seconds_take_the_digits_they_need() {
        assert_eq!(seconds(50), "0.00005", "fifty microseconds");
        assert_eq!(seconds(2_500_000), "2.5", "two and a half seconds");
        assert_eq!(seconds(10_000_000), "10", "ten seconds");
        assert_eq!(seconds(0), "0", "none");
    }

    #[test]
    fn labels_and_help_are_escaped() {
        assert_eq!(
            escape("a\\b\n\"c\"", Escape::Value),
            "a\\\\b\\n\\\"c\\\"",
            "a label"
        );
        assert_eq!(escape("\"c\"\n", Escape::Help), "\"c\"\\n", "help");
    }

    #[test]
    fn families_are_written_whole_with_their_type() {
        let total = Values::counter("demo_total", "Things\ncounted.");
        let took = Histograms::new("demo_seconds", "Took.");
        let histogram = Histogram::default();
        histogram.record(Duration::from_millis(3));
        let mut out = Exposition::default();
        out.value(&total, &[], 7);
        out.histogram(&took, &[Label::new("shard", &1)], &histogram);
        out.value(
            &total,
            &[Label::new("shard", &2), Label::new("node", &"a\"b")],
            9,
        );
        let text = out.into_text();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(
            lines.get(..6),
            Some(
                [
                    "# HELP demo_total Things\\ncounted.",
                    "# TYPE demo_total counter",
                    "demo_total 7",
                    "demo_total{shard=\"2\",node=\"a\\\"b\"} 9",
                    "# HELP demo_seconds Took.",
                    "# TYPE demo_seconds histogram",
                ]
                .as_slice()
            ),
            "{text}"
        );
        for expected in [
            "demo_seconds_bucket{shard=\"1\",le=\"0.0025\"} 0\n",
            "demo_seconds_bucket{shard=\"1\",le=\"0.005\"} 1\n",
            "demo_seconds_bucket{shard=\"1\",le=\"+Inf\"} 1\n",
            "demo_seconds_sum{shard=\"1\"} 0.003\n",
        ] {
            assert!(text.contains(expected), "{expected} in {text}");
        }
        assert!(
            text.ends_with("demo_seconds_count{shard=\"1\"} 1\n"),
            "{text}"
        );
    }
}
