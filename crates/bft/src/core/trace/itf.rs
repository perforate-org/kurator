//! The states of `specs/bft/bft.qnt`, read from a trace in the Informal
//! Trace Format (ITF): JSON in which an integer is `{"#bigint": ".."}`, a
//! set `{"#set": [..]}`, a map `{"#map": [[key, value], ..]}` and a sum
//! type `{"tag": .., "value": ..}`.

use serde_json::Value;

use super::{Block, Name, Proposal, Qc, Replica, State, Tc, Timeout, Vote};

impl State {
    /// Reads every state of `trace`.
    pub(super) fn all(trace: &Value) -> Vec<Self> {
        list(&trace["states"]).iter().map(Self::read).collect()
    }

    fn read(state: &Value) -> Self {
        let picks = state["mbt::nondetPicks"]
            .as_object()
            .expect("nondeterministic picks: run quint with --mbt")
            .iter()
            .filter(|(_, pick)| pick["tag"] == "Some")
            .map(|(name, pick)| (name.clone(), pick["value"].clone()))
            .collect();
        Self {
            replicas: pairs(var(state, "replicas"))
                .map(|(name, replica)| (text(name), Replica::read(replica)))
                .collect(),
            blocks: pairs(var(state, "blocks"))
                .map(|(id, block)| (int(id), Block::read(block)))
                .collect(),
            proposals: members(var(state, "proposals"))
                .map(Proposal::read)
                .collect(),
            votes: members(var(state, "votes")).map(Vote::read).collect(),
            timeouts: members(var(state, "timeouts")).map(Timeout::read).collect(),
            action: text(&state["mbt::actionTaken"]),
            picks,
        }
    }

    /// The replica the action stood for.
    pub(super) fn replica(&self) -> Name {
        text(self.pick("r"))
    }

    pub(super) fn proposal(&self) -> Proposal {
        Proposal::read(self.pick("p"))
    }

    pub(super) fn qc(&self) -> Qc {
        Qc::read(self.pick("q"))
    }

    pub(super) fn tc(&self) -> Tc {
        Tc::read(self.pick("tc"))
    }

    fn pick(&self, name: &str) -> &Value {
        self.picks
            .get(name)
            .unwrap_or_else(|| panic!("{} picked no {name}", self.action))
    }
}

impl Qc {
    fn read(qc: &Value) -> Self {
        Self {
            view: int(&qc["view"]),
            block: int(&qc["block"]),
        }
    }
}

impl Tc {
    fn read(tc: &Value) -> Self {
        Self {
            view: int(&tc["view"]),
            high: Qc::read(&tc["high"]),
        }
    }

    fn maybe(maybe: &Value) -> Option<Self> {
        match maybe["tag"].as_str() {
            Some("Entered") => Some(Self::read(&maybe["value"])),
            Some("NoTC") => None,
            other => panic!("not a MaybeTC: {other:?}"),
        }
    }
}

impl Replica {
    fn read(replica: &Value) -> Self {
        Self {
            view: int(&replica["view"]),
            voted: int(&replica["voted"]),
            lock: Qc::read(&replica["lock"]),
            proposed: int(&replica["proposed"]),
            entered: Tc::maybe(&replica["entered"]),
            decided: members(&replica["decided"]).map(int).collect(),
        }
    }
}

impl Block {
    fn read(block: &Value) -> Self {
        Self {
            view: int(&block["view"]),
            parent: int(&block["parent"]),
            justify: int(&block["justify"]),
            proposer: text(&block["proposer"]),
        }
    }
}

impl Proposal {
    fn read(proposal: &Value) -> Self {
        Self {
            block: int(&proposal["block"]),
            timeout: Tc::maybe(&proposal["timeout"]),
        }
    }
}

impl Vote {
    fn read(vote: &Value) -> Self {
        Self {
            voter: text(&vote["voter"]),
            view: int(&vote["view"]),
            block: int(&vote["block"]),
        }
    }
}

impl Timeout {
    fn read(timeout: &Value) -> Self {
        Self {
            voter: text(&timeout["voter"]),
            view: int(&timeout["view"]),
            high: Qc::read(&timeout["high"]),
        }
    }
}

/// The state variable `name`, whatever module path the trace gives it.
fn var<'a>(state: &'a Value, name: &str) -> &'a Value {
    let suffix = format!("::{name}");
    state
        .as_object()
        .and_then(|vars| {
            vars.iter()
                .find(|(key, _)| key.ends_with(&suffix) || *key == name)
        })
        .map_or_else(|| panic!("no variable {name}"), |(_, value)| value)
}

fn int(value: &Value) -> u64 {
    value["#bigint"]
        .as_str()
        .and_then(|digits| digits.parse().ok())
        .or_else(|| value.as_u64())
        .unwrap_or_else(|| panic!("not an integer: {value}"))
}

fn text(value: &Value) -> String {
    value
        .as_str()
        .unwrap_or_else(|| panic!("not a string: {value}"))
        .to_owned()
}

fn list(value: &Value) -> &[Value] {
    value
        .as_array()
        .map_or_else(|| panic!("not a list: {value}"), Vec::as_slice)
}

fn members(set: &Value) -> impl Iterator<Item = &Value> {
    list(&set["#set"]).iter()
}

fn pairs(map: &Value) -> impl Iterator<Item = (&Value, &Value)> {
    list(&map["#map"]).iter().map(|pair| (&pair[0], &pair[1]))
}
