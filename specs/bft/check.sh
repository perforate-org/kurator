#!/usr/bin/env bash
# The checks of bft.qnt, the consensus of crates/bft. Run through
# specs/check.sh, which runs every spec's checks.
set -euo pipefail
cd "$(dirname "$0")"
# shellcheck source=../common.sh
. ../common.sh
spec=bft.qnt

step "typecheck"
quint typecheck "$spec"

# The scenarios also show that the states which make `safety` worth
# checking are reachable: decisions, decisions after timeouts, certified
# forks, and in n4f2, with more faults than tolerated, a decided fork.
for main in n4f0 n4f1 n4f2; do
  step "scenarios of $main"
  quint test "${backend[@]}" --main "$main" "$spec"
done

# No mutant drops `b.justify >= tc.high.view` from `justified`: a replica
# observes the certificate a timeout certificate carries before it votes,
# so its lock rule refuses the same blocks, and no scenario can tell.
mutants=(
  "no lock rule|s/    b\.justify >= s\.lock\.view,/    true,/"
  "votes twice in a view|s/    s\.voted < b\.view,/    true,/"
  "quorum of 2f|s/Quorum: int = Replicas\.size\(\) - F/Quorum: int = Replicas.size() - F - 1/"
  "one-chain commit|s/\.union\(Set\(child\.parent\)\)/.union(Set(child.parent, q.block))/"
  "crash forgets the vote|s/      entered: NoTC,\n    \}\n    \/\/ It may/      entered: NoTC,\n      voted: 0,\n    }\n    \/\/ It may/"
  "crash forgets the proposal|s/      entered: NoTC,\n    \}\n    \/\/ It may/      entered: NoTC,\n      proposed: 0,\n    }\n    \/\/ It may/"
  "skip carries no certificate|s/leadIn\(r, observe\(replicas\.get\(r\), tc\.high\), now\)/(replicas.get(r), now)/"
)
check_mutants "$spec" n4f0 n4f1 n4f2

for main in n4f0 n4f1; do
  step "simulation of $main: safety"
  quint run "${backend[@]}" --main "$main" --invariant safety \
    --max-samples "$samples" --max-steps "$steps" "$spec"
done

# Trace validation: runs of the model, replayed step by step against the
# code (crates/bft/src/core/trace.rs), which must do what the model did.
mkdir "$scratch/traces"
for main in n4f0 n4f1; do
  quint run "${backend[@]}" --main "$main" --invariant safety --mbt \
    --max-samples "$traces" --n-traces "$traces" --max-steps "$steps" \
    --out-itf "$scratch/traces/$main-{seq}.itf.json" "$spec" > /dev/null
done
step "trace validation: $traces runs of n4f0 and of n4f1 replayed against crates/bft"
OLEAPH_BFT_TRACES="$scratch/traces" cargo test --locked -q -p oleaph-bft --lib core::trace

step "model checking of n4f1: safety in every run of $depth steps"
quint verify --main n4f1 --invariant safety --max-steps "$depth" --verbosity 1 "$spec"
