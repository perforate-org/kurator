#!/usr/bin/env bash
# Every check of the Quint specs, as CI runs them. Needs `quint` on the
# PATH and, for `quint verify`, Java 17 or later.
#
# QUINT_SAMPLES  random runs per simulation          (default 2000)
# QUINT_STEPS    steps per random run                (default 40)
# QUINT_DEPTH    steps the model checker explores    (default 3)
# QUINT_TRACES   runs of each instance replayed against crates/bft
#                                                    (default 100)
#
# Sensitivity: each mutant below breaks one rule the protocol's safety
# rests on, and the scenarios must catch every one. A check that passes on
# a broken spec checks nothing.
set -euo pipefail

samples="${QUINT_SAMPLES:-2000}"
steps="${QUINT_STEPS:-40}"
depth="${QUINT_DEPTH:-3}"
traces="${QUINT_TRACES:-100}"
# The TypeScript evaluator ships with quint; the Rust one is fetched from
# GitHub's API on first use, which CI runners may be rate-limited on.
backend=(--backend typescript --verbosity 1)

cd "$(dirname "$0")/bft"
spec=bft.qnt

step() { printf '\n== %s\n' "$*"; }

step "typecheck"
quint typecheck "$spec"

# The scenarios also show that the states which make `safety` worth
# checking are reachable: decisions, decisions after timeouts, certified
# forks, and in n4f2, with more faults than tolerated, a decided fork.
for main in n4f0 n4f1 n4f2; do
  step "scenarios of $main"
  quint test "${backend[@]}" --main "$main" "$spec"
done

# name | Perl substitution that breaks the rule
mutants=(
  "no lock rule|s/    b\.justify >= s\.lock\.view,/    true,/"
  "votes twice in a view|s/    s\.voted < b\.view,/    true,/"
  "quorum of 2f|s/Quorum: int = Replicas\.size\(\) - F/Quorum: int = Replicas.size() - F - 1/"
  "one-chain commit|s/\.union\(Set\(child\.parent\)\)/.union(Set(child.parent, q.block))/"
  "crash forgets the vote|s/      proposed: s\.voted,/      proposed: s.voted,\n      voted: 0,/"
  "timeout certificate ignores high|s/ and b\.justify >= tc\.high//"
)
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
for mutant in "${mutants[@]}"; do
  name="${mutant%%|*}"
  step "mutant: $name"
  perl -0pe "${mutant#*|}" "$spec" > "$scratch/mutant.qnt"
  if cmp -s "$spec" "$scratch/mutant.qnt"; then
    echo "the substitution no longer matches the spec"
    exit 1
  fi
  # It must still be a valid spec, or the scenarios fail for nothing.
  quint typecheck "$scratch/mutant.qnt" > /dev/null
  caught=no
  for main in n4f0 n4f1 n4f2; do
    if ! quint test "${backend[@]}" --main "$main" "$scratch/mutant.qnt" > /dev/null 2>&1; then
      caught="$main"
      break
    fi
  done
  if [ "$caught" = no ]; then
    echo "no scenario catches it"
    exit 1
  fi
  echo "caught by the scenarios of $caught"
done

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
