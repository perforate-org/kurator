#!/usr/bin/env bash
# The checks of txn.qnt, the two-phase commit of crates/storage and
# crates/client. Run through specs/check.sh, which runs every spec's checks.
set -euo pipefail
cd "$(dirname "$0")"
# shellcheck source=../common.sh
. ../common.sh
spec=txn.qnt

if runs run; then
  step "typecheck"
  quint typecheck "$spec"

  # The scenarios also show that the states which make `safety` worth
  # checking are reachable: commits across shards, a coordinator that
  # stops before and after the commit point, conflicts that abort, and a
  # read-only transaction that meets a lock left after a commit point.
  step "scenarios of two"
  quint test "${backend[@]}" --main two "$spec"

  mutants=(
    "read-only commits unconfirmed|s/val confirmed = not\(conflict\(s, t\)\)/val confirmed = true/"
    "prepare ignores changed reads|s/val changed = reads\.exists/val changed = false and reads.exists/"
    "prepare ignores locks|s/val locked = shards/val locked = false and shards/"
    "resolve ignores the primary|s/s, t, atPrimary\.result\)/s, t, Abort)/"
    "commits before every prepare|s/c\.prepared == shardsOf\(t\)/c.prepared.size() > 0/"
  )
  check_mutants "$spec" two

  step "simulation of two: safety"
  quint run "${backend[@]}" --main two --invariant safety \
    --max-samples "$samples" --max-steps "$steps" "$spec"
fi

if runs verify; then
  step "model checking of two: safety in every run of $depth steps"
  quint verify --main two --invariant safety --max-steps "$depth" --verbosity 1 "$spec"
fi
