# Shared by every spec's check.sh: the run sizes, the evaluator, and the
# sensitivity check. Sourced, not run.
#
# QUINT_SAMPLES  random runs per simulation          (default 2000)
# QUINT_STEPS    steps per random run                (default 40)
# QUINT_DEPTH    steps the model checker explores    (default 3)
# QUINT_TRACES   runs of each instance replayed against the code
#                                                    (default 100)

# Which checks run, the first argument of specs/check.sh: `all` (the
# default), `run` (the evaluator's: scenarios, mutants, simulations and
# trace validation) or `verify` (Apalache's bounded model checking). CI
# runs the two parts as two jobs.
part="${1:-all}"
case "$part" in
  all | run | verify) ;;
  *)
    echo "usage: specs/check.sh [all | run | verify]" >&2
    exit 2
    ;;
esac
runs() { [ "$part" = all ] || [ "$part" = "$1" ]; }

samples="${QUINT_SAMPLES:-2000}"
steps="${QUINT_STEPS:-40}"
depth="${QUINT_DEPTH:-3}"
traces="${QUINT_TRACES:-100}"
# The TypeScript evaluator ships with quint; the Rust one is fetched from
# GitHub's API on first use, which CI runners may be rate-limited on.
backend=(--backend typescript --verbosity 1)

scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT

step() { printf '\n== %s\n' "$*"; }

# Sensitivity: each mutant breaks one rule the protocol's safety rests on,
# and the scenarios of some instance must catch every one. A check that
# passes on a broken spec checks nothing.
#
# Usage: check_mutants SPEC INSTANCE... with the global array `mutants`
# holding "name|Perl substitution that breaks the rule".
check_mutants() {
  local spec="$1" mutant name main caught
  shift
  for mutant in "${mutants[@]}"; do
    name="${mutant%%|*}"
    step "mutant: $name"
    perl -0pe "${mutant#*|}" "$spec" > "$scratch/mutant.qnt"
    if cmp -s "$spec" "$scratch/mutant.qnt"; then
      echo "the substitution no longer matches the spec"
      return 1
    fi
    # It must still be a valid spec, or the scenarios fail for nothing.
    quint typecheck "$scratch/mutant.qnt" > /dev/null
    caught=no
    for main in "$@"; do
      if ! quint test "${backend[@]}" --main "$main" "$scratch/mutant.qnt" > /dev/null 2>&1; then
        caught="$main"
        break
      fi
    done
    if [ "$caught" = no ]; then
      echo "no scenario catches it"
      return 1
    fi
    echo "caught by the scenarios of $caught"
  done
}
