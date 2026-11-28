#!/usr/bin/env bash
# Every check of the Quint specs, as CI runs them: each directory under
# specs/ holds one spec and its check.sh, which uses specs/common.sh (run
# sizes are set there). Needs `quint` on the PATH, Java 17 or later for
# `quint verify`, and the Rust toolchain for trace validation.
set -euo pipefail
cd "$(dirname "$0")"
for check in */check.sh; do
  printf '\n### %s\n' "${check%/check.sh}"
  bash "$check"
done
