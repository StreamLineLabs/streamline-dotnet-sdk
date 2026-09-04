#!/usr/bin/env bash
#
# Hard-blocks release publication if a live conformance run executed zero tests, for
# example because the test filter matched nothing, or every matched test was skipped
# (an unreachable/misconfigured fixture skips rather than fails in most attribute
# implementations in this repo). A green `dotnet test` exit code alone does not
# guarantee this: dotnet test exits 0 when zero tests are collected or every collected
# test is skipped, so it cannot be trusted alone to authorize a release.
#
# This script reads the <Counters> element that MSTest/VSTest writes into a .trx
# results file and fails unless at least one test actually executed.
#
# Usage:
#   scripts/assert-executed-conformance-tests.sh <path-to-trx-file>

set -euo pipefail

trx_file="${1:-}"

fail() {
  echo "::error::$1" >&2
  exit 1
}

if [[ -z "$trx_file" ]]; then
  fail "Usage: assert-executed-conformance-tests.sh <trx-file>"
fi

if [[ ! -f "$trx_file" ]]; then
  fail "Conformance test results file '${trx_file}' was not produced. Treating this as zero executed tests; it cannot authorize release publication."
fi

counters_line="$(grep -o '<Counters[^/]*/>' "$trx_file" | head -n1 || true)"
if [[ -z "$counters_line" ]]; then
  fail "Could not find a <Counters> element in '${trx_file}'; cannot verify the executed test count."
fi

extract_attr() {
  local attr="$1"
  local value
  value="$(echo "$counters_line" | grep -o "${attr}=\"[0-9]*\"" | head -n1 | grep -o '[0-9]*' || true)"
  echo "${value:-0}"
}

total="$(extract_attr total)"
executed="$(extract_attr executed)"

if [[ "$total" -eq 0 ]]; then
  fail "The conformance run matched zero tests (total=0). A zero-test run cannot authorize release publication."
fi

if [[ "$executed" -eq 0 ]]; then
  fail "The conformance run executed zero of ${total} matched tests (all skipped/not-run). An all-skipped run cannot authorize release publication."
fi

echo "Conformance run executed ${executed}/${total} tests."
