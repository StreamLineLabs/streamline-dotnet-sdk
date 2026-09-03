#!/usr/bin/env bats
#
# Focused tests for scripts/assert-executed-conformance-tests.sh: the executed-test-
# count guard that stops a zero-test or all-skipped conformance run from authorizing
# release publication, even though `dotnet test` itself exits 0 in those cases.

setup() {
  SCRIPT="$BATS_TEST_DIRNAME/../assert-executed-conformance-tests.sh"
  WORKDIR="$(mktemp -d)"
}

teardown() {
  rm -rf "$WORKDIR"
}

write_trx() {
  local path="$1" counters="$2"
  cat > "$path" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<TestRun><ResultSummary><Counters ${counters} /></ResultSummary></TestRun>
EOF
}

@test "hard-blocks when the trx file is missing" {
  run "$SCRIPT" "$WORKDIR/does-not-exist.trx"
  [ "$status" -eq 1 ]
  [[ "$output" == *"was not produced"* ]]
}

@test "hard-blocks with no argument" {
  run "$SCRIPT"
  [ "$status" -eq 1 ]
  [[ "$output" == *"Usage:"* ]]
}

@test "hard-blocks a zero-total run (filter matched nothing)" {
  trx="$WORKDIR/zero-total.trx"
  write_trx "$trx" 'total="0" executed="0" passed="0" failed="0"'

  run "$SCRIPT" "$trx"
  [ "$status" -eq 1 ]
  [[ "$output" == *"matched zero tests"* ]]
}

@test "hard-blocks an all-skipped run (matched tests, zero executed)" {
  trx="$WORKDIR/all-skipped.trx"
  write_trx "$trx" 'total="46" executed="0" passed="0" failed="0"'

  run "$SCRIPT" "$trx"
  [ "$status" -eq 1 ]
  [[ "$output" == *"executed zero of 46 matched tests"* ]]
}

@test "hard-blocks when no <Counters> element is present" {
  trx="$WORKDIR/no-counters.trx"
  echo "<TestRun><ResultSummary></ResultSummary></TestRun>" > "$trx"

  run "$SCRIPT" "$trx"
  [ "$status" -eq 1 ]
  [[ "$output" == *"Could not find a <Counters> element"* ]]
}

@test "passes when at least one test executed" {
  trx="$WORKDIR/executed.trx"
  write_trx "$trx" 'total="46" executed="6" passed="6" failed="0"'

  run "$SCRIPT" "$trx"
  [ "$status" -eq 0 ]
  [[ "$output" == *"executed 6/46 tests"* ]]
}

@test "hard-blocks a run with executed failures alone if executed is zero (defense in depth)" {
  # executed=0 must block regardless of any other counter values.
  trx="$WORKDIR/weird.trx"
  write_trx "$trx" 'total="10" executed="0" passed="0" failed="0" notExecuted="10"'

  run "$SCRIPT" "$trx"
  [ "$status" -eq 1 ]
  [[ "$output" == *"all skipped/not-run"* ]]
}
