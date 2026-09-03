#!/usr/bin/env bats
#
# Focused tests for scripts/resolve-conformance-image.sh: the release-blocker gate
# that requires an explicit, immutable Streamline image digest before live
# conformance (and therefore publication) can proceed.

setup() {
  SCRIPT="$BATS_TEST_DIRNAME/../resolve-conformance-image.sh"
  WORKDIR="$(mktemp -d)"
  VALID_DIGEST="sha256:$(printf 'a%.0s' $(seq 1 64))"
}

teardown() {
  rm -rf "$WORKDIR"
}

@test "hard-blocks with no explicit input and no pin file" {
  run "$SCRIPT" "" "$WORKDIR/does-not-exist"
  [ "$status" -eq 1 ]
  [[ "$output" == *"No conformance image digest was supplied"* ]]
}

@test "hard-blocks when the committed pin file is only comments/placeholder" {
  pin_file="$WORKDIR/pin"
  {
    echo "# placeholder, not a real digest"
    echo ""
  } > "$pin_file"

  run "$SCRIPT" "" "$pin_file"
  [ "$status" -eq 1 ]
  [[ "$output" == *"No conformance image digest was supplied"* ]]
}

@test "hard-blocks an explicit :latest tag" {
  run "$SCRIPT" "ghcr.io/streamlinelabs/streamline:latest"
  [ "$status" -eq 1 ]
  [[ "$output" == *"mutable ':latest' reference"* ]]
}

@test "hard-blocks a floating tag with no digest" {
  run "$SCRIPT" "ghcr.io/streamlinelabs/streamline:0.3.0"
  [ "$status" -eq 1 ]
  [[ "$output" == *"not a valid immutable digest reference"* ]]
}

@test "hard-blocks a tag+digest combination (digest-only is required)" {
  run "$SCRIPT" "ghcr.io/streamlinelabs/streamline:0.3.0@${VALID_DIGEST}"
  [ "$status" -eq 1 ]
  [[ "$output" == *"not a valid immutable digest reference"* ]]
}

@test "hard-blocks a malformed (too short) digest" {
  run "$SCRIPT" "ghcr.io/streamlinelabs/streamline@sha256:abcd"
  [ "$status" -eq 1 ]
  [[ "$output" == *"not a valid immutable digest reference"* ]]
}

@test "hard-blocks an uppercase-hex digest" {
  upper="sha256:$(printf 'A%.0s' $(seq 1 64))"
  run "$SCRIPT" "ghcr.io/streamlinelabs/streamline@${upper}"
  [ "$status" -eq 1 ]
  [[ "$output" == *"not a valid immutable digest reference"* ]]
}

@test "accepts an explicit valid digest reference" {
  run "$SCRIPT" "ghcr.io/streamlinelabs/streamline@${VALID_DIGEST}"
  [ "$status" -eq 0 ]
  [ "$output" = "ghcr.io/streamlinelabs/streamline@${VALID_DIGEST}" ]
}

@test "accepts an explicit valid digest reference with a registry port" {
  run "$SCRIPT" "registry.example.com:5000/streamlinelabs/streamline@${VALID_DIGEST}"
  [ "$status" -eq 0 ]
  [ "$output" = "registry.example.com:5000/streamlinelabs/streamline@${VALID_DIGEST}" ]
}

@test "falls back to a valid pin file when no explicit input is given" {
  pin_file="$WORKDIR/pin"
  echo "ghcr.io/streamlinelabs/streamline@${VALID_DIGEST}" > "$pin_file"

  run "$SCRIPT" "" "$pin_file"
  [ "$status" -eq 0 ]
  [ "$output" = "ghcr.io/streamlinelabs/streamline@${VALID_DIGEST}" ]
}

@test "explicit input takes precedence over the pin file" {
  pin_file="$WORKDIR/pin"
  other_digest="sha256:$(printf 'c%.0s' $(seq 1 64))"
  echo "ghcr.io/streamlinelabs/streamline@${other_digest}" > "$pin_file"

  run "$SCRIPT" "ghcr.io/streamlinelabs/streamline@${VALID_DIGEST}" "$pin_file"
  [ "$status" -eq 0 ]
  [ "$output" = "ghcr.io/streamlinelabs/streamline@${VALID_DIGEST}" ]
}

@test "the actual committed release/CONFORMANCE_IMAGE_DIGEST placeholder hard-blocks" {
  run "$SCRIPT" "" "$BATS_TEST_DIRNAME/../../release/CONFORMANCE_IMAGE_DIGEST"
  [ "$status" -eq 1 ]
  [[ "$output" == *"No conformance image digest was supplied"* ]]
}
