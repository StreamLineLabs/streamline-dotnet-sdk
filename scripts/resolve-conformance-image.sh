#!/usr/bin/env bash
#
# Resolves and validates the immutable Streamline container image that the release
# workflow must run live conformance against before a release tag can be published.
#
# This is the release blocker: publication depends on live conformance against an
# *explicit* image reference pinned by content digest.
#   - No ":latest" (or any other mutable/floating tag) is accepted.
#   - There is no built-in default. A missing value hard-blocks instead of silently
#     falling back to any image.
#   - The digest is tied to the release: either supplied explicitly as the release
#     workflow's `conformance_image_digest` input, or pinned in the committed
#     release/CONFORMANCE_IMAGE_DIGEST file so it travels with the tagged commit.
#
# Usage:
#   scripts/resolve-conformance-image.sh [explicit-image] [pin-file]
#
# Prints the validated image reference to stdout on success.
# On failure, prints a "::error::"-prefixed message to stderr and exits 1.

set -euo pipefail

explicit_image="${1:-}"
pin_file="${2:-release/CONFORMANCE_IMAGE_DIGEST}"

fail() {
  echo "::error::$1" >&2
  exit 1
}

image="$explicit_image"

if [[ -z "$image" && -f "$pin_file" ]]; then
  # Take the first non-blank, non-comment line from the pin file.
  image="$(grep -vE '^[[:space:]]*(#|$)' "$pin_file" 2>/dev/null | head -n1 | tr -d '[:space:]' || true)"
fi

if [[ -z "$image" ]]; then
  fail "No conformance image digest was supplied. Provide the 'conformance_image_digest' workflow input, or pin an immutable digest in ${pin_file}. There is no default image for release conformance."
fi

# Reject any mutable/floating tag, including ":latest", anywhere in the reference.
if [[ "$image" == *":latest"* ]]; then
  fail "Conformance image '${image}' uses a mutable ':latest' reference. Release conformance requires an explicit immutable digest, not a floating tag."
fi

# Require a pure digest reference: registry[:port]/path(/path)*@sha256:<64 lowercase hex>.
# No tag component is permitted anywhere before the digest, so "repo:tag@sha256:..." is
# rejected as well as "repo:latest".
digest_pattern='^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?(:[0-9]+)?(/[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?)+@sha256:[0-9a-f]{64}$'
if [[ ! "$image" =~ $digest_pattern ]]; then
  fail "Conformance image '${image}' is not a valid immutable digest reference. Expected the form registry/repository@sha256:<64 lowercase hex characters>, with no floating tag."
fi

echo "$image"
