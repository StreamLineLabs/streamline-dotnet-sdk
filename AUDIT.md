# Engineering Audit Status

## P0/P1 Release Readiness Remediation

Status as of 2026-09-02:

| Area | Status | Resolution |
|---|---|---|
| Public transaction API | Complete | Added an explicit `ITransactionalProducer<TKey,TValue>` capability and `IStreamlineClient.CreateTransactionalProducer` overloads. README examples now compile and describe the non-atomic client-buffered semantics accurately. |
| Client interface and DI | Complete | Exposed the credential-aware admin overload through `IStreamlineClient`, made `AddStreamline` resolvable without a separate logging registration, validated required arguments, and added compile-level usage tests. |
| Package versions | Complete | Centralized the coordinated `0.4.0` version in `Directory.Build.props` and added a pack-time mismatch failure. |
| Authentication conformance | Complete | Replaced construction-only placeholders with real secured-broker produce/deny tests. Missing opt-in skips explicitly; enabled but incomplete fixtures fail closed. |
| Release validation | Complete | Release tags must be stable `vMAJOR.MINOR.PATCH` values matching the central package version. Format, build, tests, exact package-set checks, and SBOM generation are release gates. Public-repository provenance and SBOM attestations fail closed. |
| `Streamline.Embedded` packaging | Complete | Marked non-packable because the project does not ship the required native runtime assets. |
| Support policy | Complete | Updated package security support to `0.4.x`, stated the `net8.0` target accurately, and removed blanket server-version compatibility claims. |
| Dynamic HTTP paths | Complete | Escaped all dynamic path segments in admin lag/inspection routes and added reserved-character regression tests. |
| Release blocker: live conformance on an explicit image | Complete | `release.yml`'s `publish` job now declares `needs: conformance`; the `conformance` job resolves and validates an explicit, immutable image digest (`scripts/resolve-conformance-image.sh`; no `:latest`, no default, hard-blocks if missing/invalid), runs the live conformance suite against it, and hard-blocks via `scripts/assert-executed-conformance-tests.sh` if the run executed zero tests. `release/CONFORMANCE_IMAGE_DIGEST` is committed unfilled, so this intentionally blocks publication until a maintainer pins a real digest. |

Live broker and authentication execution remain environment-dependent. The hermetic
suite verifies fixture selection and fail-closed configuration; real auth tests require
explicit `STREAMLINE_AUTH_*` values and a secured broker. Release attestations can only
be exercised by GitHub Actions in a repository context that supports artifact
attestations. The repository already contains a `v0.3.0` tag, so these uncommitted
changes now target `0.4.0` and must only be published from a new matching `v0.4.0`
tag after the remaining release gates pass. No tag or publication is performed here.
The new conformance gate has not been exercised live end-to-end (no real Streamline
image digest is available in this environment); its validation, hard-block, and
executed-test-count-guard logic were verified directly (see the scripts' `bats` suite
and the manual reproductions recorded in this change), including reproducing a real
zero-executed-test `.trx` from this repository's own hermetic skip behavior.

## Clean Code and SRP Audit

## Summary

- **Highest-leverage future split:** isolate native producer-handle lifecycle
  from serialization/transaction orchestration after stronger concurrency
  tests.
- Producer and consumer are stateful I/O actors; the baseline repair made their
  creation/disposal/cancellation semantics explicit and tested.
- `AdminClient` and Schema Registry HTTP clients are already separated by API
  actor.
- The shared integration environment/test attributes are cohesive test
  infrastructure and should remain separate from production code.
- Further extraction is deferred rather than introduce wrappers around
  Confluent.Kafka handles without race coverage.

## Findings

| ID | Location | Category | Severity | Actors in conflict | Cost | Size | Behavior risk |
|---|---|---|---|---|---|---|---|
| DOTNET-SRP-1 | `Producer.cs` | Stateful mixed class | P2 | native handle lifecycle; serialization; retries/circuit breaker; transactions | Different policies share disposal, lazy handle, and delivery ordering. | L | High |
| DOTNET-SRP-2 | `Consumer.cs` | Stateful mixed class | P2 | subscription/poll loop; offset control; search HTTP helper | Cancellation and native-handle state make mechanical movement risky. | L | High |
| DOTNET-CC-1 | `StreamlineClient.cs` | Facade breadth | P2 | producer/consumer/admin/query consumers | Broad public facade, but extracting another service layer would add forwarding without removing public actors. | M | Medium |

## Ordered Refactor Sequence

1. Add injected-handle tests for concurrent first use/dispose and pending
   delivery shutdown.
2. Characterize transaction buffer ordering and cancellation.
3. Move native producer-handle ownership into an internal value only after
   those tests pass.
4. Keep public `Producer<TKey,TValue>` as serialization/transaction facade.
5. Apply the same approach independently to consumer state.

## Deferred

- Stateful handle extraction lacks injectable native-handle coverage.
- Live integration requires an explicitly selected, reachable server image or endpoint;
  authentication conformance additionally requires a real secured fixture.
- Broader public error-code/API consolidation still requires versioning decisions.

## Out of Scope

- `KafkaTimeouts`: one timeout translation actor.
- Integration test support: one test-environment actor.
- Main `StreamlineClient` facade: public compatibility surface.
