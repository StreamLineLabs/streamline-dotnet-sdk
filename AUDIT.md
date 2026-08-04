# Clean Code and SRP Audit

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
- Live integration remains blocked by the container image.
- Public error-code/API consolidation requires versioning decisions.

## Out of Scope

- `KafkaTimeouts`: one timeout translation actor.
- Integration test support: one test-environment actor.
- Main `StreamlineClient` facade: public compatibility surface.
