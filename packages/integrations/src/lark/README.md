# Lark reference integration

Lark owns one account at `/lark`. Mail and IM are enabled only by their corresponding child configuration. An absent child creates no capability services, Actors, background work or Contexts. Enabled IM requires a summary model and a configured System One client. `LarkIntegration.layer` acquires live dependencies and registers the source; `installation` accepts injected clients and model capabilities for isolated tests. Runtime owns framework activation and shutdown. Actors own connection work, catch-up and business retries.

## Module ownership

The complete file-by-file responsibility map is in [the accepted reference design](../../../../docs/lark-reference-integration-design.md).

| Module                                                             | Responsibility                                                                                                |
| ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------- |
| `index.ts`                                                         | Explicit production exports: integration, settings, client contracts, business schemas and query commands.    |
| `integration.ts`, `settings.ts`                                    | Enabled-only composition, typed dependency binding and normalized configuration.                              |
| `cli.ts`, `errors.ts`                                              | One scoped Effect process boundary, JSON/envelope decoding, provider failures and stable public query errors. |
| `paths.ts`, `dates.ts`, `captures.ts`                              | Validated opaque identities, Beijing calendar operations and enabled profile Memory capture.                  |
| `account/`                                                         | Account model/client, cached profile refresh, committed profile query and enabled children.                   |
| `mail/`                                                            | Mail models/client, bounded retrieval, mailbox checkpoint, acknowledged Email writes and queries.             |
| `im/model.ts`, `client.ts`, `polling.ts`, `actor.ts`, `queries.ts` | IM models/provider access, bounded retrieval, pending recovery, acknowledged handoff, checkpoint and queries. |
| `im/chat/state.ts`, `actor.ts`                                     | Pure transition plans and the sole mailbox-owned Chat writer.                                                 |
| `im/summary/`                                                      | Shared memory admission, System One gate, supplied AgentRunner adapter and Behavior-owned summary workflow.   |
| `testing.ts`                                                       | Internal test fixtures exposed only through `@aster/integrations/lark/testing`.                               |

## Persistence, recovery and queries

Mail and IM each retain one `through` checkpoint. Advance it only after all affected writers acknowledge their durable commits. Restart begins at the later of today's Beijing midnight and a valid checkpoint minus overlap. Continuous polling retains its startup lower bound so it can finish yesterday's tail. There is no offline historical backfill, daily retrieval Context or persisted admission timing.

Account and mailbox profiles are immediately readable from committed cache. Startup refresh runs asynchronously, retries typed failures every thirty seconds, preserves cache on failure and stops after success. A mailbox configuration change resets its profile and checkpoint. Queries never trigger profile refresh.

`list_chats` enumerates locally observed, retained Chat records. `summary(chatId)` returns the saved summary or null for a known Chat without one. `messages` reads provider history. Mail `profile` reads committed identity; `list` queries provider metadata; `read(messageId)` reads a retained email or falls back to the provider only when absent. Invalid retained data fails validation. Queries do not activate writers, ingest data, execute models or advance checkpoints. All query commands expose `ContextQueryError`.

At startup, IM activates only Chats with pending evidence or unfinished flush obligations. Completed records remain dormant and readable. New ingress activates their writer. A Chat disables idle timeout while pending evidence, model/admission work or retries remain; an empty completed Chat passivates after five minutes. Channels remain alive.

## Summary execution

Messages are immutable family members keyed by provider ID and retained in append order. Acceptance ignores IDs already retained, including edits. Model batching sorts a detached list chronologically. Summary application and covered-ID deletion commit atomically. Deleted messages leave no receipts or fingerprints: overlap or replay can append a deleted ID again.

Each Chat creates one workflow in its Behavior scope. It owns its defer cache, selected batch and finite retry budget. Workers read the bound ContextHandle directly; commits return through acknowledged mailbox commands with generation checks. System One assesses ordinary evidence. Flush and message-count overflow force summarization. The supplied AgentRunner uses the required `save_summary` tool, validated references and a two-minute timeout.

One integration-scoped FIFO admits model calls with bounded concurrency and exact minimum start spacing. Timing is memory-only; restart starts fresh. Capacity failures reduce batch size; transient failures retry three times, thirty seconds apart. Every attempt releases admission before waiting. Failed evidence remains durable. Defects and interruption retain Actor supervision and cancellation semantics.

## Verification

`packages/integrations/test/lark/` uses fake transports/models, real Actors, Deferred and TestClock. Core tests cover detached committed reads; Actor tests cover disabled/re-enabled idle timeout and outstanding work. No live Lark or model execution is required.
