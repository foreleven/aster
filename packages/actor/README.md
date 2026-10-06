# Actor module

The Actor API and runtime semantics are specified in [docs/actor-design.md](../../docs/actor-design.md). This package implements them with `effect@4.0.0` on Node 24 or later.

It exports `Actor`, `PersistentActor`, `ActorSystem`, `ActorPersistence`, SQLite and in-memory persistence Layers, and `ActorTestKit`. Actor definitions provide a static Effect Layer. `Actor.Service<Self, Services>()(key, { command })` and `PersistentActor.Service<Self, Services>()(key, { command, event, state })` infer protocol and state types from Effect Schemas. The `ReplyTo<Response>` type and `ReplyTo<Response>()` Schema helper describe response references in Commands. All repository Actor implementations and tests use the Schema forms; `Services` is optional and declares required dependencies. Command aliases use `typeof CommandSchema.Type`. The Schema argument is required. Command Schemas provide types only; Commands are not decoded or persisted. `ActorSystem.make().pipe(ActorSystem.provide(...layers))` acquires the service environment in a Scope; `system.spawn` creates top-level actors and `ActorContext.spawn` creates children.

## Ownership and Layers

The host supplies shared service Layers through `ActorSystem.provide`. Persistent actors require an `ActorPersistence` Layer; choosing its backend and configuration belongs to the host. The runtime builds each actor's Behavior Layer and owns startup, restart and shutdown. Shared Layers live until system shutdown; Behavior-local resources close and rebuild on restart. Effect's Layer memo map provides dependency reuse without a separate runtime cache.

| Module                         | Responsibility                                                        |
| ------------------------------ | --------------------------------------------------------------------- |
| `system.ts`                    | Registry, selection, shared providers, inspection and system shutdown |
| `internal/cell.ts`             | Mailbox processing, child lifecycle, supervision and Behavior scopes  |
| `internal/ref.ts`              | Delivery references and one-shot ask replies, shared with test probes |
| `internal/persistent-state.ts` | Recovery, state ownership, journal ordering and snapshots             |
| `persistence.ts`               | Storage contract and backend Layers                                   |

Internal modules are not package exports. Callers use actor definitions and the system API; they do not construct cells or coordinate persistence recovery.

## Lifecycle contracts

- `spawn` returns after registration; Layer acquisition, recovery and `started` run asynchronously under supervision. `tell` only enqueues work. `ActorRef.awaitStarted` waits for the first initialization outcome; it is not a health check or a processing barrier. Use a domain reply when processing completion matters.
- `ask` accepts one reply. Timeout or caller cancellation closes the temporary reply reference without cancelling the receiver's work.
- Restart retains the ref, mailbox, children and watches, while replacing the Behavior Scope and recovering persistent state. `pipeToSelf` belongs to that Scope; old work cannot deliver results or failures into a replacement Behavior.
- `context.stop(child)` requests a direct child's stop. `system.stop(root)` awaits one root's subtree. Descendants finish before the parent's Behavior resources close.
- `system.terminate()` waits for active handlers and all cleanup. Closing the enclosing Scope, or interrupting the owner of graceful termination, forces actor cancellation before closing shared resources.
- `inspect()` reads the registry when executed. Metadata is private by default; hosts explicitly select safe fields with `inspect({ metadata: [key] })` and interpret domain-specific values themselves.

Run `pnpm typecheck`, `pnpm test`, and `pnpm build` in this package. Tests exercise the public actor API, supervision, scoped lifecycle, provider reuse, inspection, `TestClock`, and persistence recovery.
