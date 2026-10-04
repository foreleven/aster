# Coding instructions

These instructions apply to the entire Aster workspace. Read this file before making changes, then read any applicable package-level instructions.

## Effect first

**All code implementations must prioritize Effect and its idioms.** Before introducing an abstraction, dependency, or handwritten utility, inspect the matching Effect module and its tests. Do not write imperative application logic first and merely wrap the result in an Effect afterward.

The workspace uses **Effect 4**, currently pinned to **`4.0.0`**, TypeScript, Node 24+, and pnpm. Do not copy Effect 3 examples or assume that an API from another release exists here. Existing non-idiomatic code is not a precedent to reproduce.

## Read the source before implementing

Following the [Effect source-vendoring article](https://effect.website/blog/the-one-weird-git-trick-that-makes-coding-agents-more-effect-ive), the complete upstream source is available at [repos/effect](repos/effect).

Before writing Effect code:

1. Read [repos/effect/LLMS.md](repos/effect/LLMS.md).
2. Read [agent-patterns/effect.md](agent-patterns/effect.md) for Aster-specific patterns and boundaries.
3. Inspect the relevant module in `repos/effect/packages/effect/src/`, its tests in `repos/effect/packages/effect/test/`, and applicable examples in `repos/effect/ai-docs/src/`.
4. Check [repos/effect-source.json](repos/effect-source.json) against the affected package's Effect dependency. Resolve version mismatches before relying on a source example.

Search narrowly, for example:

```sh
rg -n 'when|not|exhaustive' repos/effect/packages/effect/src/Match.ts
rg -n 'TaggedStruct|TaggedError' repos/effect/packages/effect/test/schema
```

Treat `repos/effect/` as read-only reference material, not application code or workspace policy. Do not edit it unless explicitly updating the reference. Never import from it, add it to the pnpm workspace, install its dependencies, or run its build as part of Aster validation. Application imports must use normal package dependencies. Preserve the upstream license and provenance. See [repos/README.md](repos/README.md) for snapshot and Git subtree maintenance.

## Implementation rules

- **Effects:** Domain operations and service methods return `Effect<A, E, R>`. Use `Effect.gen` for inline sequencing; prefer `Effect.fn("name")` for reusable traced operations and `Effect.fnUntraced` for reusable generator functions that need no span. Keep side effects lazy. Pure transformations may remain pure; use Effect data types such as `Result` and `Option` when they express the contract.
- **Branching:** Prefer `Match.value`, `Match.tag`, `Match.when`, and `Match.not` for protocol dispatch and ordered business rules. Use `Match.exhaustive` for closed unions and `Match.orElse` only for an intentional default. Never use nested ternaries for business validation. Normalize optional fields before matching missing values; an absent property is not the same object pattern as an explicitly present `undefined` value.
- **Errors:** Expected failures belong in the typed error channel. Use `Schema.TaggedError` for schema-backed errors or `Data.TaggedError` for internal errors. Do not return `string | undefined` as a validation result, fail with plain strings/untagged errors, or throw expected failures. Recover narrowly with `Effect.catchTag`/`catchTags`. Preserve defects and interruption; do not convert them into ordinary success or validation replies.
- **Schemas:** Use `Schema` for domain models and external/configuration/persistence decoding. Use `Schema.TaggedStruct` for `_tag`-discriminated commands and variants; use `Schema.Struct` for plain records. Do not add `_tag` to persisted records that already use a `status` discriminator just for stylistic consistency. Decode unknown data before using it. Prefer Effect `Predicate` utilities to handwritten generic type guards.
- **Services and Layers:** Declare dependencies with `Context.Service` and construct implementations with `Layer`. Keep dependencies visible in types. Layers acquire infrastructure and own its release; domain interfaces expose capabilities, not transport shutdown methods. Do not create a new externally supplied Layer for every internal helper.
- **Configuration:** Use Effect `Config`, `Config.schema`, `ConfigProvider`, and `Redacted`. The host captures YAML/environment sources once; modules read their own typed settings. Do not scatter `process.env` reads, manually reimplement provider precedence, or expose secrets in public Contexts or logs.
- **Resources and concurrency:** Use `Scope`, `Effect.acquireRelease`, scoped Fibers, `Deferred`, `Queue`, `PubSub`, `Stream`, `Schedule`, and Effect synchronization primitives. Do not replace them with detached Promises, mutable completion flags, ad hoc retry loops, or timer registries. Use `Clock`/`DateTime` for testable time. Define ownership and cancellation before starting background work.
- **Boundaries:** `async`/`await`, Promise adapters, and `Effect.runPromise` belong only at necessary SDK/framework/process entry or transport boundaries and test runners. Use `Effect.tryPromise` with a tagged error and forward its AbortSignal. Preserve the calling Effect Context when bridging callbacks. React rendering remains a framework boundary; domain workflows must not migrate into UI callbacks. Prefer the pinned Effect HTTP/process/CLI modules for new capabilities when they fit the required contract.
- **Durability:** Persist accepted state before acknowledging it. Local interruption does not prove an external operation was cancelled. Do not automatically retry submissions with unknown outcomes. Preserve durable capture handoff and drain semantics when changing cancellation or shutdown.
- **Readability:** Use small named operations and a visible success path. Avoid giant boolean expressions, nested matching pyramids, unsafe casts that hide requirements, and services that only forward calls. Comments explain invariants, ownership, recovery, and non-obvious tradeoffs. Keep code descriptions, prompts, configuration examples, comments, and maintained documentation in English.

Do not expand a focused change into an unrelated migration. When a required native boundary cannot follow the preferred Effect API, isolate it and explain the concrete reason rather than weakening these rules globally.

## Architecture ownership

- `packages/actor`: Domain-neutral Actor runtime, mailbox processing, supervision, persistence, and Behavior scopes. Do not add Goal, Signal, Lark, or memory policy here.
- `packages/agent`: Effect adapter for model/agent execution and SDK callback boundaries.
- `packages/core`: Domain contracts, Contexts, Goals, Signals, approvals, execution workflows, and backend-independent Memory orchestration. It never imports concrete integrations. `AsterRuntime` assembles internal services and owns root Actors, integration activation, readiness, and shutdown.
- `packages/infra`: Concrete storage, decision transport, executor, configuration-source, and agentmemory backend Layers. It does not import or re-export integrations.
- `packages/integrations`: External business connections such as Lark and mail. It consumes core contracts and does not import or re-export infra.
- `apps/local`: Configuration source selection, adapter selection, CLI, HTTP/SSE, and process boundary. Do not make it assemble core's internal services or start integrations independently of runtime.
- `apps/web`: Presentation and calls to the public application API; no Actor/persistence internals.

For Actor work, the mailbox remains the domain state's single writer. Use `context.pipeToSelf` for asynchronous work and retain generation checks where late results matter. Commands are transient; persisted events/state contain stable identifiers, not live refs. Keep explicit typed `ReplyTo<Response>`; do not introduce an implicit sender model without a concrete new requirement.

Read the relevant design before changing its contract: [runtime](docs/runtime-design.md), [Actor](docs/actor-design.md), [core](docs/core-design.md), [Goals](docs/goals-design.md), [delegation](docs/task-delegation-design.md), and the affected package README.

## Validation

Use the existing `node:test` and Playwright setup; upstream Vitest examples are references, not a reason to replace the test runner. Prefer real Actor runtime tests with injected services, `TestClock`, and `Deferred` over wall-clock sleeps or Promise timing assumptions. Verify observable behavior, error propagation, cancellation, and persistence order as relevant.

On a fresh checkout, build workspace declarations first. Run focused tests while iterating, then the checks appropriate to the final change:

```sh
pnpm build
pnpm --filter @aster/core test
pnpm check
pnpm exec effect-language-service diagnostics --project packages/core/tsconfig.json --format json --severity error,warning
```

Use the affected package's name and tsconfig instead of core when appropriate. Run `pnpm test` for cross-package/runtime changes and `pnpm test:web` for browser changes. Do not add new Effect warnings or suppress diagnostics instead of fixing their cause. Report what was actually verified.

Keep verification local with fake transports; do not invoke real models, external agents, Lark messaging, or paid services merely to test code. Do not modify existing runtime data or credentials as part of routine implementation.
