# Effect patterns for Aster

Read this after [AGENTS.md](../AGENTS.md) and the pinned [upstream guide](../repos/effect/LLMS.md). These are project conventions, not a replacement for reading the relevant implementation and tests. Examples target Effect `4.0.0-rc.117`.

## Locate the authoritative examples

| Topic                                 | Local upstream reference                                                                                                                                     |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Effect construction and functions     | `repos/effect/LLMS.md`, `repos/effect/ai-docs/src/01_effect/01_basics/10_creating-effects.ts`                                                                |
| Matching and property semantics       | `repos/effect/packages/effect/src/Match.ts`, `repos/effect/packages/effect/test/Match.test.ts`                                                               |
| Schema models and decoding            | `repos/effect/packages/effect/SCHEMA.md`, `repos/effect/ai-docs/src/01_effect/02_schema/10_schema-basics.ts`, `repos/effect/packages/effect/test/schema/`    |
| Services and Layer composition        | `repos/effect/ai-docs/src/01_effect/03_services/20_layer-composition.ts`                                                                                     |
| Typed errors and recovery             | `repos/effect/ai-docs/src/01_effect/04_errors/10_catch-tags.ts`                                                                                              |
| Acquisition, release, background work | `repos/effect/ai-docs/src/01_effect/05_resources/`                                                                                                           |
| Config and providers                  | `repos/effect/packages/effect/src/Config.ts`, `repos/effect/packages/effect/test/Config.test.ts`, `repos/effect/packages/effect/test/ConfigProvider.test.ts` |
| Time and concurrency testing          | `repos/effect/packages/effect/src/testing/TestClock.ts`, `repos/effect/packages/effect/test/Deferred.test.ts`                                                |

## Match selects the branch; Effect carries the outcome

Model commands with tagged schemas and use `Match.tag` plus `Match.exhaustive` to make missing protocol branches visible to TypeScript. For ordered validation, put specific failures before an intentional success fallback. Each branch returns an Effect; only the reply boundary turns a typed failure into a wire response.

This example is adapted to the approval rule in `packages/core/src/approvals/actor.ts`:

```ts
import { Data, Effect, Match } from "effect";

class ApprovalValidationError extends Data.TaggedError("ApprovalValidationError")<{
  readonly message: string;
}> {}

const validateDecision = (response: {
  readonly decision?: "approve" | "reject";
}): Effect.Effect<void, ApprovalValidationError> =>
  Effect.suspend(() =>
    Match.value({ decision: response.decision }).pipe(
      Match.when({ decision: undefined }, () =>
        Effect.fail(new ApprovalValidationError({ message: "Approval decision is required" })),
      ),
      Match.orElse(() => Effect.void),
    ),
  );
```

The normalized object deliberately contains `decision` even when the caller omits it. Object patterns require the property to be present; matching `{ decision: undefined }` directly against `{}` would miss this rule. `Match.not(pattern, handler)` is a matcher combinator, not a nested property predicate. Verify these details against the pinned implementation rather than recalling another pattern library's API.

Keep separate rule families separate: entry existence/status, approval decisions, and input completeness should not become one large predicate or deeply nested matcher. Simple guards inside an Effect generator are appropriate when they improve local sequencing; multi-branch business decisions should normally use Match.

## Schema owns data boundaries

```ts
import { Effect, Schema } from "effect";

const TaskCommand = Schema.Union([
  Schema.TaggedStruct("Execute", { taskId: Schema.String }),
  Schema.TaggedStruct("Cancel", { taskId: Schema.String }),
]);
type TaskCommand = typeof TaskCommand.Type;

class InvalidTaskCommand extends Schema.TaggedError<InvalidTaskCommand>()("InvalidTaskCommand", {
  message: Schema.String,
}) {}

const decodeTaskCommand = Effect.fnUntraced(function* (
  input: unknown,
): Effect.fn.Return<TaskCommand, InvalidTaskCommand> {
  return yield* Schema.decodeUnknownEffect(TaskCommand)(input).pipe(
    Effect.mapError(() => new InvalidTaskCommand({ message: "Invalid task command" })),
  );
});
```

Use `Schema.encodeEffect` when an outgoing value has a codec transformation. Do not cast raw SDK, HTTP, configuration, or stored data to a domain type. Preserve relevant original causes when mapping adapter errors; do not expose credentials or untrusted payloads merely to improve diagnostics.

`Schema.Struct` is still correct for ordinary records. Aster's Run and Delegation records use the existing persisted `status` discriminator; adding `_tag` is a data migration, not a cosmetic cleanup. Actor command Schemas currently supply local protocol types; the Actor runtime does not automatically decode every local command. External adapters and persistence boundaries must perform the required decoding.

## Sequence work without losing ownership

- Use `Effect.gen` inside a Layer or handler. For reusable generator functions, follow the pinned `Effect.fn`/`fnUntraced` examples. Pure Match-based functions need not acquire a tracing span.
- Keep `Effect<A, E, R>` requirements visible. Use `Context.Service` and compose Layers with `Layer.provide`/`provideMerge`; do not erase missing services through casts.
- Use `Effect.acquireRelease` for resources and fork background work into the owning Scope. Finalizers must complete even after failures. Caller cancellation must propagate through SDK callbacks and transport signals.
- Use `Effect.catchTag` for expected failures at the layer that can handle them. In a generator, `return yield* new TaggedError(...)` is valid; in a Match branch, use `Effect.fail(new TaggedError(...))`. Do not use broad cause recovery to turn defects or interruption into business errors.
- A Promise-only transport is adapted once with `Effect.tryPromise({ try: (signal) => ..., catch: ... })`. Do not nest `runPromise` in domain Effects or invent a separate runtime for each service call.

In Aster, `AsterRuntime` owns domain assembly and integration startup; the host supplies infrastructure Layers. Actors own mailbox state mutations. `pipeToSelf` brings asynchronous results back to that mailbox, and Behavior scope cancellation/generation checks protect against retired results. Passing a live ActorRef in a local command is different from persisting a stable path for recovery.

## Configuration, time, and verification

Read configuration through `Config` and injected providers; follow `packages/core/src/config/settings.ts` and the upstream Config tests. Keep credentials redacted and provider capture in infrastructure. Use `Clock`, `DateTime`, and `TestClock` for deadlines and testable time, and `Deferred` for deterministic coordination.

For this repository, use `node:test` with Effects and injected Layers. Do not introduce upstream's Vitest runner as part of a routine change. Test outcomes rather than helper implementation: invalid responses do not write state, valid responses persist before acknowledgement, defects reach supervision, cancellation releases waits, and recovery does not repeat uncertain external work.
