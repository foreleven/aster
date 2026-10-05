import { DurableContext } from "../context/persistence.js";
import { type ActorSystem, type ActorRef } from "@aster/actor";
import { Effect, Stream } from "effect";
import { ContextRegistry } from "../context/registry.js";
import { ContextCaptureSink } from "../memory/contracts.js";
import { ContextCaptures } from "../memory/capture.js";
import type { ContextChange } from "../context/model.js";
import type { ContextCapture } from "../memory/contracts.js";
import {
  ContextDescriptions,
  initializeContextDescription,
  type DescriptionInitializer,
  makeConfiguredDescriptionInitializer,
} from "../reasoning/context-description.js";
import { GoalSettings } from "../config/settings.js";
import type { GoalsRootCommand } from "../goals/actors.js";
import type { SignalRootCommand } from "../signals/actors.js";
import { SystemOneActor } from "../reactions/actor.js";
import { ReactionPolicy } from "../reactions/policy.js";

/** Live changes wake the durable journal consumer; they are not the authoritative queue. */
export const startContextReactions = <Services>(roots: {
  readonly system: ActorSystem<
    Services | ContextRegistry | ReactionPolicy | GoalSettings | DurableContext
  >;
  readonly changes: Stream.Stream<ContextChange>;
  readonly signals: ActorRef<SignalRootCommand>;
  readonly goals?: ActorRef<GoalsRootCommand>;
}) =>
  Effect.gen(function* () {
    const registry = yield* ContextRegistry;
    const capture = yield* ContextCaptureSink;
    const captures = yield* ContextCaptures;
    const descriptions = yield* ContextDescriptions;
    const describe = yield* makeConfiguredDescriptionInitializer();
    // Source ingestion needs the restored target catalogue, not partially started roots.
    yield* roots.signals.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
    if (roots.goals) {
      const ready = yield* roots.goals.ask<import("../goals/protocol.js").GoalReadyReply>(
        (replyTo) => ({ _tag: "AwaitReady", stage: "restored", replyTo }),
      );
      if (ready._tag === "Failed") return yield* ready.error;
    }
    const changes = roots.changes;
    const reactions = yield* roots.system.spawn("system-one", SystemOneActor);
    yield* reactions.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
    // Description and Memory keep their own policy. They never directly deliver Goal/Signal work.
    const process = isolateContextChange(
      makeContextMaintenance({
        registry,
        capture: capture.capture,
        captures,
        descriptions,
        describe,
      }),
    );
    return yield* Stream.runForEach(changes, (change) =>
      Effect.gen(function* () {
        yield* reactions.tell({ _tag: "Wake" });
        yield* process(change);
      }),
    ).pipe(Effect.forkScoped);
  });

/** Consumer orchestration contains no Signal/Goal evaluation path. Memory owns durable deduplication. */
export const makeContextMaintenance = (options: {
  readonly registry: ContextRegistry["Service"];
  readonly capture: (input: ContextCapture) => Effect.Effect<void>;
  readonly captures: Pick<ContextCaptures["Service"], "select">;
  readonly descriptions: Pick<ContextDescriptions["Service"], "identity">;
  readonly describe: DescriptionInitializer;
}) =>
  Effect.fn("ContextMaintenance.process")(function* (change: ContextChange) {
    const record = yield* initializeContextDescription(
      options.registry,
      change.record,
      options.descriptions.identity(change.record.path),
      options.describe,
    );
    const capture = yield* options.captures.select(record);
    if (capture)
      yield* options.capture({
        ...capture,
        records: capture.records.map(options.registry.views.project),
      });
  });

/** Expected item failures are logged; defects and interruption retain their failure semantics. */
export const isolateContextChange =
  <E, R>(handle: (change: ContextChange) => Effect.Effect<void, E, R>) =>
  (change: ContextChange) =>
    handle(change).pipe(
      Effect.catch((error) =>
        Effect.logError({ event: "context.processing.failed", path: change.record.path, error }),
      ),
    );
