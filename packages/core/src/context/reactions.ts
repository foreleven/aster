import { NotificationsActor } from "../notifications/actor.js";
import { BusinessOutbox } from "../notifications/inbox.js";
import { type ActorSystem, type ActorRef } from "@aster/actor";
import { Effect, Schema, Stream } from "effect";
import { ContextRegistry } from "./registry.js";
import { ContextCaptureSink } from "./memory.js";
import { makeContextProcessor, isolateContextChange } from "./processing.js";
import { InternalAgent } from "../tasks/services.js";
import { GoalSettings } from "../config/settings.js";
import type { GoalsRootCommand } from "../goals/actors.js";
import type { SignalRootCommand } from "../signals/actors.js";
import { SystemOneActor } from "./reaction-actor.js";
import { ReactionPolicy } from "./reaction-policy.js";

/** Live changes wake the durable journal consumer; they are not the authoritative queue. */
export const startContextReactions = <Services>(roots: {
  readonly system: ActorSystem<Services | ContextRegistry | ReactionPolicy | GoalSettings>;
  readonly signals: ActorRef<SignalRootCommand>;
  readonly goals?: ActorRef<GoalsRootCommand>;
}) =>
  Effect.gen(function* () {
    const registry = yield* ContextRegistry;
    const capture = yield* ContextCaptureSink;
    const internal = yield* InternalAgent;
    // Source ingestion needs the restored target catalogue, not partially started roots.
    yield* roots.signals.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
    if (roots.goals) {
      const ready = yield* roots.goals.ask<import("../goals/protocol.js").GoalReadyReply>(
        (replyTo) => ({ _tag: "AwaitReady", stage: "restored", replyTo }),
      );
      if (ready._tag === "Failed") return yield* ready.error;
    }
    const changes = yield* registry.subscribe;
    const notifications = yield* roots.system.spawn("notifications", NotificationsActor);
    yield* notifications.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
    const reactions = yield* roots.system.spawn("system-one", SystemOneActor);
    yield* reactions.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
    // Description and Memory keep their own policy. They never directly deliver Goal/Signal work.
    const process = isolateContextChange(
      makeContextProcessor(registry, capture.capture, () => Effect.void, internal.describe),
    );
    return yield* Stream.runForEach(changes, (change) =>
      Effect.gen(function* () {
        if (Schema.is(BusinessOutbox)(change.record.state))
          yield* notifications.tell({ _tag: "Wake" });
        if (change.record.reactionEvents?.length) yield* reactions.tell({ _tag: "Wake" });
        yield* process(change);
      }),
    ).pipe(Effect.forkScoped);
  });
