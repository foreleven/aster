import { type ActorContext } from "@aster/actor";
import { Clock, Context, Deferred, Effect, Fiber, Layer, Match, Ref, Scope } from "effect";
import { ContextActor, contextPath } from "../context/actor.js";
import { ContextRegistry } from "../context/registry.js";
import { deliverTask, taskDeliveryReceipt } from "../tasks/delivery.js";
import {
  type SignalCommand,
  SignalCommands,
  SignalDefinitions,
  SignalInternal,
} from "./protocol.js";
import { SignalState } from "./state/model.js";
import { signalView } from "./state/snapshot.js";

type Owner = ActorContext<SignalCommand>;
const handlers = Effect.gen(function* () {
  const state = yield* SignalState;
  const registry = yield* ContextRegistry;
  const scope = yield* Effect.scope;
  const active = yield* Ref.make(false);
  const timer = yield* Ref.make<{ key: string; fiber: Fiber.Fiber<void> } | undefined>(undefined);
  const retry = yield* Ref.make(false);
  const inFlight = yield* Ref.make<ReadonlySet<string>>(new Set());
  const arm = Effect.fn("Signal.arm")(function* (owner: Owner) {
    const snapshot = yield* state.snapshot;
    const due = snapshot?.nextDue;
    const enabled = (yield* Ref.get(active)) && (yield* state.enabled);
    const key = enabled && due ? `${snapshot!.version}:${due}` : undefined;
    const previous = yield* Ref.get(timer);
    if (previous?.key === key) return;
    if (previous) yield* Fiber.interrupt(previous.fiber);
    yield* Ref.set(timer, undefined);
    if (!key || !due) return;
    const version = snapshot!.version;
    const fiber = yield* Effect.sleep(
      Math.min(86400000, Math.max(0, Date.parse(due) - (yield* Clock.currentTimeMillis))),
    ).pipe(Effect.andThen(owner.self.tell({ _tag: "Tick", version, due })), Effect.forkIn(scope));
    yield* Ref.set(timer, { key, fiber });
  });
  const dispatch = Effect.fn("Signal.dispatch")(function* (owner: Owner) {
    if (!(yield* Ref.get(active))) return;
    const enabled = yield* state.enabled;
    for (const item of yield* state.deliveries) {
      if (
        !["pending", "sending"].includes(item.status) ||
        (yield* Ref.get(inFlight)).has(item.message.requestId)
      )
        continue;
      if (item.status === "pending" && !enabled) continue;
      const message =
        item.status === "pending"
          ? yield* state.beginDelivery(item.message.requestId)
          : item.message;
      if (!message) continue;
      yield* Ref.update(inFlight, (set) => new Set([...set, message.requestId]));
      yield* owner.pipeToSelf(
        enabled ? deliverTask(owner, message) : taskDeliveryReceipt(registry, message),
        (result) => ({ _tag: "Delivered", id: message.requestId, result }),
      );
    }
  });
  const scheduleRetry = Effect.fnUntraced(function* (owner: Owner) {
    if (yield* Ref.getAndSet(retry, true)) return;
    yield* owner.pipeToSelf(Effect.sleep("3 seconds"), () => ({ _tag: "Dispatch" }));
  });
  return {
    start: Effect.fnUntraced(function* (owner: Owner, gate?: Deferred.Deferred<void>) {
      if (gate) yield* owner.pipeToSelf(Deferred.await(gate), () => ({ _tag: "Dispatch" }));
      else yield* owner.self.tell({ _tag: "Dispatch" });
    }),
    receive: (command: SignalCommand, owner: Owner) =>
      Match.value(command).pipe(
        Match.tag("GetSignal", ({ replyTo }) =>
          Effect.gen(function* () {
            const snapshot = yield* state.snapshot;
            const record = yield* state.record;
            yield* replyTo.tell(
              snapshot && record ? signalView.project({ ...record, state: snapshot }) : undefined,
            );
          }),
        ),
        Match.tag("Dispatch", () =>
          Effect.gen(function* () {
            yield* Ref.set(active, true);
            yield* Ref.set(retry, false);
            yield* arm(owner);
            yield* dispatch(owner);
          }),
        ),
        Match.tag("Change", ({ input, replyTo }) =>
          Effect.gen(function* () {
            const result = yield* state.change(input).pipe(Effect.result);
            yield* replyTo.tell(
              result._tag === "Success"
                ? { _tag: "Accepted", receipt: result.success }
                : { _tag: "Rejected", error: result.failure },
            );
            yield* arm(owner);
            yield* dispatch(owner);
          }),
        ),
        Match.tag("React", ({ input, replyTo }) =>
          Effect.gen(function* () {
            const result = yield* state.react(input).pipe(Effect.result);
            yield* replyTo.tell(
              result._tag === "Success"
                ? { _tag: "Accepted", receipt: result.success }
                : { _tag: "Rejected", error: result.failure },
            );
            yield* dispatch(owner);
          }),
        ),
        Match.tag("PauseByOwner", ({ owner: ownerPath, replyTo }) =>
          Effect.gen(function* () {
            if ((yield* state.snapshot)?.owner === ownerPath) yield* state.pause;
            yield* replyTo.tell(undefined);
            yield* arm(owner);
            yield* dispatch(owner);
          }),
        ),
        Match.tag("Tick", ({ version, due }) =>
          Effect.gen(function* () {
            if ((yield* Ref.get(timer))?.key !== `${version}:${due}`) return;
            yield* Ref.set(timer, undefined);
            if (yield* Ref.get(active)) yield* state.tick(version, due);
            yield* arm(owner);
            yield* dispatch(owner);
          }),
        ),
        Match.tag("Delivered", ({ id, result }) =>
          Effect.gen(function* () {
            yield* Ref.update(inFlight, (set) => new Set([...set].filter((item) => item !== id)));
            if (result._tag === "Success" && !result.value) return yield* scheduleRetry(owner);
            yield* state.settleDelivery(id, result._tag === "Failure" ? result.error : undefined);
            if (result._tag === "Failure" && result.error.kind === "unavailable")
              yield* scheduleRetry(owner);
          }),
        ),
        Match.exhaustive,
      ),
  };
});
export const SignalActor = ContextActor.define("signals/Actor", {
  commands: SignalCommands,
  internal: SignalInternal,
})((owner) =>
  Effect.gen(function* () {
    const scope = yield* Effect.scope;
    const definitions = yield* SignalDefinitions;

    const path = contextPath(owner);
    const services = yield* Layer.buildWithScope(
      SignalState.layer(
        path,
        definitions.find((item) => `/signals/${item.slug}` === path),
      ),
      scope,
    );
    const behavior = yield* handlers.pipe(
      Effect.provideService(SignalState, Context.get(services, SignalState)),
      Effect.provideService(Scope.Scope, scope),
    );

    return {
      started: (owner) =>
        behavior.start(
          owner,
          owner.metadata.signalActivation as Deferred.Deferred<void> | undefined,
        ),

      receive: (command, owner) => behavior.receive(command, owner),
    };
  }),
);
