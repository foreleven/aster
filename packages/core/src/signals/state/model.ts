import { isDeepStrictEqual } from "node:util";
import { ApplicationError } from "../../operations.js";
import { Clock, Context, Cron, Effect, Layer, Match, Schema } from "effect";
import { ContextRegistry } from "../../context/registry.js";
import { type SignalDefinition, validateSignalTime } from "../../config/schema.js";
import { SignalChangeInput, SignalReactionInput } from "../protocol.js";
import { signalEnabled, type SignalSnapshot } from "./snapshot.js";
import { makeSignalStore } from "./store.js";

export const nextSignalTime = (
  trigger: SignalSnapshot["trigger"],
  now: number,
): string | undefined =>
  Match.value(trigger).pipe(
    Match.tag("Context", () => undefined),
    Match.tag("Schedule", ({ schedule }) =>
      Match.value(schedule).pipe(
        Match.when({ type: "once" }, ({ at }) => new Date(at).toISOString()),
        Match.when({ type: "cron" }, ({ expression, timeZone }) =>
          Cron.next(Cron.parseUnsafe(expression, timeZone), now).toISOString(),
        ),
        Match.exhaustive,
      ),
    ),
    Match.exhaustive,
  );
const cursor = (trigger: SignalSnapshot["trigger"], now: number) => {
  const nextDue = nextSignalTime(trigger, now);
  return nextDue === undefined ? {} : { nextDue };
};
const conflict = (message: string) => new ApplicationError({ kind: "conflict", message });
const makeState = Effect.fn("SignalState.make")(function* (
  path: string,
  configured?: SignalDefinition,
) {
  const registry = yield* ContextRegistry;
  const store = yield* makeSignalStore(path);
  const snapshot = store.read.pipe(Effect.map((history) => history.snapshot));
  const enabled = snapshot.pipe(
    Effect.map((state) => !!state && signalEnabled(state, (path) => registry.get(path))),
  );
  const current = Effect.gen(function* () {
    const state = yield* snapshot;
    if (!state) return yield* conflict("Signal does not exist");
    return state;
  });
  const timing = (trigger: SignalSnapshot["trigger"]) =>
    Effect.try({
      try: () => {
        if (trigger._tag === "Schedule") validateSignalTime(trigger);
      },
      catch: () =>
        new ApplicationError({ kind: "invalid-input", message: "Invalid Signal timing" }),
    });
  const setStatus = Effect.fnUntraced(function* (status: SignalSnapshot["status"]) {
    const state = yield* snapshot;
    if (!state || state.status === status || state.status === "deleted") return;
    const remainingAgentTurns = (yield* store.read).remainingAgentTurns;
    yield* store.append({
      _tag: "Changed",
      snapshot: { ...state, status, version: state.version + 1 },
      ...(remainingAgentTurns !== undefined ? { remainingAgentTurns } : {}),
    });
  });
  const saved = yield* snapshot;
  if (configured) {
    if (saved?.owner)
      return yield* Effect.die(new Error("Configured Signal collides with Goal ownership"));
    yield* timing(configured.trigger).pipe(Effect.orDie);
    if (
      !saved ||
      !isDeepStrictEqual(
        { trigger: saved.trigger, task: saved.task },
        { trigger: configured.trigger, task: configured.task },
      )
    )
      yield* store.append({
        _tag: "Changed",
        snapshot: {
          trigger: configured.trigger,
          task: configured.task,
          status: "active",
          version: (saved?.version ?? 0) + 1,
          ...cursor(configured.trigger, yield* Clock.currentTimeMillis),
        },
      });
    else if (saved.status === "paused") yield* setStatus("active");
  } else if (saved && !saved.owner) yield* setStatus("paused");
  const change = Effect.fn("SignalState.change")(function* (raw: SignalChangeInput) {
    const input = yield* Schema.decodeUnknownEffect(SignalChangeInput)(raw).pipe(
      Effect.mapError(
        () => new ApplicationError({ kind: "invalid-input", message: "Invalid Signal command" }),
      ),
    );
    if (input.target !== path) return yield* conflict("Signal command targets another owner");
    const history = yield* store.read;
    const prior = history.receipts.find((item) => item.input.requestId === input.requestId);
    if (prior) {
      if (prior._tag !== "Command" || !isDeepStrictEqual(prior.input, input))
        return yield* conflict("Signal request identity belongs to another command");
      return prior.receipt;
    }
    const state = history.snapshot;
    if (configured || (state && state.owner !== input.source))
      return yield* conflict("Signal belongs to another owner");
    if ((registry.get(input.source)?.state as { status?: string } | undefined)?.status !== "active")
      return yield* conflict("Goal has ended or is missing");
    const change = input.change;
    if (change.operation === "create" ? !!state : !state || state.status === "deleted")
      return yield* conflict("Signal operation does not match current existence");
    if (change.operation !== "create" && change.version !== state!.version)
      return yield* conflict("Signal version changed; read it again");
    const next = yield* Match.value(change).pipe(
      Match.whenOr({ operation: "create" }, { operation: "update" }, (change) =>
        Effect.gen(function* () {
          yield* timing(change.definition.trigger);
          return {
            ...change.definition,
            owner: input.source,
            status: state?.status ?? "active",
            version: (state?.version ?? 0) + 1,
            ...cursor(change.definition.trigger, yield* Clock.currentTimeMillis),
          } satisfies SignalSnapshot;
        }),
      ),
      Match.orElse((change) =>
        Effect.succeed({
          ...state!,
          version: state!.version + 1,
          status: Match.value(change.operation).pipe(
            Match.when("delete", () => "deleted" as const),
            Match.when("pause", () => "paused" as const),
            Match.when("resume", () => "active" as const),
            Match.exhaustive,
          ),
        }),
      ),
    );
    const receipt = {
      requestId: input.requestId,
      revision: ((yield* store.current)?.revision ?? 0) + 1,
    };
    yield* store.append({
      _tag: "Changed",
      snapshot: next,
      remainingAgentTurns: input.remainingAgentTurns,
      receipt: { _tag: "Command", input, receipt },
    });
    return receipt;
  });
  const react = Effect.fn("SignalState.react")(function* (raw: SignalReactionInput) {
    const input = yield* Schema.decodeUnknownEffect(SignalReactionInput)(raw).pipe(
      Effect.mapError(
        () => new ApplicationError({ kind: "invalid-input", message: "Invalid Signal reaction" }),
      ),
    );
    const history = yield* store.read;
    const { sourceContext, ...identity } = input;
    const prior = history.receipts.find((item) => item.input.requestId === input.requestId);
    const requestId = `${path}:reaction:${input.requestId}`;
    if (prior) {
      const message = history.deliveries.find(
        (item) => item.message.requestId === requestId,
      )?.message;
      if (
        prior._tag !== "Reaction" ||
        !isDeepStrictEqual(prior.input, identity) ||
        !isDeepStrictEqual(message?.evidence, sourceContext)
      )
        return yield* conflict("Signal reaction identity belongs to another input");
      return prior.receipt;
    }
    const state = yield* current;
    if (input.target !== path || !(yield* enabled) || state.trigger._tag !== "Context")
      return yield* conflict("Signal does not accept this reaction");
    if (input.version !== state.version) return yield* conflict("Signal changed after screening");
    const receipt = { requestId: input.requestId, revision: (yield* store.current)!.revision };
    yield* store.append({
      _tag: "Triggered",
      message: {
        requestId,
        source: path,
        task: state.task,
        evidence: sourceContext,
        createdAt: new Date(yield* Clock.currentTimeMillis).toISOString(),
        remainingAgentTurns: history.remainingAgentTurns ?? 4,
      },
      receipt: { _tag: "Reaction", input: identity, receipt },
    });
    return receipt;
  });
  const tick = Effect.fn("SignalState.tick")(function* (version: number, due: string) {
    const state = yield* snapshot;
    const now = yield* Clock.currentTimeMillis;
    if (
      !state ||
      !(yield* enabled) ||
      state.version !== version ||
      state.nextDue !== due ||
      Date.parse(due) > now ||
      state.trigger._tag !== "Schedule"
    )
      return;
    const requestId = `${path}:timer:${version}:${due}`;
    const remainingAgentTurns =
      state.trigger.schedule.type === "cron" ? undefined : (yield* store.read).remainingAgentTurns;
    yield* store.append({
      _tag: "Triggered",
      message: {
        requestId,
        source: path,
        task: state.task,
        createdAt: due,
        remainingAgentTurns: remainingAgentTurns ?? 4,
      },
      nextDue: state.trigger.schedule.type === "once" ? null : nextSignalTime(state.trigger, now),
    });
  });
  const beginDelivery = Effect.fn("SignalState.beginDelivery")(function* (requestId: string) {
    const item = (yield* store.read).deliveries.find(
      (item) => item.message.requestId === requestId,
    );
    if (!item || item.status !== "pending" || !(yield* enabled)) return undefined;
    yield* store.append({ _tag: "DeliveryChanged", requestId, status: "sending" });
    return item.message;
  });
  const settleDelivery = Effect.fn("SignalState.settleDelivery")(function* (
    requestId: string,
    error?: ApplicationError,
  ) {
    if (error?.kind === "unavailable") return;
    const item = (yield* store.read).deliveries.find(
      (item) => item.message.requestId === requestId,
    );
    if (item?.status !== "sending") return;
    yield* store.append({
      _tag: "DeliveryChanged",
      requestId,
      status: error ? "failed" : "delivered",
      ...(error ? { error: error.message } : {}),
    });
  });
  return {
    path,
    record: store.current,
    snapshot,
    enabled,
    change,
    react,
    tick,
    beginDelivery,
    settleDelivery,
    pause: setStatus("paused"),
    deliveries: store.read.pipe(Effect.map((state) => state.deliveries)),
  };
});
export class SignalState extends Context.Service<
  SignalState,
  Effect.Success<ReturnType<typeof makeState>>
>()("signals/State") {
  static readonly layer = (path: string, definition?: SignalDefinition) =>
    Layer.effect(SignalState, makeState(path, definition));
}
