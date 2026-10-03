import {
  ApplicationError,
  SignalDelivery,
  SignalDeliveryInput,
  type SignalDeliveryReceipt,
} from "@aster/api-contracts";
import { isDeepStrictEqual } from "node:util";
import { Effect, Schema } from "effect";
import type { ContextRegistry } from "../context/registry.js";
import { validateSignalTime, type SignalDefinition } from "../config/schema.js";

const Ownership = Schema.Struct({
  owner: Schema.optional(Schema.String),
  goal: Schema.optional(Schema.String),
  revision: Schema.optional(Schema.Number),
  commandReceipts: Schema.optional(Schema.Array(SignalDelivery)),
});

/** Only called by the Signal mailbox. The definition and receipt share one commit. */
export const applyPersonalSignal = Effect.fn("Signal.applyPersonalCommand")(function* (options: {
  registry: ContextRegistry["Service"];
  path: string;
  raw: SignalDeliveryInput;
  configured: readonly SignalDefinition[];
  agents: readonly string[];
  nextDue: (definition: SignalDefinition) => number | undefined;
}): Effect.fn.Return<SignalDeliveryReceipt, ApplicationError> {
  const input = yield* Schema.decodeUnknownEffect(SignalDeliveryInput)(options.raw).pipe(
    Effect.mapError(
      () => new ApplicationError({ kind: "invalid-input", message: "Invalid Signal command" }),
    ),
  );
  if (input.target !== options.path)
    return yield* new ApplicationError({
      kind: "invalid-input",
      message: "Signal command addressed to another owner",
    });
  const current = options.registry.get(options.path);
  const state = current && Schema.decodeUnknownSync(Ownership)(current.state);
  const previous = state?.commandReceipts?.find((item) => item.input.requestId === input.requestId);
  if (previous) {
    if (!isDeepStrictEqual(previous.input, input))
      return yield* new ApplicationError({
        kind: "conflict",
        message: "Signal request ID belongs to another command",
      });
    return previous.receipt;
  }
  const slug = input.target.slice("/signals/".length);
  if (
    options.configured.some((item) => item.slug === slug) ||
    (current && (state?.owner !== "/personal" || state.goal))
  )
    return yield* new ApplicationError({
      kind: "conflict",
      message: "Signal belongs to another owner",
    });
  if (
    (input.operation === "createSignal" && current) ||
    (input.operation === "updateSignal" && !current)
  )
    return yield* new ApplicationError({
      kind: "conflict",
      message: "Signal creation/update does not match current existence",
    });
  if (!options.agents.includes(input.definition.agent))
    return yield* new ApplicationError({
      kind: "invalid-input",
      message: "Signal executor is not configured",
    });
  const definition = { ...input.definition, slug, mode: "confirm" as const };
  const due = yield* Effect.try({
    try: () => {
      validateSignalTime(definition);
      return options.nextDue(definition);
    },
    catch: () =>
      new ApplicationError({
        kind: "invalid-input",
        message: "Invalid Signal schedule or absolute time",
      }),
  });
  const receipt = { requestId: input.requestId, revision: (current?.revision ?? 0) + 1 };
  const nextState: Record<string, unknown> = {
    ...current?.state,
    ...definition,
    action: definition.action,
    // An update supplies a full definition; omitted timing fields remove prior schedules.
    schedule: definition.schedule,
    notBefore: definition.notBefore,
    owner: "/personal",
    causal: input.causal,
    active: input.active,
    deleted: false,
    revision: (state?.revision ?? 0) + 1,
    nextDue: due,
    timerDone: false,
    commandReceipts: [...(state?.commandReceipts ?? []), { input, receipt }],
  };
  for (const key of ["action", "schedule", "notBefore", "nextDue"])
    if (nextState[key] === undefined) delete nextState[key];
  yield* options.registry
    .commit(
      {
        path: options.path,
        description: current?.description ?? `Personal Signal: ${slug}`,
        state: nextState,
        messages: current?.messages ?? [],
      },
      { expectedRevision: input.expectedRevision },
    )
    .pipe(
      Effect.catchTag("ContextConflict", () =>
        Effect.fail(
          new ApplicationError({
            kind: "conflict",
            message:
              "Signal Context revision changed; read current state before issuing a new command",
          }),
        ),
      ),
      Effect.catchTag("ContextValidationError", Effect.die),
      Effect.catchTag("ContextCommitError", Effect.die),
    );
  return receipt;
});
