import { internalAgentSettings } from "../config/settings.js";
import { makeStructuredReasoning } from "./structured.js";
import { Context, Data, Effect, Layer, Schema } from "effect";
import type { ContextRegistry } from "../context/registry.js";
import type { ContextSnapshot } from "../context/model.js";

export class ContextDescriptionError extends Data.TaggedError("ContextDescriptionError")<{
  readonly path: string;
  readonly message: string;
  readonly cause: unknown;
}> {}
export interface DescriptionPolicy {
  readonly matches: (path: string) => boolean;
  readonly identity: string;
}
export class ContextDescriptions extends Context.Service<
  ContextDescriptions,
  {
    readonly register: (policies: readonly DescriptionPolicy[]) => Effect.Effect<void>;
    readonly identity: (path: string) => string | undefined;
  }
>()("reasoning/ContextDescriptions") {
  static readonly layer = Layer.sync(ContextDescriptions, () => {
    const policies = new Set<DescriptionPolicy>();
    return {
      register: (values) =>
        Effect.sync(() => {
          for (const policy of values) policies.add(policy);
        }),
      identity: (path) => [...policies].find((policy) => policy.matches(path))?.identity,
    };
  });
}

export interface ContextIdentity {
  readonly path: string;
  readonly identity: string;
  readonly parentDescription: string;
}
export type DescriptionInitializer = (
  identity: ContextIdentity,
) => Effect.Effect<string, ContextDescriptionError>;
export const makeDescriptionInitializer =
  <E>(run: (prompt: string, schema: object) => Effect.Effect<unknown, E>): DescriptionInitializer =>
  (identity) =>
    Effect.gen(function* () {
      const raw = yield* run(
        [
          "Describe this Context's fixed identity, purpose, and relationship to the user in one concise English sentence.",
          "Use only the basic identity and parent Context description below. Do not include current activity, content, state, or speculation.",
          "For example, if an email Context has the parent description ‘My work mailbox’, its description could be ‘An email in my work mailbox’.",
          "A Signal Run records one occurrence of its parent Signal. Its description remains fixed as its content changes.",
          JSON.stringify(identity),
        ].join("\n"),
        {
          type: "object",
          properties: { description: { type: "string" } },
          required: ["description"],
          additionalProperties: false,
        },
      );
      const result = yield* Schema.decodeUnknownEffect(
        Schema.Struct({ description: Schema.String }),
      )(raw);
      if (!result.description.trim())
        return yield* new ContextDescriptionError({
          path: identity.path,
          message: "Agent returned no Context description",
          cause: raw,
        });
      return result.description.trim();
    }).pipe(
      Effect.mapError((cause) =>
        cause instanceof ContextDescriptionError
          ? cause
          : new ContextDescriptionError({
              path: identity.path,
              cause,
              message: cause instanceof Error ? cause.message : String(cause),
            }),
      ),
    );

export const makeConfiguredDescriptionInitializer = Effect.fn(
  "makeConfiguredDescriptionInitializer",
)(function* () {
  const settings = yield* internalAgentSettings;
  const run = yield* makeStructuredReasoning(settings.model);
  return makeDescriptionInitializer((prompt, schema) => run(prompt, schema));
});

/** Description initialization never replaces content committed while the model was running. */
export const initializeContextDescription = Effect.fn("ContextDescription.initialize")(function* (
  registry: ContextRegistry["Service"],
  record: ContextSnapshot,
  identity: string | undefined,
  describe: DescriptionInitializer,
) {
  if (record.description || !identity) return record;
  const existing = registry.get(record.path)?.description;
  if (existing) return { ...record, description: existing };
  let ancestor = record.path.slice(0, record.path.lastIndexOf("/"));
  while (ancestor && !registry.get(ancestor))
    ancestor = ancestor.slice(0, ancestor.lastIndexOf("/"));
  const description = yield* describe({
    path: record.path,
    identity,
    parentDescription: registry.get(ancestor)?.description ?? "",
  });
  const latest = registry.get(record.path);
  if (latest)
    yield* registry.initializeDescription(record.path, description, latest.revision).pipe(
      // An owner update wins over optional generated metadata; its notification is already queued.
      Effect.catchTag("ContextConflict", () => Effect.void),
    );
  return { ...record, description: registry.get(record.path)?.description || description };
});
