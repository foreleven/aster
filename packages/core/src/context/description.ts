import { Effect, Schema } from "effect";
import { ContextDescriptionError } from "./errors.js";
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
