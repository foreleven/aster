import {
  schemaTool,
  ToolInvocationFailure,
  type EffectTool,
  type TSchema,
  type AgentToolResult,
} from "../index.js";
import { Effect, Schema } from "effect";
import { ApplicationError } from "../../operations.js";

export type CoreTool<R = never> = EffectTool<TSchema, ToolInvocationFailure, R>;
export const output = <A>(value: A): AgentToolResult<A> => ({
  content: [{ type: "text", text: JSON.stringify(value ?? null) }],
  details: value,
});

/** Tools present domain outcomes; domain owners decide admission, uncertainty and recovery. */
export const defineTool = <A, B, R>(
  definition: {
    readonly name: string;
    readonly label: string;
    readonly description: string;
    readonly replay: "safe" | "never";
    readonly parameters: Schema.Decoder<A>;
  },
  execute: (args: A, callId: string) => Effect.Effect<B, ApplicationError, R>,
  format: (value: B) => AgentToolResult<unknown> = output,
): CoreTool<R> =>
  schemaTool({
    ...definition,
    execute: (id, args) =>
      Effect.suspend(() => execute(args, id)).pipe(
        Effect.map(format),
        Effect.catchTag("ApplicationError", (error) => {
          if (error.kind === "uncertain")
            return Effect.fail(
              new ToolInvocationFailure({
                message: error.message,
                outcome: "unknown",
                operationId: error.operationId,
              }),
            );
          return Effect.succeed({
            isError: true,
            content: [{ type: "text" as const, text: error.message }],
            details: { code: error.kind, message: error.message },
          });
        }),
      ),
  });
