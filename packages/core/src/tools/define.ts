import { type EffectTool, type AgentTool, type TSchema, rejectedToolResult } from "@aster/agent";
import { ApplicationError } from "@aster/api-contracts";
import { Effect } from "effect";
import type { CurrentActors } from "./actors.js";

export type CoreTool<R = CurrentActors> = EffectTool<TSchema, ApplicationError, R>;
export const output = (value: unknown) => ({
  content: [{ type: "text" as const, text: JSON.stringify(value) }],
  details: value,
});
type Definition<T extends TSchema> = Omit<AgentTool<T>, "execute">;
type Execute<T extends TSchema, R> = (
  args: Parameters<AgentTool<T>["execute"]>[1],
  callId: string,
) => Effect.Effect<unknown, ApplicationError, R>;

/** Domain errors become tool results; only an unconfirmed write retains uncertainty. */
const defineTool = <T extends TSchema, R>(
  definition: Definition<T>,
  execute: Execute<T, R>,
  mode: "read" | "write",
): EffectTool<T, ApplicationError, R> => ({
  ...definition,
  execute: (id, args) =>
    Effect.suspend(() => execute(args, id)).pipe(
      Effect.map(output),
      Effect.catchTag("ApplicationError", (error) =>
        mode === "write" && error.kind === "unavailable"
          ? Effect.fail(error)
          : Effect.succeed(rejectedToolResult(error.message)),
      ),
    ),
});
export const queryTool = <T extends TSchema, R>(
  definition: Definition<T>,
  execute: Execute<T, R>,
) => defineTool(definition, execute, "read");
export const commandTool = <T extends TSchema, R>(
  definition: Definition<T>,
  execute: Execute<T, R>,
) => defineTool(definition, execute, "write");
