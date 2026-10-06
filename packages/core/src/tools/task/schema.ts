import { Type } from "@aster/agent";
export const actorTask = Type.Union([
  Type.Object({
    _tag: Type.Literal("Goal"),
    target: Type.String({ pattern: "^/goals/[a-z0-9][a-z0-9-]*$" }),
    text: Type.String({ minLength: 1 }),
  }),
  Type.Object({
    _tag: Type.Literal("Agent"),
    replyTo: Type.String(),
    task: Type.Object({
      instructions: Type.String({ minLength: 1 }),
      input: Type.Array(
        Type.Object({ content: Type.String(), sources: Type.Array(Type.String()) }),
      ),
    }),
  }),
  Type.Object({
    _tag: Type.Literal("Delegate"),
    agent: Type.String(),
    replyTo: Type.String({ pattern: "^/goals/[a-z0-9][a-z0-9-]*$" }),
    task: Type.Object({
      instructions: Type.String({ minLength: 1 }),
      input: Type.Array(
        Type.Object({ content: Type.String(), sources: Type.Array(Type.String()) }),
      ),
    }),
  }),
]);
