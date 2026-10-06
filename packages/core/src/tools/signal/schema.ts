import { Type } from "@aster/agent";
import { actorTask } from "../task/schema.js";
export const signalDefinition = Type.Object({
  trigger: Type.Union([
    Type.Object({ _tag: Type.Literal("Context"), when: Type.String({ minLength: 1 }) }),
    Type.Object({
      _tag: Type.Literal("Schedule"),
      schedule: Type.Union([
        Type.Object({ type: Type.Literal("once"), at: Type.String() }),
        Type.Object({
          type: Type.Literal("cron"),
          expression: Type.String(),
          timeZone: Type.String(),
        }),
      ]),
    }),
  ]),
  task: actorTask,
});
