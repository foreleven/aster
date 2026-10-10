import { Context } from "effect";

/** Actor-local owner identity; ContextActor binds it for each Behavior instance. */
export class CurrentContextPath extends Context.Service<CurrentContextPath, string>()(
  "context/CurrentContextPath",
) {}
