import { type ActorContext } from "@aster/actor";
import { ApplicationError } from "../operations.js";
import { Context, Schema } from "effect";

/** The current runtime's addressing capability, provided by the owning execution. */
export class CurrentActors extends Context.Service<
  CurrentActors,
  Pick<ActorContext<unknown>, "select">
>()("services/CurrentActors") {}

/** Transient query replies and cancellation are never persisted in Context state. */
export const QueryReply = Schema.TaggedUnion({
  Success: { value: Schema.Unknown },
  Failure: { error: ApplicationError },
});
export type QueryReply = typeof QueryReply.Type;
