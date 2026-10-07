import type { ActorContext } from "@aster/actor";
import { Context } from "effect";

/** The current runtime's addressing capability, provided by the owning execution. */
export class CurrentActors extends Context.Service<
  CurrentActors,
  Pick<ActorContext<unknown>, "select">
>()("services/CurrentActors") {}
