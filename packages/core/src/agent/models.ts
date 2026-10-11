import type { Api, Model, Models as PiModels } from "@earendil-works/pi-ai";
import { Context, Effect } from "effect";
import { AgentError } from "./shared/contracts.js";

/** Native pi-ai model access plus the application alias lookup owned by Infra. */
export class AgentModels extends Context.Service<
  AgentModels,
  {
    readonly models: PiModels;
    readonly resolve: (alias: string) => Effect.Effect<Model<Api>, AgentError>;
  }
>()("agent/Models") {}
