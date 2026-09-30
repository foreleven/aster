import { IntegrationError } from "./errors.js";
import type { ActorSystem } from "@aster/actor";
import { Context, Effect, Layer, type Scope } from "effect";

export interface IntegrationHandle {
  /** Completes once this source has caught up. It must not depend on a future subscription. */
  readonly ready: Effect.Effect<void, Error>;
  /** Stop producing new changes; consumers and infrastructure are still alive. */
  readonly stop: Effect.Effect<void>;
}

export interface Integration<Services = any> {
  readonly name: string;
  readonly phase: "consumer" | "source";
  readonly services: Context.Context<Services>;
  readonly activate: (
    system: ActorSystem<Services>,
  ) => Effect.Effect<IntegrationHandle, Error, Scope.Scope>;
}

/** Capture typed Actor dependencies at the module boundary, before protocol erasure. */
export const defineIntegration = <Services>(integration: Integration<Services>) => integration;

/** A per-runtime installation list. Layers register capabilities; only runtime activates them. */
export class RuntimeIntegrations extends Context.Service<
  RuntimeIntegrations,
  {
    readonly register: <Services>(
      integration: Integration<Services>,
    ) => Effect.Effect<void, IntegrationError>;
    readonly installed: () => readonly Integration[];
  }
>()("runtime/Integrations") {
  static readonly layer = Layer.effect(
    RuntimeIntegrations,
    Effect.sync(() => {
      const modules = new Map<string, Integration>();
      return {
        register: <Services>(integration: Integration<Services>) =>
          Effect.gen(function* () {
            if (modules.has(integration.name))
              return yield* Effect.fail(
                new IntegrationError({
                  integration: integration.name,
                  message: `Duplicate integration: ${integration.name}`,
                }),
              );
            // Dependencies were checked by defineIntegration. The heterogeneous
            // registry erases them only after each module captures its own Context.
            modules.set(integration.name, integration as Integration);
          }),
        installed: () => [...modules.values()],
      };
    }),
  );
}
