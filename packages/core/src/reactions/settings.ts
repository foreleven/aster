import { Config, Context, Effect, Layer, Schema } from "effect";
const Concurrency = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 32 }));
const declaration = Config.schema(
  Schema.Struct({
    matchConcurrency: Concurrency.pipe(Schema.withDecodingDefaultKey(Effect.succeed(4))),
    deliveryConcurrency: Concurrency.pipe(Schema.withDecodingDefaultKey(Effect.succeed(4))),
  }),
  ["config", "reactions"],
).pipe(Config.withDefault({ matchConcurrency: 4, deliveryConcurrency: 4 }));
export class ReactionSettings extends Context.Service<
  ReactionSettings,
  { readonly matchConcurrency: number; readonly deliveryConcurrency: number }
>()("reactions/Settings") {
  static readonly layer = Layer.effect(ReactionSettings, declaration);
}
