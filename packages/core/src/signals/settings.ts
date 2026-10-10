import { Config, Context, Layer, Schema } from "effect";
import { SignalEntry, type SignalDefinition } from "./contracts.js";

const signalSettings = Config.schema(
  Schema.Record(Schema.String.check(Schema.isPattern(/^[a-z0-9][a-z0-9-]*$/)), SignalEntry),
  "signals",
).pipe(
  Config.withDefault({}),
  Config.map((entries): readonly SignalDefinition[] =>
    Object.entries(entries).map(([slug, entry]) => ({ slug, ...entry })),
  ),
);

export class SignalDefinitions extends Context.Service<
  SignalDefinitions,
  readonly SignalDefinition[]
>()("signals/Definitions") {
  static readonly layer = Layer.effect(SignalDefinitions, signalSettings);
}
