import { Config, Effect, Redacted } from "effect";
import { ProcessEnvironment, DEFAULT_EXECUTOR_PROMPT } from "@aster/core";
export const doubaoSettings = Effect.gen(function* () {
  const source = Redacted.value((yield* ProcessEnvironment).values);
  const prompt = yield* Config.String("prompt").pipe(
    Config.nested("doubao"),
    Config.nested("agents"),
    Config.withDefault(DEFAULT_EXECUTOR_PROMPT),
  );
  return { prompt, endpoint: source.DOUBAO_CDP_ENDPOINT ?? "http://127.0.0.1:9226" };
});
