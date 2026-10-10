import assert from "node:assert/strict";
import { test } from "node:test";
import { ConfigProvider, Effect } from "effect";
import { outputLanguage } from "../src/config/language.js";

test("model output language defaults to Chinese and accepts an explicit configuration", async () => {
  for (const [config, expected] of [
    [{}, "zh-CN"],
    [{ language: null }, "zh-CN"],
    [{ language: "en-US" }, "en-US"],
    [{ language: " ja-JP " }, "ja-JP"],
  ] as const) {
    assert.equal(
      await Effect.runPromise(
        outputLanguage.pipe(
          Effect.provideService(
            ConfigProvider.ConfigProvider,
            ConfigProvider.fromUnknown({ config }),
          ),
        ),
      ),
      expected,
    );
  }
});

test("invalid output language fails configuration rather than silently selecting the default", async () => {
  for (const language of ["", " ", 42, "not a language"]) {
    const result = await Effect.runPromise(
      outputLanguage.pipe(
        Effect.provideService(
          ConfigProvider.ConfigProvider,
          ConfigProvider.fromUnknown({ config: { language } }, { preserveEmptyStrings: true }),
        ),
        Effect.result,
      ),
    );
    assert.equal(result._tag, "Failure");
  }
});
