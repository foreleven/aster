import assert from "node:assert/strict";
import { test } from "node:test";
import { ConfigProvider, Effect, Schema } from "effect";
import { LarkConfig, parseLarkConfig } from "../src/lark/config.js";
import {
  ChatPollingConfig,
  AgentAdmissionConfig,
  ChatSummaryConfig,
} from "../src/lark/im/config.js";

test("IM config owns defaults and validates polling and summary settings", () => {
  assert.deepEqual(Schema.decodeUnknownSync(ChatPollingConfig)({}), {
    pollIntervalMs: 900_000,
    catchUpWindowMs: 3_600_000,
  });
  assert.deepEqual(Schema.decodeUnknownSync(AgentAdmissionConfig)({}), {
    agentStartIntervalMs: 10_000,
    agentConcurrency: 2,
  });
  assert.deepEqual(Schema.decodeUnknownSync(ChatSummaryConfig)({ model: "summary" }), {
    model: "summary",
    maxMessages: 200,
    agentStartIntervalMs: 10_000,
    agentConcurrency: 2,
  });
  for (const value of [0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
    for (const field of ["pollIntervalMs", "catchUpWindowMs"])
      assert.throws(() => parseLarkConfig({ children: { "/im": { config: { [field]: value } } } }));
    for (const field of ["agentStartIntervalMs", "agentConcurrency", "maxMessages"])
      assert.throws(() =>
        parseLarkConfig({
          children: { "/im": { config: { summary: { model: "summary", [field]: value } } } },
        }),
      );
  }
  assert.throws(() => Schema.decodeUnknownSync(ChatSummaryConfig)({ model: "" }));
  assert.throws(() => Schema.decodeUnknownSync(ChatSummaryConfig)({}));
  assert.equal(parseLarkConfig({}).im, undefined);
});

test("the Lark configuration layer preserves IM overrides and applies field defaults", async () => {
  const config = await Effect.runPromise(
    LarkConfig.pipe(
      Effect.provide(LarkConfig.layer),
      Effect.provideService(
        ConfigProvider.ConfigProvider,
        ConfigProvider.fromUnknown({
          contexts: {
            "/lark": {
              children: {
                "/im": {
                  config: {
                    pollIntervalMs: 5_000,
                    summary: { model: "summary", agentConcurrency: 3 },
                  },
                },
              },
            },
          },
        }),
      ),
    ),
  );
  assert.deepEqual(config.im?.config, {
    pollIntervalMs: 5_000,
    catchUpWindowMs: 3_600_000,
    summary: {
      model: "summary",
      maxMessages: 200,
      agentStartIntervalMs: 10_000,
      agentConcurrency: 3,
    },
  });
});

test("an incomplete summary configuration remains enabled for consumer validation", async () => {
  const config = await Effect.runPromise(
    LarkConfig.pipe(
      Effect.provide(LarkConfig.layer),
      Effect.provideService(
        ConfigProvider.ConfigProvider,
        ConfigProvider.fromUnknown({
          contexts: { "/lark": { children: { "/im": { config: { summary: {} } } } } },
        }),
      ),
    ),
  );
  const im = config.im;
  assert.ok(im);
  assert.throws(() => Schema.decodeUnknownSync(ChatSummaryConfig)(im.config?.summary));
});
