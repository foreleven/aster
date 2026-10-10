import assert from "node:assert/strict";
import { test } from "node:test";
import { ConfigProvider, Effect, Schema } from "effect";
import { LarkSettings, larkSettings } from "../../src/lark/testing.js";
import {
  ChatPollingConfig,
  AgentAdmissionConfig,
  ChatSummaryConfig,
} from "../../src/lark/testing.js";

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
      assert.throws(() => larkSettings({ children: { "/im": { config: { [field]: value } } } }));
    for (const field of ["agentStartIntervalMs", "agentConcurrency", "maxMessages"])
      assert.throws(() =>
        larkSettings({
          children: { "/im": { config: { summary: { model: "summary", [field]: value } } } },
        }),
      );
  }
  assert.throws(() => Schema.decodeUnknownSync(ChatSummaryConfig)({ model: "" }));
  assert.throws(() => Schema.decodeUnknownSync(ChatSummaryConfig)({}));
  assert.equal(larkSettings({}).im, undefined);
});

test("the Lark configuration layer preserves IM overrides and applies field defaults", async () => {
  const config = await Effect.runPromise(
    Effect.scoped(
      LarkSettings.pipe(
        Effect.provide(LarkSettings.layer),
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

test("enabled IM rejects incomplete summary settings at configuration decoding", () => {
  assert.throws(() => larkSettings({ children: { "/im": {} } }));
  assert.throws(() => larkSettings({ children: { "/im": { config: { summary: {} } } } }));
  assert.equal(larkSettings({}).mail, undefined);
});

test("the live configuration boundary rejects incomplete enabled IM instead of falling back to disabled defaults", async () => {
  for (const entry of [
    { children: { "/im": {} } },
    { children: { "/im": { config: { summary: {} } } } },
    { children: { "/unsupported": {} } },
  ]) {
    const result = await Effect.runPromise(
      LarkSettings.pipe(
        Effect.provide(LarkSettings.layer),
        Effect.provideService(
          ConfigProvider.ConfigProvider,
          ConfigProvider.fromUnknown({ contexts: { "/lark": entry } }),
        ),
        Effect.result,
      ),
    );
    assert.equal(result._tag, "Failure");
  }
});
