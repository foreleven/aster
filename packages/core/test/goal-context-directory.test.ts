import assert from "node:assert/strict";
import { test } from "node:test";
import { ConfigProvider, Effect, Layer } from "effect";
import { ContextDirectory } from "../src/config/context-directory.js";

const settings = (contexts?: unknown) =>
  ContextDirectory.pipe(
    Effect.provide(
      ContextDirectory.layer.pipe(
        Layer.provide(
          ConfigProvider.layer(
            ConfigProvider.fromUnknown(
              { config: { goals: { model: "test" } }, contexts },
              { preserveEmptyStrings: true },
            ),
          ),
        ),
      ),
    ),
  );

test("Context directory retains only explicit Context paths and descriptions across both child layouts", async () => {
  const result = await Effect.runPromise(
    Effect.scoped(
      settings({
        "/lark": {
          description: "Work account",
          children: { "/im": { description: "Retained work conversations" } },
          config: { token: "private-token", "/hidden": { description: "Not a Context" } },
        },
        "/apps": {
          description: "Travel sources",
          "/xiaohongshu": { description: "Search travel notes and comments" },
        },
        "/mail": {
          children: { "/personal": { description: "Personal email" } },
          config: { password: "private-password" },
        },
      }),
    ),
  );
  assert.deepEqual(result, [
    { path: "/apps", description: "Travel sources" },
    { path: "/apps/xiaohongshu", description: "Search travel notes and comments" },
    { path: "/lark", description: "Work account" },
    { path: "/lark/im", description: "Retained work conversations" },
    { path: "/mail/personal", description: "Personal email" },
  ]);
  assert.doesNotMatch(JSON.stringify(result), /private-|hidden|password|token/);
});

test("missing Context configuration produces an empty directory", async () => {
  assert.deepEqual(await Effect.runPromise(Effect.scoped(settings())), []);
});

test("invalid Context metadata fails settings acquisition instead of reaching the prompt", async () => {
  for (const contexts of [
    { "/apps": { description: { invalid: true } } },
    { "/apps": { description: "" } },
    { "/apps": { children: "invalid" } },
  ]) {
    const result = await Effect.runPromise(Effect.scoped(settings(contexts).pipe(Effect.result)));
    assert.equal(result._tag, "Failure");
  }
});
