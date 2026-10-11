import assert from "node:assert/strict";
import { test } from "node:test";
import { validateToolArguments } from "@earendil-works/pi-ai";
import { Context, Effect, Schema } from "effect";
import { schemaTool } from "../src/index.js";

test("schema tools expose required/nested/union/optional fields to native validation", async () => {
  const parameters = Schema.Struct({
    instructions: Schema.NonEmptyString,
    action: Schema.TaggedUnion({
      Search: { query: Schema.String },
      Expand: { id: Schema.String },
    }),
    evidence: Schema.optional(Schema.Array(Schema.Struct({ content: Schema.String }))),
  });
  const tool = schemaTool({
    name: "research",
    label: "Research",
    description: "Research",
    replay: "safe",
    parameters,
    execute: (_id, args) => Effect.succeed({ content: [], details: args }),
  });
  const input = { instructions: "Find", action: { _tag: "Search", query: "Trips" } };
  const validated = validateToolArguments(tool, {
    type: "toolCall",
    id: "one",
    name: "research",
    arguments: input,
  });
  const result = await Effect.runPromise(tool.execute("one", validated));
  assert.deepEqual(result.details, input);
  assert.throws(
    () =>
      validateToolArguments(tool, {
        type: "toolCall",
        id: "bad",
        name: "research",
        arguments: { action: input.action },
      }),
    /instructions/,
  );
  assert.equal(
    (
      await Effect.runPromise(
        tool.execute("stale", {
          ...input,
          replyTo: "/private/actor",
        }),
      )
    ).isError,
    true,
  );
});

test("runtime-only checks return safe feedback without entering tool execution", async () => {
  let calls = 0;
  const tool = schemaTool({
    name: "check",
    label: "Check",
    description: "Check",
    replay: "never",
    parameters: Schema.Struct({
      value: Schema.String.check(Schema.makeFilter((value) => value === "allowed")),
    }),
    execute: () =>
      Effect.sync(() => {
        calls++;
        return { content: [], details: "accepted" };
      }),
  });
  const result = await Effect.runPromise(
    tool.execute("one", { value: "private-disallowed-value" }),
  );
  assert.equal(result.isError, true);
  assert.deepEqual(result.details, {
    code: "invalid_arguments",
    message: "Invalid tool arguments",
    issues: [{ path: ["value"], message: "Field does not satisfy its declared constraint" }],
  });
  assert.equal(JSON.stringify(result).includes("private-disallowed-value"), false);
  assert.equal(calls, 0);
  assert.equal(
    (await Effect.runPromise(tool.execute("two", { value: "allowed" }))).details,
    "accepted",
  );
});

test("empty-argument schema tools retain Effect service requirements", async () => {
  class Caller extends Context.Service<Caller, { readonly name: string }>()("test/schemaCaller") {}
  const tool = schemaTool({
    name: "current",
    label: "Current",
    description: "Current",
    replay: "safe",
    parameters: Schema.Struct({}),
    execute: () => Caller.use((caller) => Effect.succeed({ content: [], details: caller.name })),
  });
  const _work = tool.execute("one", {});
  const requirement: [Effect.Services<typeof _work>] extends [Caller] ? true : false = true;
  assert.equal(requirement, true);
  const args = validateToolArguments(tool, {
    type: "toolCall",
    id: "one",
    name: "current",
    arguments: {},
  });
  const result = await Effect.runPromise(
    tool.execute("one", args).pipe(Effect.provideService(Caller, { name: "owner" })),
  );
  assert.equal(result.details, "owner");
});

test("tool validation feedback preserves declared field paths and masks arbitrary map keys", async () => {
  const tool = schemaTool({
    name: "check",
    label: "Check",
    description: "Check",
    replay: "safe",
    parameters: Schema.Struct({
      evidence: Schema.Array(Schema.Struct({ content: Schema.NonEmptyString })),
      args: Schema.Record(Schema.String, Schema.String),
    }),
    execute: () => Effect.succeed({ content: [], details: "accepted" }),
  });
  const result = await Effect.runPromise(
    tool.execute("bad", {
      evidence: [{ content: "" }],
      args: { "private-record-key": { value: "private-value" } },
      "private-excess-field": "private-value",
    }),
  );
  const text = JSON.stringify(result);
  assert.equal(result.isError, true);
  assert.match(text, /evidence/);
  assert.match(text, /content/);
  assert.match(text, /args/);
  assert.match(text, /<key>/);
  assert.equal(text.includes("private-record-key"), false);
  assert.equal(text.includes("private-excess-field"), false);
  assert.equal(text.includes("private-value"), false);
});
