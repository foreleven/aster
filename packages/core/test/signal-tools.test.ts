import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Schema } from "effect";
import {
  CreateSignalInput,
  UpdateSignalInput,
  ControlSignalInput,
} from "../src/signals/contracts.js";
import { ownedSignalPath, presentSignal, signalDefinition } from "../src/signals/operations.js";

test("Signal model inputs omit Actor metadata, normalize work defaults and round-trip public rules", async () => {
  const input = await Effect.runPromise(
    Schema.decodeUnknownEffect(CreateSignalInput)({
      id: "travel",
      trigger: { type: "context", when: "New relevant travel information" },
      action: {
        type: "start_task",
        title: "Family travel options",
        instructions: "Find family options",
        contextPaths: ["/apps/ctrip", "/apps/ctrip"],
        evidence: [{ content: "Two adults and one child" }],
      },
    }),
  );
  const definition = signalDefinition(input, "/goals/personal");
  assert.equal(definition.action._tag, "StartTask");
  if (definition.action._tag !== "StartTask") return;
  assert.equal(definition.action.task.replyTo, "/goals/personal");
  assert.equal(definition.action.task.task.title, "Family travel options");
  assert.deepEqual(definition.action.task.task.contextPaths, ["/apps/ctrip"]);
  assert.deepEqual(definition.action.task.task.input, [
    { content: "Two adults and one child", sources: [] },
  ]);
  assert.equal(ownedSignalPath("/goals/personal", "travel"), "/signals/personal--travel");
  const projected = presentSignal(
    "/signals/personal--travel",
    {
      ...definition,
      owner: "/goals/personal",
      status: "active",
      version: 1,
    },
    "/goals/personal",
  );
  assert.equal(projected.id, "travel");
  assert.equal(projected.action.type, "start_task");
  assert.ok("title" in projected.action && projected.action.title === "Family travel options");
  assert.ok(!JSON.stringify(projected).includes("replyTo"));
  assert.ok(!JSON.stringify(projected).includes('"_tag"'));
  assert.equal(
    (
      await Effect.runPromise(
        Schema.decodeUnknownEffect(CreateSignalInput)({
          id: "discover",
          trigger: { type: "context", when: "Changed" },
          action: {
            type: "start_task",
            title: "Discover useful sources",
            instructions: "Discover useful sources",
          },
        }),
      )
    ).action.type,
    "start_task",
  );
});

test("Signal controls require observed positive versions and schedules require timezone-qualified instants", async () => {
  for (const version of [undefined, 0, -1, 1.5]) {
    assert.equal(
      Schema.decodeUnknownResult(ControlSignalInput)({ id: "watch", operation: "pause", version })
        ._tag,
      "Failure",
    );
    assert.equal(
      (
        await Effect.runPromise(
          Schema.decodeUnknownEffect(UpdateSignalInput)({
            id: "watch",
            version,
            trigger: { type: "context", when: "Changed" },
            action: { type: "send_goal_message", goalPath: "/goals/personal", text: "Review" },
          }).pipe(Effect.result),
        )
      )._tag,
      "Failure",
    );
  }
  const result = await Effect.runPromise(
    Schema.decodeUnknownEffect(CreateSignalInput)({
      id: "timer",
      trigger: { type: "once", at: "2026-10-11T12:00:00" },
      action: {
        type: "delegate_task",
        executor: "research",
        title: "Investigate",
        instructions: "Investigate",
      },
    }).pipe(Effect.result),
  );
  assert.equal(result._tag, "Failure");
});

test("Signal SDK wire schemas retain shallow required fields and optional evidence", () => {
  const InputSchema = Schema.Struct({
    required: Schema.Array(Schema.String),
    properties: Schema.Struct({
      action: Schema.Struct({
        anyOf: Schema.Array(
          Schema.Struct({
            required: Schema.Array(Schema.String),
          }),
        ),
      }),
    }),
  });
  const wire = Schema.decodeUnknownSync(InputSchema)(
    Schema.toJsonSchemaDocument(CreateSignalInput).schema,
  );
  assert.deepEqual(wire.required, ["id", "trigger", "action"]);
  assert.deepEqual(
    wire.properties.action.anyOf.map((variant) => variant.required),
    [
      ["type", "title", "instructions"],
      ["type", "executor", "title", "instructions"],
      ["type", "goalPath", "text"],
    ],
  );
  const serialized = JSON.stringify(Schema.toJsonSchemaDocument(UpdateSignalInput));
  assert.doesNotMatch(serialized, /replyTo|remainingAgentTurns|requestId|_tag/);
});
