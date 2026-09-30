import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { Effect, Layer } from "effect";
import {
  ContextRegistry,
  makeContextRegistry,
  type ContextRecord,
  type ContextStore,
} from "@aster/core";
import { MemoryActor, MemoryRuntime } from "../src/index.js";

const until = (condition: () => boolean) =>
  Effect.gen(function* () {
    while (!condition()) yield* Effect.sleep(2);
  }).pipe(Effect.timeout("3 seconds"));
test("failed outcome memory capture remains durable and retries after restart", async () => {
  const records = new Map<string, ContextRecord>();
  const store: ContextStore = {
    loadAll: () => structuredClone([...records.values()]),
    save: (record) => {
      records.set(record.path, structuredClone(record));
    },
  };
  let attempts = 0;
  const input = {
    sessionId: "/goals/project/runs/one:outcome:completed",
    records: [
      {
        path: "/goals/project/runs/one",
        description: "Completed result",
        state: { status: "completed" },
        messages: ["Result"],
      },
    ],
  };
  for (const restart of [false, true])
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* makeContextRegistry(store);
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.succeed(ContextRegistry, registry),
              Layer.succeed(MemoryRuntime, {
                config: { description: "Memory", dataDir: "/tmp", port: 3111, autoCompress: false },
                client: {
                  capture: async () => {
                    attempts++;
                    if (!restart) throw new Error("offline");
                  },
                  search: async () => ({ mode: "compact", results: [] }),
                  expand: async () => ({ mode: "expanded", results: [], truncated: false }),
                  drain: async () => {},
                  close: () => {},
                },
              }),
            ),
          );
          const memory = yield* system.spawn("memory", MemoryActor);
          if (!restart) {
            yield* memory.tell({ _tag: "Capture", input });
            yield* until(() => attempts === 1);
            assert.equal(
              (registry.get("/memory")!.state as { pending: unknown[] }).pending.length,
              1,
            );
          } else {
            yield* until(
              () =>
                (registry.get("/memory")!.state as { captured?: string[] }).captured?.includes(
                  input.sessionId,
                ) === true,
            );
            assert.equal(attempts, 2);
            assert.equal(
              (registry.get("/memory")!.state as { pending: unknown[] }).pending.length,
              0,
            );
            yield* memory.tell({ _tag: "Capture", input });
            yield* Effect.sleep(10);
            assert.equal(attempts, 2);
          }
        }),
      ),
    );
});
