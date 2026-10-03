import { makeMemoryRecall } from "./recall.js";
import { MemoryCaptureError } from "./errors.js";
import {
  ContextActor,
  ContextRegistry,
  ContextRecord,
  defineContext,
  contextView,
  ConfigLocation,
  ProcessEnvironment,
  RuntimeIntegrations,
  defineIntegration,
  ContextCaptureSink,
  MemoryRecall,
  secretConfig,
  validateConfig,
} from "@aster/core";
import { Config, ConfigProvider, Context, Deferred, Effect, Layer, Redacted, Schema } from "effect";
import type { ActorRef } from "@aster/actor";
import { writeFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { MemoryEntry, parseMemoryConfig, type MemoryConfig } from "./config.js";
import { managedMemory } from "./runtime.js";
import type { MemoryClient } from "./client.js";
import { openMemoryReader } from "./reader.js";
export * from "./config.js";
export * from "./client.js";
export * from "./runtime.js";
export * from "./reader.js";

export class MemoryRuntime extends Context.Service<
  MemoryRuntime,
  {
    readonly config: MemoryConfig;
    readonly client: MemoryClient;
    readonly connectionPath?: string;
  }
>()("memory/Runtime") {}

export const memorySettings = Effect.gen(function* () {
  const location = yield* ConfigLocation;
  const entry = yield* Config.schema(MemoryEntry, ["contexts", "/memory"]).pipe(
    Config.withDefault({}),
  );
  return yield* validateConfig("Memory", () => parseMemoryConfig(entry, location.baseDir));
});

export const configuredMemoryReader = memorySettings.pipe(
  Effect.flatMap((settings) => openMemoryReader(join(settings.dataDir, "connection.json"))),
);

/** Infrastructure Layer: owns the daemon and its connection file. */
export const AgentMemoryBackend = {
  layer: Layer.effect(
    MemoryRuntime,
    Effect.gen(function* () {
      const config = yield* memorySettings;
      const location = yield* ConfigLocation;
      const environment = { ...(yield* ProcessEnvironment).values };
      const provider = yield* ConfigProvider.ConfigProvider;
      for (const name of [config.llm?.apiKeyEnv, config.embedding?.apiKeyEnv]) {
        if (name) environment[name] = Redacted.value(yield* secretConfig(`\${${name}}`, provider));
      }
      const runtime = yield* managedMemory(config, location.projectRoot, environment);
      const connectionPath = join(config.dataDir, "connection.json");
      yield* Effect.acquireRelease(
        Effect.tryPromise(() =>
          writeFile(connectionPath, JSON.stringify(runtime.connection), { mode: 0o600 }),
        ),
        () => Effect.promise(() => rm(connectionPath, { force: true })),
      );
      return { config, client: runtime.client, connectionPath };
    }),
  ),
};

/** Runtime installs the capture consumer before Context reactions or sources start. */
export const MemoryIntegration = {
  layer: Layer.effectContext(
    Effect.gen(function* () {
      const backend = yield* MemoryRuntime;
      const registry = yield* ContextRegistry;
      yield* registry.registerViews([memoryView]);
      const integrations = yield* RuntimeIntegrations;
      const ready = yield* Deferred.make<ActorRef<MemoryCommand>>();
      yield* integrations.register(
        defineIntegration({
          name: "memory",
          phase: "consumer",
          services: Context.make(MemoryRuntime, backend).pipe(
            Context.add(ContextRegistry, registry),
          ),
          activate: (system) =>
            Effect.gen(function* () {
              const root = yield* system.spawn("memory", MemoryActor);
              yield* Deferred.succeed(ready, root);
              return { ready: Effect.void, stop: system.stop(root) };
            }),
        }),
      );
      return Context.make(MemoryRecall, makeMemoryRecall(backend.client)).pipe(
        Context.add(ContextCaptureSink, {
          capture: (input) =>
            Deferred.await(ready).pipe(
              Effect.flatMap((root) => root.tell({ _tag: "Capture", input })),
            ),
          drain: Effect.promise(backend.client.drain),
        }),
      );
    }),
  ),
};

export const MemoryCommand = Schema.Union([
  Schema.TaggedStruct("Retry", {}),
  Schema.TaggedStruct("Capture", {
    input: Schema.Struct({ sessionId: Schema.String, records: Schema.Array(ContextRecord) }),
  }),
  Schema.TaggedStruct("Captured", {
    sessionId: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: Schema.Void }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(MemoryCaptureError) }),
    ]),
  }),
]);
export type MemoryCommand = typeof MemoryCommand.Type;

const memoryView = contextView({
  matches: (path) => path === "/memory",
  state: Schema.Struct({
    status: Schema.Literal("ready"),
    retrieval: Schema.Literals(["bm25", "hybrid"]),
    llm: Schema.optional(Schema.Struct({ provider: Schema.String, model: Schema.String })),
  }),
});

export class MemoryActor extends ContextActor.Service<MemoryActor, MemoryRuntime>()(
  "memory/Actor",
  {
    command: MemoryCommand,
    context: defineContext({
      view: memoryView,
      identity: "My long-term memory",
      state: Schema.Struct({
        pending: Schema.optional(
          Schema.Array(
            Schema.Struct({ sessionId: Schema.String, records: Schema.Array(ContextRecord) }),
          ),
        ),
        captured: Schema.optional(Schema.Array(Schema.String)),
        status: Schema.Literal("ready"),
        retrieval: Schema.Literals(["bm25", "hybrid"]),
        llm: Schema.optional(Schema.Struct({ provider: Schema.String, model: Schema.String })),
      }),
      message: Schema.Never,
    }),
  },
) {
  static readonly layer = Layer.effect(
    MemoryActor,
    Effect.gen(function* () {
      const { client, config } = yield* MemoryRuntime;
      const registry = yield* ContextRegistry;
      yield* registry.registerViews([memoryView]);
      const inFlight = new Set<string>();
      const state = () =>
        registry.get("/memory")!.state as {
          pending?: { sessionId: string; records: readonly (typeof ContextRecord.Type)[] }[];
          captured?: string[];
        };
      const save = (patch: object) =>
        Effect.suspend(() => {
          const current = registry.get("/memory")!;
          return registry
            .commit(
              { ...current, state: { ...current.state, ...patch } },
              { expectedRevision: current.revision ?? 0 },
            )
            .pipe(Effect.asVoid, Effect.orDie);
        });
      return MemoryActor.of({
        started: (context) =>
          Effect.gen(function* () {
            const previous = registry.get("/memory");
            yield* registry
              .commit(
                {
                  path: "/memory",
                  description: config.description,
                  state: {
                    ...previous?.state,
                    status: "ready",
                    retrieval: config.embedding ? "hybrid" : "bm25",
                    ...(config.llm
                      ? { llm: { provider: config.llm.provider, model: config.llm.model } }
                      : {}),
                  },
                  messages: [],
                },
                { expectedRevision: previous?.revision ?? 0 },
              )
              .pipe(Effect.asVoid, Effect.orDie);
            yield* context.self.tell({ _tag: "Retry" });
          }),
        receive: (command, context) =>
          Effect.gen(function* () {
            if (command._tag === "Capture") {
              if (
                state().captured?.includes(command.input.sessionId) ||
                state().pending?.some((p) => p.sessionId === command.input.sessionId)
              )
                return;
              yield* save({
                pending: [
                  ...(state().pending ?? []),
                  { ...command.input, records: command.input.records.map(registry.project) },
                ],
              });
            } else if (command._tag === "Captured") {
              inFlight.delete(command.sessionId);
              if (command.result._tag === "Success")
                yield* save({
                  pending: state().pending?.filter((p) => p.sessionId !== command.sessionId) ?? [],
                  captured: [...(state().captured ?? []), command.sessionId],
                });
              else
                yield* Effect.logError(
                  `Memory capture will retry for ${command.sessionId}: ${command.result.error.message}`,
                );
              return;
            } else {
              // Recover a capture whose Context commit preceded a process interruption.
              for (const record of Object.values(registry.snapshot())) {
                const capture = registry.definition(record.path)?.capture?.(record);
                if (
                  capture &&
                  !state().captured?.includes(capture.sessionId) &&
                  !state().pending?.some((p) => p.sessionId === capture.sessionId)
                )
                  yield* save({
                    pending: [
                      ...(state().pending ?? []),
                      { ...capture, records: capture.records.map(registry.project) },
                    ],
                  });
              }
              yield* context.pipeToSelf(Effect.sleep("30 seconds"), () => ({ _tag: "Retry" }));
            }
            for (const input of state().pending ?? []) {
              if (inFlight.size >= 2) break;
              if (inFlight.has(input.sessionId)) continue;
              inFlight.add(input.sessionId);
              yield* context.pipeToSelf(
                Effect.tryPromise({
                  try: () =>
                    client.capture({ ...input, records: input.records.map(registry.project) }),
                  catch: (cause) =>
                    new MemoryCaptureError({
                      cause,
                      message: cause instanceof Error ? cause.message : String(cause),
                    }),
                }),
                (result) => ({ _tag: "Captured", sessionId: input.sessionId, result }),
              );
            }
          }),
      });
    }),
  );
}
export * from "./errors.js";

export * from "./recall.js";
