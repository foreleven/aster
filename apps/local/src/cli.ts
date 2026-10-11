#!/usr/bin/env node
import { NodeServices } from "@effect/platform-node";
import { FetchHttpClient } from "effect/http";
import { localConfiguration } from "./configuration.js";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { Effect, Layer } from "effect";
import { openMemoryReader, configuredMemoryReader } from "@aster/infra";
import { startApplication } from "./application.js";

const projectRoot = fileURLToPath(new URL("../../..", import.meta.url));
const defaultConfigPath = resolve(projectRoot, "aster.config.yaml");

const readContext = (path: string) => {
  const snapshotPath = process.env.SIGNALS_CONTEXT_SNAPSHOT;
  if (!snapshotPath) throw new Error("SIGNALS_CONTEXT_SNAPSHOT is not set");
  const snapshot = JSON.parse(readFileSync(snapshotPath, "utf8")) as Record<string, unknown>;
  const context = snapshot[path];
  if (context === undefined) throw new Error(`Context not found: ${path}`);
  console.log(JSON.stringify(context, null, 2));
};

const recallMemory = async (command: "search" | "expand", args: string[]) => {
  const { values, positionals } = parseArgs({
    args,
    allowPositionals: true,
    options: {
      config: { type: "string" },
      limit: { type: "string" },
      session: { type: "string" },
    },
  });
  if (command === "search" && (positionals.length !== 1 || values.session !== undefined)) {
    throw new Error("Usage: aster memory search <query> [--limit N] [--config FILE]");
  }
  if (command === "expand" && (positionals.length === 0 || values.limit !== undefined)) {
    throw new Error(
      "Usage: aster memory expand <obs-id> [<obs-id>...] [--session ID] [--config FILE]",
    );
  }
  const configPath = values.config ?? defaultConfigPath;
  const reader = process.env.SIGNALS_MEMORY_CONNECTION
    ? openMemoryReader(process.env.SIGNALS_MEMORY_CONNECTION)
    : configuredMemoryReader.pipe(Effect.provide(localConfiguration(projectRoot, configPath)));
  const result = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const client = yield* reader;
        return yield* command === "search"
          ? client.search(positionals[0]!, {
              limit: values.limit === undefined ? 10 : Number(values.limit),
            })
          : client.expand(
              positionals.map((obsId) => ({
                obsId,
                ...(values.session === undefined ? {} : { sessionId: values.session }),
              })),
            );
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
  );
  console.log(JSON.stringify(result, null, 2));
};

const main = async () => {
  if (process.env.SIGNALS_AGENT_NETWORK_PROXY !== "true")
    process.env.NO_PROXY = [
      process.env.NO_PROXY ?? process.env.no_proxy ?? "",
      "127.0.0.1",
      "localhost",
      "::1",
    ]
      .filter(Boolean)
      .join(",");
  const args = process.argv.slice(2);
  const [command, subcommand, value] = args;
  if (command === "context" && subcommand === "get" && value) {
    readContext(value);
    return;
  }
  if (command === "memory" && (subcommand === "search" || subcommand === "expand")) {
    await recallMemory(subcommand, args.slice(2));
    return;
  }
  if (command === "start") {
    const configPath = subcommand === "--config" ? value : subcommand;
    await startApplication(projectRoot, configPath ?? defaultConfigPath, {
      source: import.meta.url.endsWith(".ts"),
    });
    return;
  }
  throw new Error(
    "Usage: aster start [--config FILE] | aster context get <path> | aster memory search <query> [--limit N] [--config FILE] | aster memory expand <obs-id> [<obs-id>...] [--session ID] [--config FILE]",
  );
};

main().catch((error: unknown) => {
  // Preserve nested causes and stacks, including Effect's wrapped startup failures.
  console.error(error);
  process.exitCode = 1;
});
