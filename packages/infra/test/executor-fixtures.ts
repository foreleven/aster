import { Context, Effect, Layer, ManagedRuntime } from "effect";
import { NodeServices } from "@effect/platform-node";
import { FetchHttpClient } from "effect/http";
import type { ExternalAgent } from "@aster/core";
import { makeCodexExecutor } from "../src/executors/codex/executor.js";
class TestExecutor extends Context.Service<TestExecutor, ExternalAgent>()("test/Executor") {}
/** The test runner owns this standalone adapter scope, including explicit shutdown/reopen scenarios. */
export const testCodex = async (
  environment: Readonly<Record<string, string | undefined>>,
  executable: string,
  root: string,
) => {
  const runtime = ManagedRuntime.make(
    Layer.effect(TestExecutor, makeCodexExecutor(environment, root, executable)).pipe(
      Layer.provide(NodeServices.layer),
    ),
  );
  return {
    ...(await runtime.runPromise(TestExecutor)),
    close: () => Effect.promise(() => runtime.dispose()),
  };
};
export const testPlatform = Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer);
