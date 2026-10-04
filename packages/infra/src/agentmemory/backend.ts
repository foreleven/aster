import { MemoryBackend, MemoryCaptureError } from "@aster/core";
import { Effect } from "effect";
import type { MemoryClient } from "./client.js";
import { makeMemoryRecall } from "./recall.js";

/** The transport boundary exposes only capabilities and public metadata to core. */
export const makeMemoryBackend = (
  client: MemoryClient,
  metadata: Pick<MemoryBackend["Service"], "description" | "retrieval" | "llm">,
): MemoryBackend["Service"] => ({
  ...metadata,
  recall: makeMemoryRecall(client),
  // Agentmemory cannot cancel submitted observations. Its client retains admitted
  // Promises after Fiber interruption; runtime drains them before daemon release.
  // Core's durable queue keeps any result that its Actor has not acknowledged.
  capture: (input) =>
    Effect.tryPromise({
      try: () => client.capture(input),
      catch: (cause) =>
        new MemoryCaptureError({
          cause,
          message: cause instanceof Error ? cause.message : String(cause),
        }),
    }),
  drain: Effect.promise(client.drain),
});
