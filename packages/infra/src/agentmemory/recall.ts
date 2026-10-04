import { MemoryRecall, MemoryRecallError } from "@aster/core";
import { Effect } from "effect";
import type { MemoryClient } from "./client.js";

/** The backend owns transport; core receives only cancellable retrieval capabilities. */
export const makeMemoryRecall = (
  client: Pick<MemoryClient, "search" | "expand">,
): MemoryRecall["Service"] => {
  const failure = (cause: unknown) =>
    new MemoryRecallError({
      cause,
      message: cause instanceof Error ? cause.message : String(cause),
    });
  return {
    search: (query) =>
      Effect.tryPromise({ try: (signal) => client.search(query, { signal }), catch: failure }),
    expand: (ids) =>
      Effect.tryPromise({ try: (signal) => client.expand(ids, signal), catch: failure }),
  };
};
