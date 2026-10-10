import { Redacted, Effect, Layer } from "effect";
import { NodeServices } from "@effect/platform-node";
import { http } from "./http-fixtures.js";
import { openProvenance } from "../src/memory/provenance.js";
import { makeMemoryClient } from "../src/memory/client.js";
import { makeCapture } from "../src/memory/capture.js";
export const memory = (
  dataDir: string,
  fetch: typeof globalThis.fetch,
  readOnly = false,
  timeout = 120000,
) =>
  Effect.gen(function* () {
    const connection = {
      url: "https://memory.test",
      secret: Redacted.make("private"),
      dataDir,
      project: "test",
      cwd: dataDir,
    };
    const provenance = yield* openProvenance(dataDir, readOnly);
    const client = yield* makeMemoryClient(connection, provenance);
    const capture = readOnly
      ? undefined
      : yield* makeCapture(client, provenance, connection, timeout);
    return { client, capture, provenance };
  }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, http(fetch))));
