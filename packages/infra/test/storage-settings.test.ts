import assert from "node:assert/strict";
import { test } from "node:test";
import { ConfigProvider, Effect, Layer, Path } from "effect";
import { ConfigLocation } from "../src/config/location.js";
import { StorageSettings } from "../src/storage/settings.js";
import { MemorySettings } from "../src/memory/settings.js";
test("memory defaults follow the configured durable root while explicit directories override it", async () => {
  for (const dataDir of [undefined, "./custom", "~/custom"]) {
    const sources = Layer.mergeAll(
      Path.layer,
      Layer.succeed(ConfigLocation, {
        baseDir: "/config",
        projectRoot: "/project",
        homeDirectory: "/home/test",
        envPath: "/project/.env",
      }),
      ConfigProvider.layer(
        ConfigProvider.fromUnknown({
          config: { durable: { root: "./data" } },
          contexts: { "/memory": { config: { dataDir } } },
        }),
      ),
    );
    const settings = await Effect.runPromise(
      Effect.scoped(
        MemorySettings.pipe(
          Effect.provide(
            MemorySettings.layer.pipe(
              Layer.provide(StorageSettings.layer.pipe(Layer.provideMerge(sources))),
            ),
          ),
        ),
      ),
    );
    assert.equal(
      settings.dataDir,
      dataDir === undefined
        ? "/config/data/memory"
        : dataDir.startsWith("~/")
          ? "/home/test/custom"
          : "/config/custom",
    );
  }
});
