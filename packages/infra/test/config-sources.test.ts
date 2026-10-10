import assert from "node:assert/strict";
import { test } from "node:test";
import {
  Config,
  ConfigProvider,
  Effect,
  FileSystem,
  Layer,
  Path,
  PlatformError,
  Redacted,
} from "effect";
import { ProcessEnvironment } from "@aster/core";
import { LocalConfig } from "../src/config/provider.js";
import { ConfigLocation } from "../src/config/location.js";

const options = {
  configPath: "/project/config.yaml",
  envPath: "/project/.env",
  projectRoot: "/project",
  homeDirectory: "/home/test",
  environment: { ASTER_HTTP_PORT: "4321", SERVICE_KEY: "process-secret" },
};

test("source loading uses injected filesystem/path and snapshots files and environment once", async () => {
  const reads: string[] = [];
  const environment: Record<string, string | undefined> = {
    ...options.environment,
    UNDERSCORE_PRIVATE_KEY: undefined,
  };
  const sources = LocalConfig.layer({ ...options, environment }).pipe(
    Layer.provide(
      Layer.mergeAll(
        Path.layer,
        FileSystem.layerNoop({
          readFileString: (path) =>
            Effect.sync(() => {
              reads.push(path);
              return path.endsWith(".env")
                ? 'SERVICE_KEY="dotenv-secret"\nUNDERSCORE_PRIVATE_KEY=private\n'
                : "http: {port: 4317}";
            }),
        }),
      ),
    ),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const provider = yield* ConfigProvider.ConfigProvider;
        environment.ASTER_HTTP_PORT = "9999";
        environment.SERVICE_KEY = "changed";
        assert.equal(yield* Config.Port("port").pipe(Config.nested("http")), 4321);
        assert.equal(yield* Config.Port("port").pipe(Config.nested("http")), 4321);
        assert.equal((yield* provider.load(["secrets", "SERVICE_KEY"]))?.value, "process-secret");
        const captured = yield* ProcessEnvironment;
        assert.equal(Redacted.value(captured.values).UNDERSCORE_PRIVATE_KEY, "private");
        assert.deepEqual(captured.privateKeys, ["SERVICE_KEY", "UNDERSCORE_PRIVATE_KEY"]);
        assert.equal(JSON.stringify(captured.values).includes("private"), false);
        assert.equal((yield* ConfigLocation).homeDirectory, "/home/test");
      }).pipe(Effect.provide(sources)),
    ),
  );
  assert.deepEqual(reads, ["/project/config.yaml", "/project/.env"]);
});

for (const reason of ["NotFound", "PermissionDenied"] as const) {
  test(`only dotenv NotFound is optional: ${reason}`, async () => {
    const sources = LocalConfig.layer(options).pipe(
      Layer.provide(
        Layer.mergeAll(
          Path.layer,
          FileSystem.layerNoop({
            readFileString: (path) =>
              path.endsWith(".env")
                ? Effect.fail(
                    PlatformError.systemError({
                      _tag: reason,
                      module: "FileSystem",
                      method: "readFileString",
                      pathOrDescriptor: path,
                    }),
                  )
                : Effect.succeed("http: {port: 4317}"),
          }),
        ),
      ),
    );
    const result = await Effect.runPromise(
      Effect.scoped(
        Config.Port("port").pipe(Config.nested("http"), Effect.provide(sources), Effect.result),
      ),
    );
    assert.equal(result._tag, reason === "NotFound" ? "Success" : "Failure");
  });
}
