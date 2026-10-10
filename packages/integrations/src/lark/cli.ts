import { ProcessEnvironment } from "@aster/core";
import { Context, Effect, Layer, Schema, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { LarkCliError } from "./errors.js";
import { LarkSettings } from "./settings.js";

/** Single process boundary. Inject this capability to test all provider clients. */
export class LarkCli extends Context.Service<
  LarkCli,
  {
    readonly run: (argv: readonly string[]) => Effect.Effect<string, LarkCliError>;
  }
>()("lark/Cli") {
  static readonly layer = Layer.effect(
    LarkCli,
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const environment = yield* ProcessEnvironment;
      const settings = yield* LarkSettings;
      const env: NodeJS.ProcessEnv = {
        ...environment.values,
        LARKSUITE_CLI_NO_UPDATE_NOTIFIER: "1",
        LARKSUITE_CLI_NO_SKILLS_NOTIFIER: "1",
      };
      for (const key of environment.privateKeys) delete env[key];
      return {
        run: Effect.fn("LarkCli.run")((argv: readonly string[]) =>
          Effect.scoped(
            Effect.gen(function* () {
              const handle = yield* spawner.spawn(
                ChildProcess.make(
                  "lark-cli",
                  [...(settings.profile ? ["--profile", settings.profile] : []), ...argv],
                  {
                    env,
                    extendEnv: false,
                    shell: false,
                    stdin: "ignore",
                    forceKillAfter: "2 seconds",
                  },
                ),
              );
              let bytes = 0;
              const output = handle.stdout.pipe(
                Stream.mapEffect((chunk) => {
                  bytes += chunk.byteLength;
                  return bytes <= 32 * 1024 * 1024
                    ? Effect.succeed(chunk)
                    : Effect.fail(
                        new LarkCliError({ message: "Lark output exceeded 32 MiB", cause: bytes }),
                      );
                }),
                Stream.decodeText(),
                Stream.mkString,
              );
              // Drain both pipes even when the command fails. Diagnostics can contain private payloads.
              const [, stdout, code] = yield* Effect.all(
                [Stream.runDrain(handle.stderr), output, handle.exitCode],
                { concurrency: "unbounded" },
              );
              if (code !== 0)
                return yield* new LarkCliError({
                  message: `Lark command failed with exit code ${code}`,
                  cause: code,
                });
              return stdout;
            }),
          ).pipe(
            Effect.timeout("60 seconds"),
            Effect.catchTag("TimeoutError", (cause) =>
              Effect.fail(
                new LarkCliError({ kind: "timeout", message: "Lark command timed out", cause }),
              ),
            ),
            Effect.catchTag("PlatformError", (cause) =>
              Effect.fail(
                new LarkCliError({
                  kind: "unavailable",
                  message: "Cannot execute lark-cli",
                  cause,
                }),
              ),
            ),
          ),
        ),
      };
    }),
  );
}

const Envelope = Schema.Struct({
  ok: Schema.optional(Schema.Boolean),
  data: Schema.optional(Schema.Unknown),
  error: Schema.optional(Schema.Struct({ message: Schema.optional(Schema.String) })),
});
/** Decode JSON and the CLI envelope before endpoint-specific decoding. */
export const decodeCliOutput = Effect.fn("LarkCli.decode")(function* (stdout: string) {
  const json = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(
    stdout,
  ).pipe(
    Effect.mapError((cause) => new LarkCliError({ message: "Lark returned invalid JSON", cause })),
  );
  const envelope = yield* Schema.decodeUnknownEffect(Envelope)(json).pipe(
    Effect.mapError(
      (cause) => new LarkCliError({ message: "Lark returned an invalid envelope", cause }),
    ),
  );
  if (envelope.ok === false)
    return yield* new LarkCliError({
      message: envelope.error?.message ?? "Lark request failed",
      cause: envelope.error,
    });
  return envelope.data ?? json;
});
