import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import {
  ContextQueries,
  ContextRegistry,
  ContextQueryError,
  ProcessEnvironment,
  RuntimeIntegrations,
} from "@aster/core";
import { makeContextRegistry } from "@aster/core/testing";
import {
  Clock,
  ConfigProvider,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Logger,
  Schema,
  Sink,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { ChildProcessSpawner } from "effect/process";
import { AppsIntegration } from "../src/apps/integration.js";
import { AppsSettings, type AppsConfiguration } from "../src/apps/config.js";
import { AppState } from "../src/apps/contexts.js";
import { OpenCli } from "../src/apps/client.js";
import { appCommands, queryArgv } from "../src/apps/commands.js";

const settings: AppsConfiguration = {
  description: "Apps",
  apps: [
    { name: "ctrip", description: "Ctrip travel" },
    { name: "xiaohongshu", description: "Travel experiences" },
  ],
};
const launch = Effect.fnUntraced(function* (
  cli: OpenCli["Service"],
  registry: ContextRegistry["Service"],
  config = settings,
) {
  return yield* Effect.gen(function* () {
    yield* Layer.build(AppsIntegration.installation);
    const modules = yield* RuntimeIntegrations;
    const queries = yield* ContextQueries;
    const system = yield* ActorSystem.make().pipe(
      ActorSystem.provide(Layer.succeedContext(modules.installed()[0]!.services)),
    );
    const handle = yield* modules.installed()[0]!.activate(system);
    yield* handle.ready;
    return {
      system,
      handle,
      queries,
      reader: registry.reader,
    };
  }).pipe(
    Effect.provide(Layer.merge(RuntimeIntegrations.layer, ContextQueries.layer)),
    Effect.provideService(ContextRegistry, registry),
    Effect.provideService(AppsSettings, config),
    Effect.provideService(OpenCli, cli),
  );
});
const input = { path: "/apps/ctrip", command: "search", args: { query: "春节 三亚" } };

test("apps register discoverable query-only Contexts without running OpenCLI; queries persist before reply", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const saving = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const controlled: typeof registry = {
          ...registry,
          commit: (record, options) =>
            record.path === input.path && "lastResult" in record.state
              ? Deferred.succeed(saving, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.andThen(registry.commit(record, options)),
                )
              : registry.commit(record, options),
        };
        const mutableCalls: string[][] = [];
        const runtime = yield* launch(
          {
            run: (argv) =>
              Effect.sync(() => {
                mutableCalls.push([...argv]);
                return [{ name: "Sanya", cityId: "43" }];
              }),
          },
          controlled,
        );
        assert.equal(mutableCalls.length, 0);
        const contexts = Object.values(runtime.reader.snapshot());
        assert.deepEqual(contexts.map((c) => c.path).sort(), [
          "/apps",
          "/apps/ctrip",
          "/apps/xiaohongshu",
        ]);
        const ctrip = runtime.reader.get(input.path)!;
        assert.equal(Schema.decodeUnknownSync(AppState)(ctrip.state).mode, "query-only");
        assert.ok(!JSON.stringify(ctrip.state).includes("hotel-search"));
        assert.ok(
          JSON.stringify(yield* runtime.queries.describe(input.path)).includes("hotel-search"),
        );
        const query = yield* runtime.queries.query(input).pipe(Effect.forkScoped);
        yield* Deferred.await(saving);
        assert.equal(
          Schema.decodeUnknownSync(AppState)(registry.get(input.path)!.state).lastResult,
          undefined,
        );
        yield* Deferred.succeed(release, undefined);
        const result = yield* Fiber.join(query);
        assert.deepEqual(result.data, [{ name: "Sanya", cityId: "43" }]);
        assert.deepEqual(
          Schema.decodeUnknownSync(AppState)(registry.get(input.path)!.state).lastResult,
          result,
        );
        assert.deepEqual(mutableCalls, [
          ["ctrip", "search", "春节 三亚", "--limit", "10", "-f", "json"],
        ]);
        assert.equal(registry.backend.journal().length, 0);
        yield* runtime.handle.stop;
        const gone = yield* Effect.flip(runtime.queries.query(input));
        assert.equal(gone.kind, "unavailable");
      }),
    ),
  );
});

test("apps reject writes and invalid arguments before transport; failure leaves last successful evidence intact", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        let calls = 0;
        const runtime = yield* launch(
          {
            run: () =>
              Effect.suspend(() =>
                ++calls === 1
                  ? Effect.succeed([])
                  : Effect.fail(
                      new ContextQueryError({ kind: "failed", message: "Login required" }),
                    ),
              ),
          },
          registry,
        );
        for (const request of [
          { ...input, command: "login" },
          { ...input, command: "__proto__" },
          { ...input, args: { query: "sanya", execute: true } },
          { path: "/apps/xiaohongshu", command: "publish", args: { query: "hello" } },
        ])
          assert.equal((yield* Effect.flip(runtime.queries.query(request))).kind, "invalid-input");
        assert.equal(calls, 0);
        const result = yield* runtime.queries.query(input);
        assert.equal((yield* Effect.flip(runtime.queries.query(input))).message, "Login required");
        assert.deepEqual(
          Schema.decodeUnknownSync(AppState)(registry.get(input.path)!.state).lastResult,
          result,
        );
      }),
    ),
  );
});

test("caller cancellation and runtime stop release query work; busy queries do not start another process", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const started = yield* Deferred.make<void>();
        const released = yield* Deferred.make<void>();
        const runtime = yield* launch(
          {
            run: () =>
              Deferred.succeed(started, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.ensuring(Deferred.succeed(released, undefined)),
              ),
          },
          registry,
        );
        const caller = yield* runtime.queries.query(input).pipe(Effect.forkScoped);
        yield* Deferred.await(started);
        assert.equal((yield* Effect.flip(runtime.queries.query(input))).kind, "busy");
        yield* Fiber.interrupt(caller);
        yield* Deferred.await(released);
        assert.equal(
          Schema.decodeUnknownSync(AppState)(registry.get(input.path)!.state).lastResult,
          undefined,
        );
        yield* runtime.handle.stop;
      }),
    ),
  );
});

test("supported query arguments map to exact OpenCLI argv and validate dates, signed URLs and limits", async () => {
  const signed =
    "https://www.xiaohongshu.com/explore/abc123?xsec_token=token&xsec_source=pc_search";
  assert.deepEqual(
    await Effect.runPromise(
      queryArgv("xiaohongshu", "comments", { url: signed, withReplies: true }),
    ),
    ["xiaohongshu", "comments", signed, "--limit", "10", "--with-replies", "-f", "json"],
  );
  assert.deepEqual(
    await Effect.runPromise(
      queryArgv("ctrip", "hotel-search", {
        city: "43",
        checkin: "2027-02-05",
        checkout: "2027-02-10",
        limit: 5,
      }),
    ),
    [
      "ctrip",
      "hotel-search",
      "43",
      "--checkin",
      "2027-02-05",
      "--checkout",
      "2027-02-10",
      "--limit",
      "5",
      "-f",
      "json",
    ],
  );
  for (const [app, command, args] of [
    ["ctrip", "flight", { from: "SHA", to: "SYX", date: "2027-02-30" }],
    ["ctrip", "hotel-search", { city: "43", checkin: "2027-02-05", checkout: "2027-02-05" }],
    ["ctrip", "search", { query: "--help" }],
    ["ctrip", "search", { query: "sanya", limit: 100 }],
    ["xiaohongshu", "note", { url: "abc123" }],
    ["xiaohongshu", "note", { url: "https://evil.example/explore/abc?xsec_token=t" }],
    ["xiaohongshu", "search", { query: "travel", sort: "invalid" }],
  ] as const)
    assert.equal(
      (await Effect.runPromise(Effect.flip(queryArgv(app, command, args)))).kind,
      "invalid-input",
    );
  assert.equal(Object.keys(appCommands.ctrip).length, 13);
});

test("apps configuration preserves direct child paths; absent configuration disables integration", async () => {
  const read = (value: unknown) =>
    Effect.gen(function* () {
      return yield* AppsSettings;
    }).pipe(
      Effect.provide(AppsSettings.layer),
      Effect.provideService(
        ConfigProvider.ConfigProvider,
        ConfigProvider.fromUnknown(value, { preserveEmptyStrings: true }),
      ),
    );
  assert.deepEqual((await Effect.runPromise(read({}))).apps, []);
  assert.equal(
    (await Effect.runPromise(read({ contexts: { "/apps": { "/ctrip": {} } } }))).apps[0]?.name,
    "ctrip",
  );
  assert.ok(
    Exit.isFailure(
      await Effect.runPromiseExit(
        read({ contexts: { "/apps": { "/ctrip": { description: "" } } } }),
      ),
    ),
  );
});

const fakeSpawner = (
  options: {
    stdout?: string;
    stderr?: string;
    code?: number;
    wait?: Effect.Effect<void>;
    acquired?: () => void;
    released?: () => void;
    inspect?: (command: import("effect/process/ChildProcess").Command) => void;
  } = {},
) =>
  ChildProcessSpawner.make((command) =>
    Effect.acquireRelease(
      Effect.sync(() => {
        options.inspect?.(command);
        options.acquired?.();
        return ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(1),
          exitCode: (options.wait ?? Effect.void).pipe(
            Effect.as(ChildProcessSpawner.ExitCode(options.code ?? 0)),
          ),
          isRunning: Effect.succeed(true),
          kill: () => Effect.void,
          stdin: Sink.drain,
          stdout: Stream.make(new TextEncoder().encode(options.stdout ?? "[]")),
          stderr: Stream.make(new TextEncoder().encode(options.stderr ?? "")),
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
          unref: Effect.succeed(Effect.void),
        });
      }),
      () => Effect.sync(() => options.released?.()),
    ),
  );

const cliRun = (
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
  argv: readonly string[] = ["ctrip", "search", "sanya", "-f", "json"],
) =>
  Effect.gen(function* () {
    return yield* (yield* OpenCli).run(argv);
  }).pipe(
    Effect.provide(OpenCli.layer),
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    Effect.provideService(ProcessEnvironment, {
      values: { PATH: "/bin", HOME: "/tmp", PRIVATE: "secret", MINIMAX_API_KEY: "secret" },
      privateKeys: ["PRIVATE"],
    }),
  );

test("OpenCLI process boundary uses explicit argv/environment, checks exit status/JSON and bounds output", async () => {
  let released = false;
  assert.deepEqual(
    await Effect.runPromise(
      cliRun(
        fakeSpawner({
          stdout: '[{"name":"Sanya"}]',
          released: () => {
            released = true;
          },
          inspect: (command) => {
            assert.equal(command._tag, "StandardCommand");
            if (command._tag !== "StandardCommand") return;
            assert.equal(command.command, "opencli");
            assert.equal(command.options.shell, false);
            assert.equal(command.options.env?.PRIVATE, undefined);
            assert.equal(command.options.env?.MINIMAX_API_KEY, undefined);
            assert.equal(command.options.env?.PATH, "/bin");
          },
        }),
      ),
    ),
    [{ name: "Sanya" }],
  );
  assert.ok(released);
  for (const options of [
    { stdout: "not-json" },
    { stdout: "[]", code: 1 },
    { stdout: "x".repeat(128 * 1024 + 1) },
  ]) {
    const error = await Effect.runPromise(Effect.flip(cliRun(fakeSpawner(options))));
    assert.ok(Schema.is(ContextQueryError)(error));
    assert.equal(error.kind, "failed");
  }
});

test("OpenCLI timeout releases the child process", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* Effect.gen(function* () {
          const started = yield* Deferred.make<void>();
          let released = false;
          const run = yield* cliRun(
            fakeSpawner({
              wait: Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
              released: () => {
                released = true;
              },
            }),
          ).pipe(Effect.forkScoped);
          yield* Deferred.await(started);
          yield* clock.adjust("91 seconds");
          const result = yield* Fiber.await(run);
          assert.ok(Exit.isFailure(result));
          assert.ok(released);
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ),
  );
});

test("stopping an Apps Actor releases active query work and rejects its waiting caller", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const entered = yield* Deferred.make<void>();
        let released = false;
        const runtime = yield* launch(
          {
            run: () =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.ensuring(
                  Effect.sync(() => {
                    released = true;
                  }),
                ),
              ),
          },
          registry,
        );
        const waiting = yield* runtime.queries.query(input).pipe(Effect.flip, Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* runtime.handle.stop;
        assert.equal((yield* Fiber.join(waiting)).kind, "unavailable");
        assert.ok(released);
      }),
    ),
  );
});

test("OpenCLI preserves its structured failure code and actionable diagnostic", async () => {
  const error = await Effect.runPromise(
    Effect.flip(
      cliRun(
        fakeSpawner({
          code: 1,
          stderr: JSON.stringify({
            ok: false,
            error: {
              code: "SECURITY_BLOCK",
              message: "Xiaohongshu search was blocked by request-frequency or security controls.",
              help: "Wait before retrying or check the logged-in browser session.",
              exitCode: 1,
            },
          }),
        }),
      ),
    ),
  );
  assert.match(error.message, /SECURITY_BLOCK/);
  assert.match(error.message, /request-frequency or security controls/);
  assert.match(error.message, /Wait before retrying/);
});

test("OpenCLI reads YAML stderr emitted by version 1.8.8 and excludes private fields", async () => {
  const error = await Effect.runPromise(
    Effect.flip(
      cliRun(
        fakeSpawner({
          code: 1,
          stderr: `ok: false
error:
  code: COMMAND_EXEC
  message: >-
    Cannot read properties of undefined (reading 'search')
    while querying sanya at https://www.xiaohongshu.com/explore/id?xsec_token=signed-token
  help: Check the Xiaohongshu page and login state.
  exitCode: 1
  cause: secret raw browser response
  stack: secret stack
trace:
  dir: /private/traces
# AutoFix: re-run with --trace=retain-on-failure
`,
        }),
      ),
    ),
  );
  assert.match(error.message, /COMMAND_EXEC/);
  assert.match(error.message, /Cannot read properties of undefined/);
  assert.match(error.message, /Check the Xiaohongshu page/);
  assert.doesNotMatch(
    error.message,
    /sanya|xsec_token|signed-token|secret|private\/traces|AutoFix/,
  );
});

for (const [code, kind] of [
  ["ARGUMENT", "invalid-input"],
  ["BROWSER_CONNECT", "unavailable"],
  ["TIMEOUT", "timeout"],
  ["SESSION_BUSY", "busy"],
  ["AUTH_REQUIRED", "failed"],
])
  test(`OpenCLI classifies ${code} and can read a JSON error from stdout`, async () => {
    const error = await Effect.runPromise(
      Effect.flip(
        cliRun(
          fakeSpawner({
            code: 1,
            stdout: JSON.stringify({
              ok: false,
              error: { code, message: "Query failed", help: "Check the local browser" },
            }),
          }),
        ),
      ),
    );
    assert.equal(error.kind, kind);
    assert.match(error.message, new RegExp(code));
  });

test("OpenCLI diagnostics redact credentials and bound large or malformed error output", async () => {
  const error = await Effect.runPromise(
    Effect.flip(
      cliRun(
        fakeSpawner({
          code: 1,
          stderr: JSON.stringify({
            error: {
              code: "UNKNOWN",
              message:
                "secret\nAuthorization: Bearer private-auth\nCookie: session=private-cookie\nxsec_token=private-token\n" +
                "x".repeat(5000),
            },
          }),
        }),
      ),
    ),
  );
  assert.doesNotMatch(error.message, /secret|private-auth|private-cookie|private-token/);
  assert.ok(error.message.length < 1700);
  let released = false;
  const malformed = await Effect.runPromise(
    Effect.flip(
      cliRun(
        fakeSpawner({
          code: 1,
          stderr: "<html>private browser body" + "x".repeat(128 * 1024),
          released: () => {
            released = true;
          },
        }),
      ),
    ),
  );
  assert.ok(released);
  assert.match(malformed.message, /no structured diagnostic/);
  assert.doesNotMatch(malformed.message, /private browser body/);
});

test("OpenCLI diagnostics redact short Chinese query arguments", async () => {
  const error = await Effect.runPromise(
    Effect.flip(
      cliRun(
        fakeSpawner({
          code: 1,
          stderr: JSON.stringify({
            error: { code: "COMMAND_EXEC", message: "Search failed for 春节" },
          }),
        }),
        ["xiaohongshu", "search", "春节", "--limit", "10", "-f", "json"],
      ),
    ),
  );
  assert.match(error.message, /COMMAND_EXEC/);
  assert.doesNotMatch(error.message, /春节/);
});

test("OpenCLI logs a copyable command preserving quotes, spaces and signed URLs", async () => {
  const messages: unknown[] = [];
  const logger = Logger.make(({ message }) => {
    messages.push(message);
  });
  const argv = [
    "xiaohongshu",
    "search",
    "春节 O'Brien $(printf expanded) `printf expanded`",
    "--limit",
    "10",
    "-f",
    "json",
  ];
  for (const args of [
    argv,
    [
      "xiaohongshu",
      "note",
      "https://www.xiaohongshu.com/explore/abc?xsec_token=token&xsec_source=pc_search",
      "-f",
      "json",
    ],
  ]) {
    messages.length = 0;
    await Effect.runPromise(
      cliRun(
        fakeSpawner({
          inspect: () => {
            assert.equal(messages.length, 1);
          },
        }),
        args,
      ).pipe(Effect.provide(Logger.layer([logger]))),
    );
    const [logged] = Schema.decodeUnknownSync(
      Schema.Array(
        Schema.Struct({ event: Schema.Literal("apps.query.command"), command: Schema.String }),
      ),
    )(messages[0]);
    const parsed = execFileSync(
      "/bin/sh",
      ["-c", "set -- " + logged.command + "; printf '%s\\0' \"$@\""],
      { encoding: "utf8" },
    )
      .split("\0")
      .slice(0, -1);
    assert.deepEqual(parsed, ["opencli", ...args]);
    assert.doesNotMatch(logged.command, /MINIMAX_API_KEY|PRIVATE/);
  }
});
