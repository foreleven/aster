import { Deferred, Effect, Fiber, Logger, References } from "effect";
import assert from "node:assert/strict";
import { test } from "node:test";
import { choice, matchGoal, type GoalScreeningRecord } from "@aster/core";
import { makeSystemOneClient } from "../src/index.js";

const screeningSource = {
  path: "/lark/im/chats/project",
  revision: 1,
  description: "Project chat",
  state: { summary: "Private launch evidence" },
  messages: [],
};
const screeningGoal = { slug: "release", description: "Release readiness" };
const candidate = { definition: screeningGoal, title: screeningGoal.description, summary: "" };
const levelError = "Too many score levels. Must have at most 10 levels.";
const testConfig = {
  url: "https://system-one.test",
  model: "test-model",
  apiKey: "test-secret",
};

test("concurrent System One calls correlate attempts, retries and headers without logging payloads", async () => {
  const logs: string[] = [];
  const annotations: Array<Readonly<Record<string, unknown>>> = [];
  const logger = Logger.make<unknown, void>((entry) => {
    logs.push(...(Array.isArray(entry.message) ? entry.message : [entry.message]).map(String));
    annotations.push(entry.fiber.getRef(References.CurrentLogAnnotations));
  });
  const client = makeSystemOneClient(testConfig, async (_url, init) => {
    if (!new Headers(init?.headers).has("X-TypeSafe-Retry-Count"))
      return Response.json(
        { error: { message: "private response body" } },
        {
          status: 503,
          headers: { "retry-after-ms": "0" },
        },
      );
    return Response.json({ answers: {}, usage: { input_tokens: 1, output_tokens: 1 } });
  });
  await Effect.runPromise(
    Effect.all(
      ["private chat one", "private chat two"].map((state) =>
        client.systemOne({
          state,
          questions: { intent: choice("private question", { yes: "private criterion" }) },
        }),
      ),
      { concurrency: "unbounded" },
    ).pipe(
      Effect.annotateLogs({ contextPath: "/test/logging" }),
      Effect.provide(Logger.layer([logger])),
    ),
  );
  const entries = logs.map((line) => JSON.parse(line));
  const starts = entries.filter((entry) => entry.event === "system-one.request.started");
  assert.equal(starts.length, 2);
  assert.notEqual(starts[0].requestId, starts[1].requestId);
  for (const start of starts) {
    assert.equal(start.timeoutMs, 10000);
    assert.equal(start.maxRetries, 2);
    const request = entries.filter((entry) => entry.requestId === start.requestId);
    assert.deepEqual(
      request
        .filter((entry) => entry.event === "system-one.request.attempt.started")
        .map((entry) => entry.attempt),
      [1, 2],
    );
    const headers = request.filter((entry) => entry.event === "system-one.request.headers");
    assert.deepEqual(
      headers.map((entry) => entry.status),
      [503, 200],
    );
    assert.ok(headers.every((entry) => entry.headersMs >= 0));
    assert.ok(
      request.some(
        (entry) => entry.event === "system-one.request.sdk" && entry.message.includes("retry 1/2"),
      ),
    );
    const completed = request.find((entry) => entry.event === "system-one.request.completed");
    assert.equal(completed.attempt, 2);
    assert.ok(completed.elapsedMs >= 0);
  }
  assert.ok(!logs.join("\n").includes("private"));
  assert.ok(!logs.join("\n").includes(testConfig.apiKey));
  assert.ok(annotations.every((entry) => entry.contextPath === "/test/logging"));
});

test("System One connection failures expose nested network codes and preserve SDK retry limits", async () => {
  const logs: string[] = [];
  const logger = Logger.make<unknown, void>((entry) => {
    logs.push(...(Array.isArray(entry.message) ? entry.message : [entry.message]).map(String));
  });
  const network = Object.assign(new Error("private transport detail"), {
    code: "UND_ERR_CONNECT_TIMEOUT",
  });
  const client = makeSystemOneClient(testConfig, async () => {
    throw new TypeError("fetch failed", {
      cause: new AggregateError([network], "private connection detail"),
    });
  });
  const result = await Effect.runPromise(
    client
      .systemOne({
        state: "private chat",
        questions: { intent: choice("Choose", { yes: "Yes" }) },
      })
      .pipe(Effect.result, Effect.provide(Logger.layer([logger]))),
  );
  assert.equal(result._tag, "Failure");
  const entries = logs.map((line) => JSON.parse(line));
  assert.equal(
    entries.filter((entry) => entry.event === "system-one.request.attempt.started").length,
    3,
  );
  assert.equal(entries.filter((entry) => entry.event === "system-one.request.headers").length, 0);
  const failed = entries.find((entry) => entry.event === "system-one.request.failed");
  assert.deepEqual(failed.errorCodes, ["UND_ERR_CONNECT_TIMEOUT"]);
  assert.equal(failed.attempt, 3);
  assert.ok(
    entries.some(
      (entry) =>
        entry.event === "system-one.request.sdk" &&
        entry.errorCodes.includes("UND_ERR_CONNECT_TIMEOUT"),
    ),
  );
  assert.ok(!logs.join("\n").includes("private"));
  assert.ok(!logs.join("\n").includes(testConfig.apiKey));
});

test("Goal screening respects the System One level limit and normalizes the top score to one", async () => {
  const client = makeSystemOneClient(testConfig, async (_url, init) => {
    const body = JSON.parse(String(init?.body));
    const levels = body.questions.relevance.criteria.length;
    if (levels > 10) return Response.json({ error: { message: levelError } }, { status: 400 });
    return Response.json({
      answers: { relevance: { type: "score", score: levels - 1 } },
      usage: { input_tokens: 0, output_tokens: 0 },
    });
  });
  const result = await Effect.runPromise(matchGoal(client, screeningSource, candidate));
  assert.equal(result._tag === "Matched" ? result.relevance.score : undefined, 1);
});

test("failed Goal screening emits an error log and audit record while retaining the failure", async () => {
  const records: GoalScreeningRecord[] = [];
  const logs: Array<{ level: string; message: unknown }> = [];
  const logger = Logger.make<unknown, void>((entry) => {
    logs.push({ level: entry.logLevel, message: entry.message });
  });
  let requests = 0;
  const client = makeSystemOneClient(testConfig, async () => {
    requests++;
    return Response.json({ error: { message: levelError } }, { status: 400 });
  });
  const result = await Effect.runPromise(
    matchGoal(client, screeningSource, candidate, {
      append: (record) => Effect.sync(() => records.push(record)),
    }).pipe(Effect.result, Effect.provide(Logger.layer([logger]))),
  );
  assert.equal(result._tag, "Failure");
  if (result._tag === "Failure") assert.match(result.failure.message, /400 Too many score levels/);
  assert.equal(requests, 1);
  for (const event of ["system-one.request.failed", "goal.screening.failed"])
    assert.ok(
      logs.some(
        (entry) =>
          entry.level === "Error" &&
          JSON.stringify(entry.message).includes(event) &&
          JSON.stringify(entry.message).includes("400 Too many score levels"),
      ),
    );
  assert.ok(JSON.stringify(logs).includes(screeningSource.path));
  assert.ok(JSON.stringify(logs).includes(screeningGoal.slug));
  assert.equal(records.length, 1);
  assert.equal(records[0]?.admitted, false);
  assert.match(records[0]?.error ?? "", /400 Too many score levels/);
  assert.equal(records[0]?.sourcePath, screeningSource.path);
  assert.equal(records[0]?.goalSlug, screeningGoal.slug);
  assert.ok(!JSON.stringify(logs).includes("Private launch evidence"));
  assert.ok(!JSON.stringify(logs).includes(testConfig.apiKey));
});

test("interrupting a System One fiber aborts the SDK request without retrying", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        let requestSignal: AbortSignal | undefined;
        let requests = 0;
        const client = makeSystemOneClient(
          {
            url: "https://system-one.invalid",
            model: "test",
            apiKey: "test",
          },
          (_url, init) => {
            requests++;
            requestSignal = init?.signal ?? undefined;
            assert.ok(requestSignal);
            Deferred.doneUnsafe(entered, Effect.void);
            return new Promise<Response>((_resolve, reject) => {
              requestSignal!.addEventListener("abort", () => reject(requestSignal!.reason), {
                once: true,
              });
            });
          },
        );
        const fiber = yield* client
          .systemOne({
            state: "test",
            questions: { execute: choice("Execute?", { yes: "Yes", no: "No" }) },
          })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(entered).pipe(Effect.timeout("2 seconds"));
        yield* Fiber.interrupt(fiber);
        assert.equal(requestSignal?.aborted, true);
        assert.equal(requests, 1);
      }),
    ),
  );
});

test("configured URL, model and resolved credential reach the System One request", async () => {
  for (const suffix of ["", "/", "/v1", "/v1/systemone/"]) {
    let request: { url: string; headers: Headers; body: Record<string, unknown> } | undefined;
    const client = makeSystemOneClient(
      {
        url: `http://localhost:8000/proxy${suffix}`,
        model: "multilingual",
        apiKey: "test-laya-key",
      },
      async (url, init) => {
        request = {
          url: String(url),
          headers: new Headers(init?.headers),
          body: JSON.parse(String(init?.body)),
        };
        return Response.json({
          model: "multilingual",
          answers: { intent: { type: "choice", choice: "refund" } },
          usage: { input_tokens: 1, output_tokens: 1 },
        });
      },
    );
    const result = await Effect.runPromise(
      client.systemOne({
        state: "I was charged twice for my order.",
        questions: {
          intent: choice("What does the user need?", { refund: "Refund", shipping: "Shipping" }),
        },
      }),
    );
    assert.equal(request!.url, "http://localhost:8000/proxy/v1/systemone");
    assert.equal(request!.headers.get("Authorization"), "Bearer test-laya-key");
    assert.equal(request!.body.model, "multilingual");
    assert.equal(request!.body.state, "I was charged twice for my order.");
    assert.equal(result.answers.intent.choice, "refund");
  }
});

test("resolved credentials work; missing or invalid settings fail before requests", async () => {
  const config = {
    url: "https://api.typesafe.ai",
    model: "jev-latest",
    apiKey: "literal-test-key",
  };
  const client = makeSystemOneClient(config, async (_url, init) => {
    assert.equal(new Headers(init?.headers).get("Authorization"), "Bearer literal-test-key");
    assert.equal(JSON.parse(String(init?.body)).model, "jev-latest");
    return Response.json({
      answers: { intent: { type: "choice", choice: "a" } },
      usage: { input_tokens: 0, output_tokens: 0 },
    });
  });
  await Effect.runPromise(
    client.systemOne({
      state: "test",
      questions: { intent: choice("intent", { a: "first", b: "second" }) },
    }),
  );
  assert.throws(() => makeSystemOneClient(undefined), /config\.system-one is required/);
  assert.throws(() => makeSystemOneClient({ ...config, apiKey: " " }), /apiKey must be nonempty/);
  assert.throws(() => makeSystemOneClient({ ...config, model: " " }), /model must be nonempty/);
  assert.throws(() => makeSystemOneClient({ ...config, url: "invalid" }), /url must be an HTTP/);
});
