import { Deferred, Effect, Fiber, Logger } from "effect";
import assert from "node:assert/strict";
import { test } from "node:test";
import { choice, matchGoal, type GoalScreeningRecord } from "@aster/core";
import { makeSystemOneClient } from "../src/index.js";

const screeningSource = {
  path: "/lark/im/chats/project",
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

test("Goal screening respects the System One level limit and normalizes the top score to one", async () => {
  const client = makeSystemOneClient(testConfig, {}, async (_url, init) => {
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
  const client = makeSystemOneClient(testConfig, {}, async () => {
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
          {},
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

test("configured URL, model and environment credential reach the System One request", async () => {
  for (const suffix of ["", "/", "/v1", "/v1/systemone/"]) {
    let request: { url: string; headers: Headers; body: Record<string, unknown> } | undefined;
    const client = makeSystemOneClient(
      {
        url: `http://localhost:8000/proxy${suffix}`,
        model: "multilingual",
        apiKey: "${LAYA_API_KEY}",
      },
      { LAYA_API_KEY: "test-laya-key", TYPESAFE_API_KEY: "unrelated-key" },
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

test("literal credentials work; missing settings and environment references fail before requests", async () => {
  const config = {
    url: "https://api.typesafe.ai",
    model: "jev-latest",
    apiKey: "literal-test-key",
  };
  const client = makeSystemOneClient(config, {}, async (_url, init) => {
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
  assert.throws(() => makeSystemOneClient(undefined, {}), /config\.system-one is required/);
  assert.throws(
    () => makeSystemOneClient({ ...config, apiKey: "${CUSTOM_KEY}" }, {}),
    /variable is missing: CUSTOM_KEY/,
  );
  assert.throws(
    () => makeSystemOneClient({ ...config, apiKey: " " }, {}),
    /apiKey must be nonempty/,
  );
  assert.throws(() => makeSystemOneClient({ ...config, model: " " }, {}), /model must be nonempty/);
  assert.throws(
    () => makeSystemOneClient({ ...config, url: "invalid" }, {}),
    /url must be an HTTP/,
  );
});
