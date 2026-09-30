import { test, expect } from "@playwright/test";
const at = "2026-09-29T12:30:00Z";
function fixture() {
  const context = (path, description, state = {}, messages = []) => ({
    path,
    description,
    state,
    messages,
  });
  const actor = (path, contextPath) => ({
    path,
    contextPath,
    parent: path.slice(0, path.lastIndexOf("/")),
    incarnation: path,
    status: "running",
    phase: "running",
    pendingEffects: 0,
    failures: 0,
    restarts: 0,
    processing: false,
    mailboxSize: 0,
    processed: 12,
    lastActivity: at,
  });
  return {
    at,
    runtime: {
      phase: "ready",
      actors: [
        actor("/user/lark", "/lark"),
        actor("/user/lark/im", "/lark/im"),
        actor("/user/lark/im/chat-1", "/lark/im/chats/chat-1"),
        actor("/user/goals", "/goals"),
        actor("/user/goals/engine", "/goals/engine"),
        actor("/user/signals", "/signals"),
        actor("/user/signals/progress", "/signals/progress"),
        actor("/user/signals/progress/run-1", "/signals/progress/runs/run-1"),
        actor("/user/approvals", "/approvals"),
        actor("/user/memory", "/memory"),
      ],
      events: [
        {
          _tag: "CommandProcessed",
          incarnation: "run-1",
          commandTag: "TaskPrepared",
          path: "/user/signals/progress/run-1",
          timestamp: at,
          success: true,
        },
        {
          _tag: "CommandProcessed",
          incarnation: "chat-1",
          commandTag: "Summarized",
          path: "/user/lark/im/chat-1",
          timestamp: at,
          success: true,
        },
      ],
    },
    contexts: [
      context("/lark", "My work account", { status: "ready" }),
      context("/lark/im", "Work IM", { ready: true }),
      context(
        "/lark/im/chats/chat-1",
        "Knowledge Engine · Frontend discussion",
        { summary: "Core workflow integration is complete; validation starts this week." },
        [
          {
            type: "Summary",
            text: "Core workflow integration is complete; validation starts this week.",
            at,
          },
        ],
      ),
      context(
        "/goals/engine",
        "Monitor Knowledge Engine project progress",
        { slug: "engine", status: "active", progress: "Awaiting test feedback" },
        [
          {
            type: "assistant",
            text: "Monitoring project progress. Next, track integration and validation results.",
            at,
          },
        ],
      ),
      context("/signals/progress", "Significant change in project progress", {
        goal: "engine",
        active: true,
      }),
      context(
        "/signals/progress/runs/run-1",
        "Summarize Knowledge Engine project progress",
        {
          status: "awaiting-confirmation",
          sourcePath: "/lark/im/chats/chat-1",
          definition: { goal: "engine" },
        },
        [
          { type: "Triggered", at },
          {
            type: "TaskPrepared",
            task: { instructions: "Summarize this week’s project progress", input: [] },
            at,
          },
          { type: "ConfirmationRequested", at },
        ],
      ),
      context("/approvals", "Task approval queue", {
        entries: [
          {
            id: "approval-1",
            target: "/user/signals/progress/run-1",
            contextPath: "/signals/progress/runs/run-1",
            kind: "confirmation",
            status: "pending",
            request: {
              id: "approval-1",
              kind: "approval",
              prompt:
                "Summarize this week’s Knowledge Engine integration progress and remaining validation work.",
            },
          },
          {
            id: "question-1",
            target: "/user/signals/progress/run-1",
            contextPath: "/signals/progress/runs/run-1",
            kind: "input",
            status: "pending",
            request: {
              id: "question-1",
              kind: "input",
              prompt: "Specify the release scope",
              questions: [
                {
                  id: "scope",
                  prompt: "Which environment should receive the release?",
                  options: ["Test", "Production"],
                },
              ],
            },
          },
        ],
      }),
      context("/memory", "Long-term work memory", { status: "ready" }),
      context("/signals/old/runs/old", "Historical execution record", { status: "completed" }, [
        { type: "Completed", text: "Historical result", at },
      ]),
    ],
  };
}
async function setup(page, data = fixture()) {
  const errors = [];
  page.on("pageerror", (e) => errors.push(e.message));
  page.on("console", (message) => {
    if (message.type() === "error" || message.type() === "warning") errors.push(message.text());
  });
  const writes = [];
  const reads = [];
  const historyRequests = [];
  // Keep transport callbacks deterministic; the real-server test covers native EventSource.
  await page.addInitScript(() => {
    window.testEvents = { opened: 0, closed: 0 };
    window.EventSource = class extends EventTarget {
      constructor() {
        super();
        window.testEvents.opened++;
        window.testEvents.emit = (type, data = {}) =>
          this.dispatchEvent(new MessageEvent(type, { data: JSON.stringify(data) }));
        queueMicrotask(() => window.testEvents.emit("ready"));
      }
      close() {
        window.testEvents.closed++;
      }
    };
  });
  await page.route(/^https?:\/\/[^/]+\/api\//, async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname === "/api/events")
      return route.fulfill({
        contentType: "text/event-stream",
        body: "event: ready\ndata: {}\n\n",
      });
    if (url.pathname.replace(/\/$/, "") === "/api/rpc") {
      const rpc = JSON.parse(route.request().postData().trim());
      const body = rpc.payload;
      reads.push(rpc.tag);
      let value;
      switch (rpc.tag) {
        case "ListContexts":
          value = data.contexts;
          break;
        case "InspectRuntime":
          value = data.runtime;
          break;
        case "ListApprovals":
          value = data.contexts.find((c) => c.path === "/approvals")?.state.entries ?? [];
          break;
        case "GetGoalHistory": {
          historyRequests.push(body);
          const messages =
            data.contexts.find((c) => c.path === `/goals/${body.slug}`)?.messages ?? [];
          const before = body.before ?? messages.length + 1;
          const entries = messages
            .map((message, i) => ({ seq: i + 1, at, message }))
            .filter((e) => e.seq < before)
            .slice(-30);
          value = {
            entries,
            total: messages.length,
            nextBefore: entries[0]?.seq > 1 ? entries[0].seq : null,
          };
          break;
        }
        case "RespondToApproval":
          writes.push({ tag: rpc.tag, body });
          data.contexts
            .find((c) => c.path === "/approvals")
            .state.entries.find((e) => e.id === body.id).status = "acknowledged";
          break;
        case "SendGoalMessage": {
          writes.push({ tag: rpc.tag, body });
          const goal = data.contexts.find((c) => c.path === `/goals/${body.slug}`);
          goal.messages.push({ role: "user", content: body.text, timestamp: Date.parse(at) });
          goal.state.historyCount = goal.messages.length;
          break;
        }
        case "EndGoal":
          writes.push({ tag: rpc.tag, body });
          data.contexts.find((c) => c.path === `/goals/${body.slug}`).state.status = "completed";
          break;
        default:
          throw new Error(`Unhandled test RPC: ${rpc.tag}`);
      }
      return route.fulfill({
        contentType: "application/ndjson",
        body:
          JSON.stringify({
            _tag: "Exit",
            requestId: rpc.id,
            exit: { _tag: "Success", value: value ?? null },
          }) + "\n",
      });
    }
    return route.fulfill({ status: 404, json: { error: "Not found" } });
  });
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Overview", exact: true })).toBeVisible();
  await expect(page.getByText("Live", { exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "Refresh data" })).toBeEnabled();
  return { errors, writes, reads, historyRequests };
}
test("overview, actor inspection, execution links and approval responses", async ({ page }) => {
  const { errors, writes } = await setup(page);
  await expect(page.getByText("10 instances")).toBeVisible();
  await page.screenshot({
    path: "/tmp/aster-dashboard-desktop.png",
    fullPage: true,
  });
  await page
    .getByRole("button", {
      name: "View /user/signals/progress/run-1",
      exact: true,
    })
    .click();
  await expect(page.getByRole("dialog")).toBeVisible();
  await expect(page.getByText("Current stage", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "/lark/im/chats/chat-1", exact: true }).click();
  await expect(
    page
      .getByRole("dialog")
      .getByText("Core workflow integration is complete; validation starts this week."),
  ).toBeVisible();
  await page.keyboard.press("Escape");
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /Approvals/ })
    .click();
  await page.getByRole("button", { name: "Approve execution" }).click();
  await expect(
    page.getByText("The task has received your decision. View its records for execution results."),
  ).toBeVisible();
  await page.getByLabel("Which environment should receive the release?").fill("Test");
  await page.getByRole("button", { name: "Submit response" }).click();
  expect(writes[1].body.response.answers).toEqual({ scope: ["Test"] });
  await page.getByRole("navigation").getByRole("button", { name: "Goals", exact: true }).click();
  await page.getByRole("button", { name: "View /user/goals/engine" }).click();
  await page.getByLabel("Add information to Goal").fill("Prioritize frontend validation");
  await page.getByRole("button", { name: "Send", exact: true }).click();
  await expect(page.getByText("Prioritize frontend validation", { exact: true })).toBeVisible();
  await page.keyboard.press("Escape");
  await page.getByRole("navigation").getByRole("button", { name: "Actors", exact: true }).click();
  await page.getByLabel("Search Actors").fill("nonexistent-path");
  await expect(page.getByText("No matching results")).toBeVisible();
  expect(errors).toEqual([]);
});
test("mobile and empty state remain usable", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const { errors } = await setup(page);
  await page.screenshot({
    path: "/tmp/aster-dashboard-mobile.png",
    fullPage: true,
  });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(
    true,
  );
  await page.getByRole("navigation").getByRole("button", { name: "Actors", exact: true }).click();
  await page.getByRole("tab", { name: "Persisted" }).click();
  await expect(page.getByText("Historical execution record", { exact: true })).toBeVisible();
  expect(errors).toEqual([]);
});
test("empty runtime and API failures are explicit", async ({ page }) => {
  const { errors } = await setup(page, {
    at,
    contexts: [],
    runtime: { phase: "ready", actors: [], events: [] },
  });
  await expect(page.getByText("No running Actors")).toBeVisible();
  await page.route("**/api/rpc{,/}", async (route) => {
    const rpc = JSON.parse(route.request().postData().trim());
    if (rpc.tag !== "ListContexts") return route.fallback();
    return route.fulfill({
      contentType: "application/ndjson",
      body:
        JSON.stringify({
          _tag: "Exit",
          requestId: rpc.id,
          exit: {
            _tag: "Failure",
            cause: [
              {
                _tag: "Fail",
                error: {
                  _tag: "ApplicationError",
                  kind: "unavailable",
                  message: "Test connection failed",
                },
              },
            ],
          },
        }) + "\n",
    });
  });
  await page.getByRole("button", { name: "Refresh data" }).click();
  await expect(page.getByRole("alert")).toContainText("Test connection failed");
  expect(errors).toEqual([]);
});

test("built dashboard reads real HTTP runtime and refreshes public Context changes", async ({
  page,
}) => {
  const { createRequire } = await import("node:module");
  const { fileURLToPath } = await import("node:url");
  const requireLocal = createRequire(new URL("../../local/package.json", import.meta.url));
  const { Effect, Layer, Schema } = await import(requireLocal.resolve("effect"));
  const { makeApplicationApi, makeContextRegistry, GoalActor, makeMemoryGoalHistory } =
    await import("../../../packages/core/dist/index.js");
  const { ActorSystem, Actor } = await import("../../../packages/actor/dist/index.js");
  const { startGoalApi } = await import("../../local/dist/http-api.js");
  const registry = await Effect.runPromise(makeContextRegistry());
  await Effect.runPromise(registry.register("/goals/real-http", GoalActor.context));
  const record = {
    path: "/goals/real-http",
    description: "Live HTTP validation Goal",
    state: {
      slug: "real-http",
      description: "Live HTTP validation Goal",
      status: "active",
      progress: "",
      summary: "",
      tasks: [],
      historyThrough: 0,
      historyCount: 0,
      pendingEvaluation: false,
      receivedEvents: [],
    },
    messages: [],
  };
  await Effect.runPromise(registry.set(record));
  const { Scope, Exit } = await import(requireLocal.resolve("effect"));
  const scope = await Effect.runPromise(Scope.make());
  const system = await Effect.runPromise(
    ActorSystem.make().pipe(Effect.provideService(Scope.Scope, scope)),
  );
  class Preview extends Actor.Service()("dashboard/Preview", {
    command: Schema.String,
  }) {}
  Preview.layer = Layer.succeed(Preview, Preview.of({ receive: () => Effect.void }));
  const ref = await Effect.runPromise(
    system.spawn("preview", Preview, {
      metadata: { contextPath: record.path },
    }),
  );
  await Effect.runPromise(ref.tell("inspect"));
  const errors = [];
  page.on("pageerror", (e) => errors.push(e.message));
  page.on("console", (message) => {
    if (message.type() === "error" || message.type() === "warning") errors.push(message.text());
  });
  const history = makeMemoryGoalHistory();
  const api = await startGoalApi({
    application: makeApplicationApi({
      registry,
      history,
      inspect: system.inspect({ metadata: ["contextPath"] }).pipe(
        Effect.map((actors) => ({
          actors: actors.map(({ metadata, ...actor }) => ({
            ...actor,
            contextPath: metadata.contextPath,
          })),
          events: [],
          phase: "ready",
        })),
      ),
    }),
    port: 0,
    webDir: fileURLToPath(new URL("../dist", import.meta.url)),
  });
  try {
    await page.goto(api.url);
    await expect(page).toHaveTitle("Aster · Actor Dashboard");
    await expect(page.getByText("Live", { exact: true })).toBeVisible();
    await page.getByRole("navigation").getByRole("button", { name: "Goals", exact: true }).click();
    await page.getByRole("button", { name: "View /user/preview" }).click();
    await expect(
      page.getByRole("dialog").getByText("No messages yet", { exact: true }),
    ).toBeVisible();
    await Effect.runPromise(
      history.append("real-http", {
        role: "assistant",
        content: [{ type: "text", text: "Live HTTP and SSE updates received" }],
        timestamp: Date.now(),
      }),
    );
    const updated = {
      ...record,
      state: { ...record.state, historyCount: 1 },
      messages: [
        {
          type: "assistant",
          text: "Live HTTP and SSE updates received",
          references: [],
          at,
        },
      ],
    };
    await Effect.runPromise(registry.set(updated));
    await expect(page.getByText("Live HTTP and SSE updates received")).toBeVisible();
    expect(errors).toEqual([]);
  } finally {
    try {
      if (!page.isClosed()) await page.goto("about:blank");
    } finally {
      try {
        await api.close();
      } finally {
        await Effect.runPromise(Scope.close(scope, Exit.void));
      }
    }
  }
});

test("empty approvals explain absent runs and expose Goal planning failure", async ({ page }) => {
  const data = fixture();
  data.contexts = data.contexts.filter((c) => !c.path.includes("/runs/"));
  data.contexts.find((c) => c.path === "/approvals").state.entries = [];
  data.contexts.find((c) => c.path === "/goals/engine").messages = [
    { type: "error", text: "Goal Agent returned no plan", at, references: [] },
  ];
  await setup(page, data);
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /^Approvals/ })
    .click();
  await expect(page.getByText("No approval requests yet", { exact: true })).toBeVisible();
  await expect(page.getByText("No Signal occurrences yet", { exact: false })).toBeVisible();
  await expect(page.getByText("Goal Agent returned no plan", { exact: true })).toBeVisible();
  await page.screenshot({ path: "/tmp/aster-approvals-diagnostic.png", fullPage: true });
  await page.getByRole("button", { name: "View Goal failure records" }).click();
  await expect(page.getByRole("dialog")).toContainText("Goal Agent returned no plan");
});

test("Goal feed loads older native messages and displays flat tasks", async ({ page }) => {
  const data = fixture();
  const goal = data.contexts.find((c) => c.path === "/goals/engine");
  goal.state.summary = "Key conclusions saved";
  goal.state.historyCount = 65;
  goal.state.tasks = [
    {
      id: "analysis",
      title: "Analyze compatibility",
      instructions: "Check existing evidence",
      status: "open",
      revision: 1,
    },
  ];
  goal.messages = Array.from({ length: 65 }, (_, i) => ({
    role: "user",
    content: `Historical observation ${i + 1}`,
    timestamp: Date.parse(at),
  }));
  // Both persisted legacy events and new English events keep their progress presentation.
  goal.messages[63].content = "[运行时事件，仅作为证据]\nHistorical observation 64";
  goal.messages[64].content = "[Runtime event, evidence only]\nHistorical observation 65";
  const { errors, historyRequests } = await setup(page, data);
  await page.getByRole("navigation").getByRole("button", { name: "Goals", exact: true }).click();
  await page.getByRole("button", { name: "View /user/goals/engine" }).click();
  const dialog = page.getByRole("dialog");
  await expect(dialog.getByText("Key conclusions saved", { exact: true })).toBeVisible();
  await expect(dialog.getByText("Historical observation 65", { exact: true })).toBeVisible();
  await expect(dialog.getByText("Historical observation 64", { exact: true })).toBeVisible();
  await expect(dialog.getByText("Progress", { exact: true })).toHaveCount(2);
  await dialog.getByRole("button", { name: "Load earlier records" }).click();
  await expect(dialog.getByText("Historical observation 6", { exact: true })).toBeVisible();
  goal.messages.push(
    ...Array.from({ length: 70 }, (_, i) => ({
      role: "user",
      content: `New observation ${i + 1}`,
      timestamp: Date.parse(at),
    })),
  );
  await page.evaluate(() =>
    window.testEvents.emit("invalidate", {
      _tag: "Invalidate",
      keys: ["goal-history:engine"],
    }),
  );
  await expect(dialog.getByText("New observation 70", { exact: true })).toBeVisible();
  await expect(dialog.getByText("Historical observation 6", { exact: true })).toBeVisible();
  await expect(dialog.locator("article.message")).toHaveCount(130);
  goal.messages.push(
    ...Array.from({ length: 46 }, (_, i) => ({
      role: "user",
      content: `Later observation ${i + 1}`,
      timestamp: Date.parse(at),
    })),
  );
  await page.evaluate(() =>
    window.testEvents.emit("invalidate", {
      _tag: "Invalidate",
      keys: ["goal-history:engine"],
    }),
  );
  await expect(dialog.getByText("Later observation 46", { exact: true })).toBeVisible();
  await expect(dialog.locator("article.message")).toHaveCount(176);
  await dialog.getByRole("tab", { name: "Tasks · 1" }).click();
  await expect(dialog.getByText("Analyze compatibility", { exact: true })).toBeVisible();
  expect(historyRequests.map((request) => request.before)).toEqual([
    undefined,
    36,
    undefined,
    106,
    76,
    undefined,
    152,
  ]);
  expect(errors).toEqual([]);
});

test("SSE keys isolate queries, reconnect refreshes all, and invalidation cancels stale reads", async ({
  page,
}) => {
  const data = fixture();
  const { reads, errors } = await setup(page, data);
  const contextsBefore = reads.filter((tag) => tag === "ListContexts").length;
  const runtimeBefore = reads.filter((tag) => tag === "InspectRuntime").length;
  // The independent telemetry tick must not refetch durable Contexts.
  await expect
    .poll(() => reads.filter((tag) => tag === "InspectRuntime").length)
    .toBeGreaterThan(runtimeBefore);
  expect(reads.filter((tag) => tag === "ListContexts").length).toBe(contextsBefore);
  const approvalsBefore = reads.filter((tag) => tag === "ListApprovals").length;
  await page.evaluate(() =>
    window.testEvents.emit("invalidate", { _tag: "Invalidate", keys: ["approvals"] }),
  );
  await expect
    .poll(() => reads.filter((tag) => tag === "ListApprovals").length)
    .toBeGreaterThan(approvalsBefore);
  expect(reads.filter((tag) => tag === "ListContexts").length).toBe(contextsBefore);

  let delayed;
  await page.route("**/api/rpc{,/}", async (route) => {
    const rpc = JSON.parse(route.request().postData().trim());
    if (rpc.tag !== "ListContexts" || delayed) return route.fallback();
    delayed = { route, rpc, value: structuredClone(data.contexts) };
  });
  await page.evaluate(() =>
    window.testEvents.emit("invalidate", { _tag: "Invalidate", keys: ["contexts"] }),
  );
  await expect.poll(() => Boolean(delayed)).toBe(true);
  data.contexts.find((c) => c.path === "/memory").description = "Latest committed memory";
  await page.evaluate(() =>
    window.testEvents.emit("invalidate", { _tag: "Invalidate", keys: ["contexts"] }),
  );
  await expect(page.getByText("Latest committed memory", { exact: true })).toBeVisible();
  await delayed.route.fulfill({
    contentType: "application/ndjson",
    body:
      JSON.stringify({
        _tag: "Exit",
        requestId: delayed.rpc.id,
        exit: { _tag: "Success", value: delayed.value },
      }) + "\n",
  });
  await expect(page.getByText("Latest committed memory", { exact: true })).toBeVisible();

  const beforeReconnect = [...reads];
  await page.evaluate(() => window.testEvents.emit("ready"));
  for (const tag of ["ListContexts", "ListApprovals", "InspectRuntime"]) {
    await expect
      .poll(() => reads.filter((t) => t === tag).length)
      .toBeGreaterThan(beforeReconnect.filter((t) => t === tag).length);
  }
  expect(await page.evaluate(() => window.testEvents.opened)).toBe(1);
  expect(errors).toEqual([]);
});

test("malformed SSE releases its connection and Refresh reconnects", async ({ page }) => {
  const { errors } = await setup(page);
  await page.evaluate(() => window.testEvents.emit("invalidate", { keys: 42 }));
  await expect(page.getByRole("alert")).toContainText("Invalid live update");
  await expect.poll(() => page.evaluate(() => window.testEvents.closed)).toBe(1);
  await page.getByRole("button", { name: "Refresh data" }).click();
  await expect(page.getByText("Live", { exact: true })).toBeVisible();
  expect(await page.evaluate(() => window.testEvents.opened)).toBe(2);
  expect(errors).toEqual([]);
});

test("rejected approval preserves pending state and is not resubmitted", async ({ page }) => {
  const { reads, errors } = await setup(page);
  let attempts = 0;
  await page.route("**/api/rpc{,/}", async (route) => {
    const rpc = JSON.parse(route.request().postData().trim());
    if (rpc.tag !== "RespondToApproval") return route.fallback();
    attempts++;
    return route.fulfill({
      contentType: "application/ndjson",
      body:
        JSON.stringify({
          _tag: "Exit",
          requestId: rpc.id,
          exit: {
            _tag: "Failure",
            cause: [
              {
                _tag: "Fail",
                error: {
                  _tag: "ApplicationError",
                  kind: "conflict",
                  message: "Approval no longer accepts this response",
                },
              },
            ],
          },
        }) + "\n",
    });
  });
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /^Approvals/ })
    .click();
  const before = reads.filter((tag) => tag === "ListApprovals").length;
  await page.getByRole("button", { name: "Approve execution", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("Approval no longer accepts this response");
  await expect(page.getByRole("button", { name: "Approve execution", exact: true })).toBeEnabled();
  expect(attempts).toBe(1);
  expect(reads.filter((tag) => tag === "ListApprovals").length).toBe(before);
  expect(errors).toEqual([]);
});

test("history invalidation during an older-page request retains both ends without refetching cached pages", async ({
  page,
}) => {
  const data = fixture();
  const goal = data.contexts.find((c) => c.path === "/goals/engine");
  goal.messages = Array.from({ length: 65 }, (_, i) => ({
    role: "user",
    content: `Entry ${i + 1}`,
    timestamp: Date.parse(at),
  }));
  const { errors } = await setup(page, data);
  await page.getByRole("navigation").getByRole("button", { name: "Goals", exact: true }).click();
  await page.getByRole("button", { name: "View /user/goals/engine" }).click();
  const dialog = page.getByRole("dialog");
  await expect(dialog.getByText("Entry 65", { exact: true })).toBeVisible();
  let delayed;
  await page.route("**/api/rpc{,/}", async (route) => {
    const rpc = JSON.parse(route.request().postData().trim());
    if (rpc.tag !== "GetGoalHistory" || rpc.payload.before !== 36 || delayed)
      return route.fallback();
    delayed = { route, rpc };
  });
  await dialog.getByRole("button", { name: "Load earlier records" }).click();
  await expect.poll(() => Boolean(delayed)).toBe(true);
  goal.messages.push(
    { role: "user", content: "Entry 66", timestamp: Date.parse(at) },
    { role: "user", content: "Entry 67", timestamp: Date.parse(at) },
  );
  await page.evaluate(() =>
    window.testEvents.emit("invalidate", { _tag: "Invalidate", keys: ["goal-history:engine"] }),
  );
  await expect(dialog.getByText("Entry 67", { exact: true })).toBeVisible();
  await expect(dialog.getByText("Entry 6", { exact: true })).toBeVisible();
  await expect(dialog.locator("article.message")).toHaveCount(62);
  await delayed.route.fulfill({
    contentType: "application/ndjson",
    body:
      JSON.stringify({
        _tag: "Exit",
        requestId: delayed.rpc.id,
        exit: {
          _tag: "Success",
          value: {
            entries: goal.messages.slice(5, 35).map((message, i) => ({ seq: i + 6, at, message })),
            total: 65,
            nextBefore: 6,
          },
        },
      }) + "\n",
  });
  await expect(dialog.getByText("Entry 67", { exact: true })).toBeVisible();
  expect(errors).toEqual([]);
});

test("invalid dashboard fields report a projection error while preserving raw Context inspection", async ({
  page,
}) => {
  const data = fixture();
  data.contexts.find((c) => c.path === "/goals/engine").state = {
    status: "active",
    tasks: "invalid-task-list",
    customEvidence: { source: "kept verbatim" },
  };
  const { errors } = await setup(page, data);
  await expect(page.getByRole("alert")).toContainText(
    "Unsupported dashboard fields in /goals/engine",
  );
  await page.getByRole("button", { name: "View /user/goals/engine" }).click();
  await page.getByRole("dialog").getByRole("tab", { name: "State", exact: true }).click();
  await expect(page.getByRole("dialog").locator("pre")).toContainText("invalid-task-list");
  await expect(page.getByRole("dialog").locator("pre")).toContainText("kept verbatim");
  expect(errors).toEqual([]);
});
