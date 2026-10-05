import { test, expect } from "@playwright/test";
import { at, fixture, designFixture } from "./fixtures.js";
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
        case "InspectDelegation": {
          const record = data.contexts.find((item) => item.path === body.path);
          const state = record.state;
          value = {
            path: record.path,
            revision: record.revision,
            runPath: state.request.runPath,
            agent: state.request.agent,
            status: state.status,
            instructions: state.request.task.instructions,
            sources: state.request.task.input.flatMap((item) => item.sources),
            hasExecution: !!state.session,
            ...(state.result ? { result: state.result } : {}),
            ...(state.error ? { error: state.error } : {}),
            requests: Object.entries(state.requests).map(([id, request]) => ({
              id,
              kind: request.kind,
              prompt: request.prompt,
              responseStatus: state.responses[id]?.status ?? "pending",
            })),
          };
          break;
        }
        case "InspectProcessing": {
          value = data.processing?.[body.owner] ?? { owner: body.owner, revision: 0, entries: [] };
          break;
        }
        case "GetGoalTimeline": {
          const timeline = data.timelines?.[body.slug] ?? { groups: [] };
          const groups = timeline.groups
            .filter((group) => group.ordinal < (body.before ?? Infinity))
            .slice(-30);
          value = {
            ...timeline,
            groups,
            total: timeline.groups.length,
            nextBefore: groups[0]?.ordinal > 1 ? groups[0].ordinal : null,
          };
          break;
        }
        case "GetGoalHistory": {
          historyRequests.push(body);
          const messages =
            data.contexts.find((c) => c.path === `/goals/${body.slug}`)?.messages ?? [];
          const before = body.before ?? messages.length + 1;
          const entries = messages
            .map((message, i) => ({ seq: i + 1, at: message.at ?? at, message }))
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
          data.timelines ??= {};
          const timeline = (data.timelines[body.slug] ??= { groups: [] });
          if (!timeline.groups.some((group) => group.requestId === body.requestId))
            timeline.groups.push({
              requestId: body.requestId,
              ordinal: timeline.groups.length + 1,
              status: "pending",
              input: {
                inputId: body.requestId,
                goalSlug: body.slug,
                ordinal: timeline.groups.length + 1,
                receivedAt: at,
                payload: { _tag: "UserInput", text: body.text },
              },
            });
          break;
        }
        case "ResumeRun": {
          writes.push({ tag: rpc.tag, body });
          const run = data.contexts.find((item) => item.path === body.target);
          run.state.resumptions = [
            {
              input: body,
              receipt: { requestId: body.requestId, revision: run.revision + 1 },
              status: "delivered",
            },
          ];
          run.revision++;
          value = { requestId: body.requestId, revision: run.revision };
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
  const firstGoal =
    data.contexts.find((context) => context.path === "/goals/personal") ??
    data.contexts.find((context) => /^\/goals\/[^/]+$/.test(context.path)) ??
    data.contexts[0];
  await expect(
    page.getByRole("heading", {
      name:
        firstGoal?.path === "/goals/personal"
          ? "Personal assistant"
          : firstGoal?.state.title || firstGoal?.description || "No Contexts yet",
      exact: true,
    }),
  ).toBeVisible();
  await expect(page.getByText("Live", { exact: true })).toHaveCount(1);
  await expect(
    page.getByRole("button", { name: "Refresh data", includeHidden: true }),
  ).toBeEnabled();
  return { errors, writes, reads, historyRequests };
}
test("Goals workspace links executions, accepts notes, and handles approvals", async ({ page }) => {
  const { errors, writes } = await setup(page);
  await expect(page.getByRole("heading", { name: "Goal Timeline" })).toBeVisible();
  for (const name of ["Home", "Search", "Library", "Settings", "Overview", "Actors"]) {
    await expect(
      page.getByRole("navigation").getByRole("button", { name, exact: true }),
    ).toHaveCount(0);
  }
  await page.getByRole("button", { name: /^View execution:/ }).click();
  await expect(page.getByRole("dialog")).toContainText("Current stage");
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "/lark/im/chats/chat-1", exact: true })
    .click();
  await expect(page.getByRole("dialog")).toContainText("Core workflow integration is complete");
  await page.keyboard.press("Escape");
  await page.getByRole("button", { name: "Approve execution", exact: true }).click();
  await expect(
    page.getByText("The task has received your decision. View its records for execution results."),
  ).toBeVisible();
  await page.getByLabel("Which environment should receive the release?").fill("Test");
  await page.getByRole("button", { name: "Submit response" }).click();
  expect(writes[1].body.response.answers).toEqual({ scope: ["Test"] });
  await page.getByLabel("Add information to Goal").fill("Prioritize frontend validation");
  await page.getByRole("button", { name: "Send", exact: true }).click();
  await expect(page.getByText("Prioritize frontend validation", { exact: true })).toBeVisible();
  await page.getByRole("tab", { name: "Notes", exact: true }).click();
  await expect(page.getByText("Prioritize frontend validation", { exact: true })).toBeVisible();
  await expect(
    page.getByText("Monitoring project progress. Next, track integration and validation results.", {
      exact: true,
    }),
  ).toHaveCount(0);
  await page.getByRole("tab", { name: "Related", exact: true }).click();
  await expect(
    page
      .getByRole("tabpanel", { name: "Related", exact: true })
      .getByRole("button", { name: /Significant change in project progress/ }),
  ).toBeVisible();
  await page.getByRole("tab", { name: "Details", exact: true }).click();
  await expect(
    page
      .getByRole("tabpanel", { name: "Details", exact: true })
      .getByText("Awaiting test feedback", { exact: true }),
  ).toBeVisible();
  expect(errors).toEqual([]);
});

test("Goal title appears in navigation and inspector while Details keeps the description", async ({
  page,
}) => {
  const data = fixture();
  const goal = data.contexts.find((context) => context.path === "/goals/engine");
  goal.state.title = "Knowledge Engine";
  const { errors } = await setup(page, data);
  await expect(page.locator(".breadcrumb-title")).toHaveText("Knowledge Engine");
  await expect(
    page.getByRole("navigation").getByText("Knowledge Engine", { exact: true }),
  ).toHaveText("Knowledge Engine");
  await page.getByRole("tab", { name: "Details", exact: true }).click();
  await expect(
    page.getByRole("tabpanel").getByText(goal.description, { exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Inspect stored state" }).click();
  await expect(
    page.getByRole("dialog").getByRole("heading", { name: "Knowledge Engine" }),
  ).toBeVisible();
  expect(errors).toEqual([]);
});

test("Goal summaries and responses render Markdown", async ({ page }) => {
  const data = fixture();
  const markdown = [
    "## Travel options",
    "",
    "**Family trip** with a [reference](https://example.com/travel).",
    "",
    "- Compare destinations",
    "- Check `flight` prices",
    "",
    "| Destination | Fare |",
    "| --- | --- |",
    "| Xiamen | 1300 |",
    "| Sanya | 3920 |",
  ].join("\n");
  const goal = data.contexts.find((context) => context.path === "/goals/engine");
  goal.state.summary = markdown;
  goal.state.progress = markdown;
  data.timelines.engine.groups[0].response = markdown;
  const { errors } = await setup(page, data);
  await page.goto("/?context=%2Fgoals%2Fengine");

  for (const selector of [".goal-description", ".conversation-response"]) {
    const content = page.locator(selector);
    await expect(content.getByRole("heading", { name: "Travel options" })).toBeVisible();
    await expect(content.locator("strong").filter({ hasText: "Family trip" })).toBeVisible();
    await expect(content.getByRole("link", { name: "reference" })).toHaveAttribute(
      "href",
      "https://example.com/travel",
    );
    await expect(content.getByRole("listitem")).toHaveCount(2);
    await expect(content.locator("code")).toHaveText("flight");
    await expect(content.getByRole("table").getByRole("row")).toHaveCount(3);
  }

  await page.getByRole("tab", { name: "Details", exact: true }).click();
  await expect(page.getByRole("tabpanel", { name: "Details" }).getByRole("table")).toBeVisible();
  expect(errors).toEqual([]);
});

test("Goal timeline omits internal Agent tool exchanges", async ({ page }) => {
  const data = fixture();
  const goal = data.contexts.find((context) => context.path === "/goals/engine");
  goal.messages.push(
    {
      role: "assistant",
      content: [
        { type: "text", text: "I will inspect the latest contexts." },
        { type: "toolCall", id: "tool-1", name: "search_contexts", arguments: { query: "lark" } },
      ],
      timestamp: Date.parse(at),
    },
    {
      role: "toolResult",
      toolCallId: "tool-1",
      toolName: "search_contexts",
      content: [{ type: "text", text: '{"items":[{"path":"/lark/im/chats/unrelated"}]}' }],
      timestamp: Date.parse(at),
    },
  );
  const { errors } = await setup(page, data);
  await expect(page.locator(".goal-timeline .timeline-event")).toHaveCount(1);
  await expect(page.getByText("search_contexts", { exact: true })).toHaveCount(0);
  expect(errors).toEqual([]);
});

test("mobile keeps navigation, composer, and Goal work accessible without overflow", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const data = fixture();
  data.contexts.push({
    path: "/goals/second",
    description: "Another active goal",
    state: { status: "active" },
    messages: [],
  });
  const { errors } = await setup(page, data);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(
    true,
  );
  await page.getByRole("button", { name: "Choose goal" }).click();
  await page
    .getByRole("navigation", { name: "Contexts" })
    .getByRole("button", { name: /Another active goal/ })
    .click();
  await expect(
    page.getByRole("heading", { name: "Another active goal", exact: true }),
  ).toBeVisible();
  await expect(page.getByRole("heading", { name: "No conversation yet" })).toBeVisible();
  await page.getByRole("link", { name: "View tasks and signals" }).click();
  await expect(page.getByRole("complementary", { name: "Goal work" })).toBeVisible();
  await page.screenshot({ path: "/tmp/aster-goals-mobile.png", fullPage: true });
  expect(errors).toEqual([]);
});

test("empty runtime and API failures are explicit", async ({ page }) => {
  const { errors } = await setup(page, {
    at,
    contexts: [],
    runtime: { phase: "ready", actors: [], events: [] },
  });
  await expect(page.getByText("No Contexts yet", { exact: true })).toBeVisible();
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
  const { makeApplicationApi, GoalActor, makeMemoryGoalHistory } =
    await import("../../../packages/core/dist/index.js");
  const { ActorSystem, Actor } = await import("../../../packages/actor/dist/index.js");
  const { startGoalApi } = await import("../../local/dist/http-api.js");
  const { makeContextRegistry } = await import("../../../packages/core/dist/testing/context.js");
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
      inputs: [],
      historyThrough: 0,
      historyCount: 0,
    },
    messages: [],
  };
  await Effect.runPromise(registry.commit(record, { expectedRevision: 0 }));
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
    await expect(page).toHaveTitle("Aster · Workspace");
    await expect(page.getByText("Live", { exact: true })).toBeVisible();
    await page.getByRole("button", { name: "Goal actions" }).click();
    await page.getByRole("menuitem", { name: "Inspect goal" }).click();
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
    await Effect.runPromise(
      registry.commit(updated, { expectedRevision: registry.get(updated.path)?.revision ?? 0 }),
    );
    await expect(
      page.getByRole("dialog").getByText("Live HTTP and SSE updates received"),
    ).toBeVisible();
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

test("Goal errors and absent runs remain explicit", async ({ page }) => {
  const data = fixture();
  data.contexts = data.contexts.filter((context) => !context.path.includes("/runs/"));
  data.contexts.find((context) => context.path === "/approvals").state.entries = [];
  data.contexts.find((context) => context.path === "/goals/engine").messages = [
    { type: "error", text: "Goal conversation failed", at, references: [] },
  ];
  Object.assign(data.timelines.engine.groups[0], {
    status: "failed",
    error: "Goal conversation failed",
    response: undefined,
  });
  await setup(page, data);
  await expect(page.getByText("Goal conversation failed", { exact: true })).toBeVisible();
  await expect(page.getByText("No tasks yet. Planned work will appear here.")).toBeVisible();
});

test("Goal input history loads older messages and displays independent Tasks", async ({ page }) => {
  const data = fixture();
  const goal = data.contexts.find((c) => c.path === "/goals/engine");
  goal.state.summary = "Key conclusions saved";
  goal.state.historyCount = 65;
  data.contexts.push({
    path: "/runs/goal--analysis",
    description: "Analyze compatibility",
    state: {
      sourcePath: goal.path,
      status: "running",
      task: { instructions: "Check existing evidence", input: [] },
    },
    messages: [],
  });
  goal.messages = Array.from({ length: 65 }, (_, i) => ({
    role: "user",
    content: `Historical observation ${i + 1}`,
    timestamp: Date.parse(at),
  }));
  // Both persisted legacy events and new English events keep their progress presentation.
  goal.messages[63].content = "[运行时事件，仅作为证据]\nHistorical observation 64";
  goal.messages[64].content = "[Runtime event, evidence only]\nHistorical observation 65";
  const { errors, historyRequests } = await setup(page, data);
  await page.getByRole("button", { name: "Goal actions" }).click();
  await page.getByRole("menuitem", { name: "Inspect goal" }).click();
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
  data.contexts.find((c) => c.path === "/goals/engine").state.progress =
    "Latest committed progress";
  await page.getByRole("tab", { name: "Details", exact: true }).click();
  await page.evaluate(() =>
    window.testEvents.emit("invalidate", { _tag: "Invalidate", keys: ["contexts"] }),
  );
  await expect(
    page
      .getByRole("tabpanel", { name: "Details", exact: true })
      .getByText("Latest committed progress", { exact: true }),
  ).toBeVisible();
  await delayed.route.fulfill({
    contentType: "application/ndjson",
    body:
      JSON.stringify({
        _tag: "Exit",
        requestId: delayed.rpc.id,
        exit: { _tag: "Success", value: delayed.value },
      }) + "\n",
  });
  await expect(
    page
      .getByRole("tabpanel", { name: "Details", exact: true })
      .getByText("Latest committed progress", { exact: true }),
  ).toBeVisible();

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
  const before = reads.filter((tag) => tag === "ListApprovals").length;
  await page.getByRole("button", { name: "Approve execution", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("Approval no longer accepts this response");
  await expect(page.getByRole("button", { name: "Approve execution", exact: true })).toBeEnabled();
  expect(attempts).toBe(1);
  expect(reads.filter((tag) => tag === "ListApprovals").length).toBe(before);
  expect(errors).toEqual([]);
});

test("uncertain approval keeps its exact decision across navigation and reconnect", async ({
  page,
}) => {
  const { writes, errors } = await setup(page);
  const attempts = [];
  await page.route("**/api/rpc{,/}", async (route) => {
    const rpc = JSON.parse(route.request().postData().trim());
    if (rpc.tag !== "RespondToApproval") return route.fallback();
    attempts.push(rpc.payload);
    if (attempts.length > 1) return route.fallback();
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
                  message: "Approval acknowledgement missing",
                },
              },
            ],
          },
        }) + "\n",
    });
  });
  await page.getByRole("button", { name: "Approve execution", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("Approval acknowledgement missing");
  await expect(page.getByRole("button", { name: "Reject", exact: true })).toHaveCount(0);
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /Summarize Knowledge Engine project progress/ })
    .first()
    .click();
  await page.evaluate(() => window.testEvents.emit("ready"));
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /Monitor Knowledge Engine project progress/ })
    .click();
  await expect(
    page.getByRole("button", { name: "Reconcile saved decision", exact: true }),
  ).toBeVisible();
  expect(attempts).toHaveLength(1);
  await page.getByRole("button", { name: "Reconcile saved decision", exact: true }).click();
  await expect.poll(() => writes.length).toBe(1);
  expect(attempts[1]).toEqual(attempts[0]);
  expect(writes[0]).toEqual({ tag: "RespondToApproval", body: attempts[0] });
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
  await page.getByRole("button", { name: "Goal actions" }).click();
  await page.getByRole("menuitem", { name: "Inspect goal" }).click();
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
    progress: { invalid: "invalid-progress" },
    customEvidence: { source: "kept verbatim" },
  };
  const { errors } = await setup(page, data);
  await expect(page.getByRole("alert")).toContainText(
    "Unsupported dashboard fields in /goals/engine",
  );
  await page.getByRole("button", { name: "Goal actions" }).click();
  await page.getByRole("menuitem", { name: "Inspect goal" }).click();
  await page.getByRole("dialog").getByRole("tab", { name: "State", exact: true }).click();
  await expect(page.getByRole("dialog").locator("pre")).toContainText("invalid-progress");
  await expect(page.getByRole("dialog").locator("pre")).toContainText("kept verbatim");
  expect(errors).toEqual([]);
});

test("reference layout has a fixed composer, scoped work, and working timeline filters", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1487, height: 1058 });
  const { errors } = await setup(page, designFixture());
  await expect(page).toHaveTitle(/Aster/);
  await expect(page).toHaveURL(/127\.0\.0\.1:4329/);
  await expect(page.locator("vite-error-overlay")).toHaveCount(0);
  await expect(page.locator(".goal-timeline .timeline-event")).toHaveCount(3);
  await expect(
    page.getByRole("button", { name: "Google Flights – HND to CTS", exact: true }),
  ).toBeVisible();
  await expect(page.getByText("hashed-source-fingerprint", { exact: true })).toHaveCount(0);
  await page.screenshot({ path: "/tmp/aster-goals-reference-desktop.png" });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(
    true,
  );
  await expect(page.getByRole("button", { name: "Send", exact: true })).toBeInViewport();
  await page.getByLabel("Filter timeline events").selectOption("signals");
  await expect(page.locator(".goal-timeline .timeline-event")).toHaveCount(0);
  await page.getByRole("tab", { name: "Notes", exact: true }).click();
  await expect(page.locator(".goal-timeline .timeline-event")).toHaveCount(2);
  await page.getByRole("tab", { name: "Timeline", exact: true }).click();
  await page.getByLabel("Filter timeline events").selectOption("all");
  await page.getByRole("button", { name: "Google Flights – HND to CTS", exact: true }).click();
  await expect(page.getByRole("dialog")).toContainText("Google Flights – HND to CTS");
  await page.keyboard.press("Escape");
  await page.getByRole("button", { name: "Add context reference" }).click();
  await page.getByRole("menuitem", { name: "Google Flights – HND to CTS", exact: true }).click();
  await expect(page.getByLabel("Add information to Goal")).toHaveValue("/sources/flights");
  expect(errors).toEqual([]);
});

test("Goal completion is confirmed and retains its Context", async ({ page }) => {
  const data = fixture();
  data.contexts.push({
    path: "/goals/other",
    description: "Another goal",
    state: { status: "active" },
    messages: [],
  });
  const { writes, errors } = await setup(page, data);
  await page.getByRole("button", { name: "Goal actions" }).click();
  await page.getByRole("menuitem", { name: "End Goal", exact: true }).click();
  await expect(page.getByRole("alertdialog")).toContainText(
    "Work already submitted may still finish",
  );
  await page.getByRole("button", { name: "Keep Goal active" }).click();
  expect(writes).toEqual([]);
  await page.getByRole("button", { name: "Goal actions" }).click();
  await page.getByRole("menuitem", { name: "End Goal", exact: true }).click();
  await page.getByRole("button", { name: "Confirm ending Goal" }).click();
  await expect(page.getByRole("alertdialog")).toHaveCount(0);
  expect(writes).toEqual([{ tag: "EndGoal", body: { slug: "engine" } }]);
  await expect(page.getByLabel("Add information to Goal")).toHaveCount(0);
  await expect(
    page.getByRole("navigation").getByRole("button", { name: /Monitor Knowledge Engine/ }),
  ).toBeVisible();
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /Another goal/ })
    .click();
  await expect(page.getByLabel("Add information to Goal")).toBeEnabled();
  expect(errors).toEqual([]);
});

test("failed message submission preserves the draft and never retries automatically", async ({
  page,
}) => {
  const { errors } = await setup(page);
  let attempts = 0;
  const requestIds = [];
  await page.route("**/api/rpc{,/}", async (route) => {
    const rpc = JSON.parse(route.request().postData().trim());
    if (rpc.tag !== "SendGoalMessage") return route.fallback();
    attempts++;
    requestIds.push(rpc.payload.requestId);
    await route.fulfill({
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
                  message: "Message not accepted",
                },
              },
            ],
          },
        }) + "\n",
    });
  });
  await page.getByLabel("Add information to Goal").fill("Keep this draft");
  await page.getByRole("button", { name: "Send", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("Message not accepted");
  await expect(page.getByLabel("Add information to Goal")).toHaveValue("Keep this draft");
  expect(attempts).toBe(1);
  await page.getByRole("button", { name: "Send", exact: true }).click();
  await expect.poll(() => attempts).toBe(2);
  expect(requestIds[0]).toBeTruthy();
  expect(requestIds[1]).toBe(requestIds[0]);
  expect(errors).toEqual([]);
});

test("populated responsive layouts retain all Goal work and wrap long content", async ({
  page,
}) => {
  const data = designFixture();
  const goal = data.contexts.find((context) => context.path === "/goals/engine");
  goal.description = "Plan our Hokkaido trip and compare every transport and accommodation option";
  const { errors } = await setup(page, data);
  for (const width of [1280, 1024, 390]) {
    await page.setViewportSize({ width, height: 844 });
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
    ).toBe(true);
    await expect(page.getByLabel("Add information to Goal")).toBeVisible();
    await expect(page.getByRole("complementary", { name: "Goal work" })).toBeVisible();
    await page.screenshot({ path: `/tmp/aster-goals-responsive-${width}.png`, fullPage: true });
    if (width === 390) await page.screenshot({ path: "/tmp/aster-goals-timeline-mobile.png" });
  }
  await page.getByRole("button", { name: "Choose goal" }).click();
  await page.getByRole("button", { name: "Close contexts", exact: true }).click();
  await expect(page.getByRole("navigation", { name: "Contexts" })).toBeHidden();
  expect(errors).toEqual([]);
});

function personalFixture() {
  const data = fixture();
  data.contexts.unshift({
    path: "/goals/personal",
    description: "Personal assistant",
    revision: 4,
    state: {
      slug: "personal",
      title: "Personal assistant",
      status: "active",
      summary: "Ready to help",
      progress: "Ready to help",
      historyCount: 0,
    },
    messages: [],
  });
  data.timelines.personal = { groups: [] };
  return data;
}

test("Context tree navigates existing paths, search, history and missing selections", async ({
  page,
}) => {
  const data = personalFixture();
  const { errors } = await setup(page, data);
  await page.getByLabel("Find context").fill("chat-1");
  const target = page
    .getByRole("navigation")
    .getByRole("button", { name: /\/lark\/im\/chats\/chat-1/ });
  await target.click();
  await expect(page.getByRole("main")).toContainText("Core workflow integration is complete");
  await expect(page).toHaveURL(/context=%2Flark%2Fim%2Fchats%2Fchat-1/);
  await page.goBack();
  await expect(
    page.getByRole("heading", { name: "Personal assistant", exact: true }),
  ).toBeVisible();
  await page.goForward();
  await expect(page.getByRole("main")).toContainText("Core workflow integration is complete");
  await page.reload();
  await expect(page.getByRole("main")).toContainText("Core workflow integration is complete");
  data.contexts = data.contexts.filter((context) => context.path !== "/lark/im/chats/chat-1");
  await page.evaluate(() => window.testEvents.emit("ready"));
  await expect(page.getByRole("heading", { name: "Context unavailable" })).toBeVisible();
  expect(errors).toEqual([]);
});

test("Delegation workspace uses business inspection and refreshes after committed changes", async ({
  page,
}) => {
  const data = personalFixture();
  const record = {
    path: "/delegations/execution",
    revision: 3,
    description: "Release execution",
    state: {
      request: {
        runPath: "/runs/5f02eb8dc61a2610739dc2b134208b5c7ed6a939043ceb6d9de1fe26114eb1a3",
        agent: "pi",
        task: {
          instructions: "Analyze release evidence",
          input: [{ content: "Evidence", sources: ["/lark/im/chats/chat-1"] }],
        },
      },
      status: "uncertain",
      error: "Handle acknowledgement was lost",
      requests: {},
      responses: {},
      metadata: { token: "private-provider-token" },
    },
    messages: [{ role: "system", content: "native-provider-frame" }],
  };
  data.contexts.push(record);
  const { reads, errors } = await setup(page, data);
  await page.getByLabel("Find context").fill("Release execution");
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /Release execution/ })
    .click();
  await expect(
    page.getByText("No execution handle is recorded. The task has not been resubmitted."),
  ).toBeVisible();
  await expect(page.getByText("Analyze release evidence", { exact: true })).toBeVisible();
  await expect(page.getByText("private-provider-token", { exact: false })).toHaveCount(0);
  await expect(page.getByText("native-provider-frame", { exact: false })).toHaveCount(0);
  record.state.status = "completed";
  record.state.session = { sessionId: "existing" };
  record.state.result = "Release review completed";
  delete record.state.error;
  record.revision = 4;
  await page.evaluate(() => window.testEvents.emit("ready"));
  await expect(page.getByText("Release review completed", { exact: true })).toBeVisible();
  expect(reads.filter((tag) => tag === "InspectDelegation").length).toBeGreaterThan(1);
  expect(errors).toEqual([]);
});

test("Run resumption preserves uncertain admission across navigation and reconnect", async ({
  page,
}) => {
  const data = personalFixture();
  const personal = data.contexts.find((item) => item.path === "/goals/personal");
  const run = data.contexts.find(
    (item) =>
      item.path === "/runs/5f02eb8dc61a2610739dc2b134208b5c7ed6a939043ceb6d9de1fe26114eb1a3",
  );
  run.revision = 7;
  run.state.status = "failed";
  run.state.task = { instructions: "Continue original work", input: [] };
  const { writes, errors } = await setup(page, data);
  let frozen;
  await page.route("**/api/rpc{,/}", async (route) => {
    const rpc = JSON.parse(route.request().postData().trim());
    if (rpc.tag !== "ResumeRun" || frozen) return route.fallback();
    frozen = rpc.payload;
    await route.fulfill({
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
                  message: "Resume acknowledgement missing",
                },
              },
            ],
          },
        }) + "\n",
    });
  });
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /Summarize Knowledge Engine project progress/ })
    .first()
    .click();
  await page.getByRole("button", { name: "Resume execution", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("Resume acknowledgement missing");
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /Personal assistant/ })
    .click();
  personal.revision = 10;
  run.revision = 9;
  await page.evaluate(() => window.testEvents.emit("ready"));
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /Summarize Knowledge Engine project progress/ })
    .first()
    .click();
  await page.getByRole("button", { name: "Reconcile resumption", exact: true }).click();
  await expect(page.getByText("Resumption delivered", { exact: true })).toBeVisible();
  expect(writes.at(-1)).toEqual({ tag: "ResumeRun", body: frozen });
  expect(frozen).toMatchObject({ expectedRevision: 7, target: run.path });
  expect(errors).toEqual([]);
});

test("restricted Contexts retain navigation and revision without exposing a raw state panel", async ({
  page,
}) => {
  const data = personalFixture();
  data.contexts.push({
    path: "/archive/private",
    description: "Archived Context",
    revision: 19,
    state: {},
    messages: [],
    projection: { version: 1, visibility: "restricted", reason: "missing-policy" },
  });
  data.contexts.push({
    path: "/goals/restricted",
    description: "Restricted Goal",
    revision: 20,
    state: {},
    messages: [],
    projection: { version: 1, visibility: "restricted", reason: "invalid-data" },
  });
  const { errors } = await setup(page, data);
  await page.getByRole("button", { name: /Archived Context.*archive\/private/ }).click();
  await expect(page.getByText("Revision 19", { exact: true })).toBeVisible();
  await expect(
    page.getByText(
      "This Context exposes its path and revision only. Its contents are not available in the public view.",
    ),
  ).toBeVisible();
  await expect(page.getByText("View Context state", { exact: true })).toHaveCount(0);
  await page.getByRole("button", { name: /Restricted Goal.*goals\/restricted/ }).click();
  await expect(page.getByText("Revision 20", { exact: true })).toBeVisible();
  await expect(
    page.getByText(
      "This Context exposes its path and revision only. Its contents are not available in the public view.",
    ),
  ).toBeVisible();
  await expect(page.getByRole("textbox", { name: /Message|information/ })).toHaveCount(0);
  expect(errors).toEqual([]);
});

test("Timeline refreshes older deliveries after reconnect and displays newly accepted notes", async ({
  page,
}) => {
  const data = fixture();
  const template = data.timelines.engine.groups[0];
  data.timelines.engine.groups = Array.from({ length: 65 }, (_, i) => ({
    ...template,
    requestId: `input-${i + 1}`,
    ordinal: i + 1,
    response: `Response ${i + 1}`,
    input: { ...template.input, inputId: `input-${i + 1}`, ordinal: i + 1 },
  }));
  const older = data.timelines.engine.groups[10];
  older.status = "unknown";
  const { errors } = await setup(page, data);
  await expect(page.locator(".conversation-entry")).toHaveCount(30);
  await page.getByRole("button", { name: "Load earlier messages" }).click();
  await expect(page.locator(".conversation-entry")).toHaveCount(60);
  await expect(page.locator("#input-input-11")).toContainText("unknown");
  older.status = "completed";
  data.timelines.engine.groups.push({
    requestId: "pending-note",
    ordinal: 66,
    status: "pending",
    input: {
      inputId: "pending-note",
      goalSlug: "engine",
      ordinal: 66,
      receivedAt: at,
      payload: { _tag: "UserInput", text: "Please verify the release date" },
    },
  });
  await page.evaluate(() => window.testEvents.emit("ready"));
  await expect(page.locator("#input-input-11")).toContainText("completed");
  await expect(page.locator("#input-pending-note")).toContainText("Please verify the release date");
  await page.getByRole("button", { name: "Load earlier messages" }).click();
  await expect(page.locator(".conversation-entry")).toHaveCount(66);
  await page.getByRole("tab", { name: "Notes", exact: true }).click();
  await expect(page.locator("#input-pending-note")).toBeVisible();
  expect(errors).toEqual([]);
});

test("Run publication shows separate approval and retained unknown outcome after reconnect", async ({
  page,
}) => {
  const data = personalFixture();
  const source = "/runs/cb4e6fd93a8701d7e1020404e479c56377824d8bf23e8426fb3943480e9d7534";
  const operation = {
    request: {
      requestId: "publish-one",
      source,
      taskSource: "/signals/report",
      causationId: "user-one",
      createdAt: at,
      action: { _tag: "PublishResult", channelPath: "/lark/im/chats/oc_test", identity: "user" },
      content: "Release blockers resolved; ready for the launch review.",
      causal: { rootRequestId: "user-one", remainingAgentTurns: 0 },
    },
    status: "waiting-approval",
  };
  const run = {
    path: source,
    description: "Report publication",
    revision: 1,
    state: {
      status: "completed",
      outcomeText: operation.request.content,
      writeback: operation,
      task: {
        instructions: "Summarize launch readiness for the release channel",
        input: [{ content: "Release blockers are resolved.", sources: ["/goals/engine"] }],
      },
    },
    messages: [
      { type: "TaskPrepared", at },
      { type: "Delegating", at },
      { type: "Completed", at },
    ],
  };
  data.contexts.push(run);
  const { errors } = await setup(page, data);
  await page.getByLabel("Find context").fill("Report publication");
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /Report publication/ })
    .click();
  const publication = page.getByRole("article", { name: "External publication" });
  await expect(publication.getByText("Status: waiting-approval")).toBeVisible();
  await expect(publication.getByText("Sending as: user")).toBeVisible();
  await expect(
    publication.getByText("Release blockers resolved; ready for the launch review.", {
      exact: true,
    }),
  ).toBeVisible();
  await publication.getByRole("button", { name: "Review publication approval" }).click();
  await page.getByLabel("Find context").fill("Report publication");
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /Report publication/ })
    .click();
  run.state.writeback = {
    ...operation,
    status: "unknown",
    submittedAt: at,
    authorization: {
      approvalId: `${source}:writeback:publish-one`,
      approvalsRevision: 2,
      approvedAt: at,
    },
    error: "Acknowledgement unavailable",
  };
  run.revision = 2;
  await page.evaluate(() => window.testEvents.emit("ready"));
  await expect(publication.getByText("Status: unknown")).toBeVisible();
  await expect(publication.getByText(/Automatic resend is disabled/)).toBeVisible();
  await expect(page.getByRole("heading", { name: "Outcome", exact: true })).toBeVisible();
  await expect(page.getByText(operation.request.content, { exact: true })).toHaveCount(2);
  await expect(publication.getByRole("button", { name: /retry|resend/i })).toHaveCount(0);
  run.state.writeback = { ...run.state.writeback, status: "published", externalId: "om_receipt" };
  delete run.state.writeback.error;
  run.revision = 3;
  await page.evaluate(() => window.testEvents.emit("ready"));
  await expect(publication.getByText("Receipt: om_receipt")).toBeVisible();
  await page.screenshot({ path: "/tmp/aster-publication-desktop.png", fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(publication.getByText("Receipt: om_receipt")).toBeVisible();
  await page.screenshot({ path: "/tmp/aster-publication-mobile.png", fullPage: true });
  expect(errors).toEqual([]);
});

test("Goal turn retry retains its request identity after an unknown acknowledgement", async ({
  page,
}) => {
  const data = designFixture();
  const group = data.timelines.engine.groups[1];
  group.status = "failed";
  group.error = "Provider failed before producing a result";
  const { errors } = await setup(page, data);
  const requests = [];
  await page.route("**/api/rpc{,/}", async (route) => {
    const rpc = JSON.parse(route.request().postData().trim());
    if (rpc.tag !== "RetryGoalTurn") return route.fallback();
    requests.push(rpc.payload);
    const first = requests.length === 1;
    if (!first) group.status = "running";
    await route.fulfill({
      contentType: "application/ndjson",
      body:
        JSON.stringify({
          _tag: "Exit",
          requestId: rpc.id,
          exit: first
            ? {
                _tag: "Failure",
                cause: [
                  {
                    _tag: "Fail",
                    error: {
                      _tag: "ApplicationError",
                      kind: "unavailable",
                      message: "Turn retry acknowledgement missing",
                    },
                  },
                ],
              }
            : { _tag: "Success", value: { requestId: rpc.payload.requestId, revision: 42 } },
        }) + "\n",
    });
  });
  await page.getByRole("button", { name: "Retry turn", exact: true }).click();
  await expect(page.getByText("Turn retry acknowledgement missing", { exact: true })).toBeVisible();
  await page.getByRole("tab", { name: "Notes", exact: true }).click();
  await page.getByRole("tab", { name: "Timeline", exact: true }).click();
  await page.evaluate(() => window.testEvents.emit("ready"));
  await page.getByRole("button", { name: "Retry turn", exact: true }).click();
  await expect(page.getByRole("button", { name: "Retry turn", exact: true })).toHaveCount(0);
  expect(requests).toHaveLength(2);
  expect(requests[1]).toEqual(requests[0]);
  expect(requests[0].turnId).toBe(group.requestId);
  expect(errors).toEqual([]);
});

test("built-in personal assistant uses the ordinary Goal conversation", async ({ page }) => {
  const data = personalFixture();
  const { writes, errors } = await setup(page, data);
  await page.getByLabel("Add information to Goal").fill("Review my priorities");
  await page.getByRole("button", { name: "Send", exact: true }).click();
  await expect(page.getByText("Review my priorities", { exact: true })).toBeVisible();
  expect(writes.at(-1)).toMatchObject({
    tag: "SendGoalMessage",
    body: { slug: "personal", text: "Review my priorities" },
  });
  expect(errors).toEqual([]);
});
