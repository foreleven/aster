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
  await expect(
    page.getByRole("heading", {
      name:
        data.contexts.find((context) => /^\/goals\/[^/]+$/.test(context.path))?.description ??
        "No Goals yet",
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
    .getByRole("navigation", { name: "Goals" })
    .getByRole("button", { name: /Another active goal/ })
    .click();
  await expect(
    page.getByRole("heading", { name: "Another active goal", exact: true }),
  ).toBeVisible();
  await expect(page.getByRole("heading", { name: "No messages yet" })).toBeVisible();
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
  await expect(page.getByText("No Goals yet", { exact: true })).toBeVisible();
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
    await expect(page).toHaveTitle("Aster · Goals");
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
    await Effect.runPromise(registry.set(updated));
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
    { type: "error", text: "Goal Agent returned no plan", at, references: [] },
  ];
  await setup(page, data);
  await expect(page.getByText("Goal Agent returned no plan", { exact: true })).toBeVisible();
  await expect(page.getByText("No tasks yet. Planned work will appear here.")).toBeVisible();
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
    tasks: "invalid-task-list",
    customEvidence: { source: "kept verbatim" },
  };
  const { errors } = await setup(page, data);
  await expect(page.getByRole("alert")).toContainText(
    "Unsupported dashboard fields in /goals/engine",
  );
  await page.getByRole("button", { name: "Goal actions" }).click();
  await page.getByRole("menuitem", { name: "Inspect goal" }).click();
  await page.getByRole("dialog").getByRole("tab", { name: "State", exact: true }).click();
  await expect(page.getByRole("dialog").locator("pre")).toContainText("invalid-task-list");
  await expect(page.getByRole("dialog").locator("pre")).toContainText("kept verbatim");
  expect(errors).toEqual([]);
});

test("reference layout has a fixed composer, scoped work, and working timeline filters", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1487, height: 1058 });
  const { errors } = await setup(page, designFixture());
  await expect(page.locator(".goal-timeline .timeline-event")).toHaveCount(9);
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
  await expect(page.locator(".goal-timeline .timeline-event")).toHaveCount(2);
  await page.getByRole("tab", { name: "Notes", exact: true }).click();
  await expect(page.locator(".goal-timeline .timeline-event")).toHaveCount(2);
  await page.getByRole("tab", { name: "Timeline", exact: true }).click();
  await page.getByLabel("Filter timeline events").selectOption("all");
  await page.getByText("View 3 context items", { exact: true }).click();
  await page.getByRole("button", { name: "/sources/flights", exact: true }).click();
  await expect(page.getByRole("dialog")).toContainText("Google Flights – HND to CTS");
  await page.keyboard.press("Escape");
  await page.getByRole("button", { name: "Add context reference" }).click();
  await page.getByRole("menuitem", { name: "Google Flights – HND to CTS", exact: true }).click();
  await expect(page.getByLabel("Add information to Goal")).toHaveValue("/sources/flights");
  expect(errors).toEqual([]);
});

test("Goal completion is confirmed and moves only the selected Goal to Archived", async ({
  page,
}) => {
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
    page
      .getByRole("navigation")
      .getByRole("button", { name: /Monitor Knowledge Engine.*Archived/ }),
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
  await page.route("**/api/rpc{,/}", async (route) => {
    const rpc = JSON.parse(route.request().postData().trim());
    if (rpc.tag !== "SendGoalMessage") return route.fallback();
    attempts++;
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
  }
  await page.getByRole("button", { name: "Choose goal" }).click();
  await page.getByRole("button", { name: "Close goals", exact: true }).click();
  await expect(page.getByRole("navigation", { name: "Goals" })).toBeHidden();
  expect(errors).toEqual([]);
});
