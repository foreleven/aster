import { test, expect } from "@playwright/test";
import { createHash } from "node:crypto";
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
        case "InspectPersonalDelegation": {
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
          const timeline = data.timelines?.[body.slug] ?? { groups: [], pendingInputs: [] };
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
          const timeline = (data.timelines[body.slug] ??= { groups: [], pendingInputs: [] });
          if (!timeline.pendingInputs.some((input) => input.inputId === body.requestId))
            timeline.pendingInputs.push({
              inputId: body.requestId,
              goalSlug: body.slug,
              ordinal: goal.messages.length,
              receivedAt: at,
              payload: { _tag: "UserInput", text: body.text },
            });
          break;
        }
        case "SendPersonalMessage": {
          writes.push({ tag: rpc.tag, body });
          const personal = data.contexts.find((c) => c.path === "/personal");
          let message = personal.messages.find((entry) => entry.requestId === body.requestId);
          if (!message) {
            personal.revision++;
            message = {
              requestId: body.requestId,
              causationId: body.causationId,
              source: "user",
              target: "/personal",
              revision: personal.revision,
              sequence: personal.messages.length + 1,
              createdAt: at,
              payload: { _tag: "UserInput", text: body.text },
            };
            personal.messages.push(message);
            personal.state.pendingRequestIds.push(body.requestId);
          }
          value = {
            requestId: body.requestId,
            revision: message.revision,
            sequence: message.sequence,
          };
          break;
        }
        case "RetryPersonalInput": {
          writes.push({ tag: rpc.tag, body });
          const personal = data.contexts.find((c) => c.path === "/personal");
          personal.revision++;
          const previous = personal.state.runs.findLast(
            (run) => run.requestId === body.inputRequestId,
          );
          personal.state.runs.push({
            ...previous,
            executionId: `retry:${body.requestId}`,
            revision: personal.revision,
            status: "running",
            error: undefined,
          });
          value = {
            requestId: body.requestId,
            revision: personal.revision,
            sequence: previous.inputSequence,
          };
          break;
        }
        case "RequestPersonalApproval": {
          writes.push({ tag: rpc.tag, body });
          const personal = data.contexts.find((item) => item.path === "/personal");
          const delivery = personal.state.outbox.find(
            (item) => item.input.requestId === body.requestId,
          );
          delivery.status = "delivered";
          delivery.error = undefined;
          delivery.receipt = { requestId: body.requestId, revision: body.approvalsRevision + 1 };
          value = { requestId: body.requestId, revision: delivery.acceptedRevision };
          break;
        }
        case "RespondPersonalApproval": {
          writes.push({ tag: rpc.tag, body });
          const personal = data.contexts.find((c) => c.path === "/personal");
          let item = personal.state.outbox.find(
            (entry) => entry.input.requestId === body.requestId,
          );
          if (!item) {
            personal.revision++;
            item = {
              input: {
                operation: "respondApproval",
                requestId: body.requestId,
                causationId: body.causationId,
                source: "/personal",
                target: "/approvals",
                expectedRevision: body.approvalsRevision,
                createdAt: at,
                approvalId: body.approvalId,
                response: body.response,
              },
              acceptedRevision: personal.revision,
              status: "pending",
            };
            personal.state.outbox.push(item);
          }
          item.status = "delivered";
          item.error = undefined;
          item.receipt = { requestId: body.requestId, revision: body.approvalsRevision + 1 };
          const queue = data.contexts.find((c) => c.path === "/approvals");
          queue.state.entries.find((entry) => entry.id === body.approvalId).status = "acknowledged";
          value = { requestId: body.requestId, revision: item.acceptedRevision };
          break;
        }
        case "ResumePersonalRun": {
          writes.push({ tag: rpc.tag, body });
          const run = data.contexts.find((item) => item.path === body.runPath);
          const personal = data.contexts.find((item) => item.path === "/personal");
          const previous = personal.state.outbox.find(
            (item) => item.input.requestId === body.requestId,
          );
          if (!previous) {
            personal.revision++;
            const input = {
              operation: "resumeRun",
              requestId: body.requestId,
              causationId: body.causationId,
              source: "/personal",
              target: body.runPath,
              expectedRevision: body.runRevision,
              createdAt: at,
            };
            run.revision++;
            run.state.resumptions = [
              {
                input,
                receipt: { requestId: body.requestId, revision: run.revision },
                status: "delivered",
              },
            ];
            personal.state.outbox.push({
              input,
              acceptedRevision: personal.revision,
              status: "delivered",
              receipt: { requestId: body.requestId, revision: run.revision },
            });
          }
          value = {
            requestId: body.requestId,
            revision: previous?.acceptedRevision ?? personal.revision,
          };
          break;
        }
        case "StartPersonalTask":
        case "ApplyPersonalSignal":
        case "SendPersonalGoalMessage": {
          writes.push({ tag: rpc.tag, body });
          const personal = data.contexts.find((c) => c.path === "/personal");
          const item = personal.state.outbox.find(
            (entry) => entry.input.requestId === body.requestId,
          );
          item.status = "delivered";
          item.error = undefined;
          item.receipt = {
            requestId: body.requestId,
            revision: (body.goalRevision ?? body.signalRevision ?? 0) + 1,
          };
          value = { requestId: body.requestId, revision: item.acceptedRevision };
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
    data.contexts.find((context) => context.path === "/personal") ??
    data.contexts.find((context) => /^\/goals\/[^/]+$/.test(context.path)) ??
    data.contexts[0];
  await expect(
    page.getByRole("heading", {
      name:
        firstGoal?.path === "/personal"
          ? "Personal Agent"
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
    { type: "error", text: "Goal Agent returned no plan", at, references: [] },
  ];
  Object.assign(data.timelines.engine.groups[0], {
    status: "failed",
    error: "Goal Agent returned no plan",
    conclusion: undefined,
  });
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

test("notification deliveries render without a dashboard projection error", async ({ page }) => {
  const data = fixture();
  data.contexts.push({
    path: "/notifications",
    revision: 1,
    description: "Business notification delivery",
    state: {
      deliveries: [
        {
          input: {
            requestId: "notification-1",
            source: "/goals/engine",
            target: "/personal",
            kind: "GoalProgress",
            revision: 1,
          },
          status: "delivered",
          attempts: 1,
          receipt: { requestId: "notification-1", revision: 2 },
        },
      ],
    },
    messages: [],
  });
  const { errors } = await setup(page, data);
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /Business notification delivery/ })
    .click();
  await expect(
    page.getByText("Unsupported dashboard fields in /notifications", { exact: false }),
  ).toHaveCount(0);
  await expect(page.locator(".context-related")).toContainText("Knowledge Engine");
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
    path: "/personal",
    description: "Personal workspace",
    revision: 4,
    state: {
      owner: { kind: "ownerless", id: "personal" },
      pendingRequestIds: ["input-1"],
      processedThrough: 0,
      runs: [
        {
          requestId: "input-1",
          executionId: "input:input-1",
          inputSequence: 1,
          status: "failed",
          startedAt: at,
          error: "Provider temporarily unavailable",
        },
      ],
      outbox: [
        {
          input: {
            requestId: "delivery-1",
            causationId: "input-1",
            source: "/personal",
            target: "/goals/engine",
            expectedRevision: 3,
            createdAt: at,
            text: "Prioritize the release checklist",
          },
          acceptedRevision: 4,
          status: "unknown",
          error: "Receiver acknowledgement not confirmed",
        },
      ],
    },
    messages: [
      {
        requestId: "input-1",
        causationId: "user-1",
        source: "user",
        target: "/personal",
        revision: 1,
        sequence: 1,
        createdAt: at,
        payload: { _tag: "UserInput", text: "Help me prepare the release" },
      },
    ],
  });
  return data;
}

const rpcFailure = (rpc, kind, message) => ({
  contentType: "application/ndjson",
  body:
    JSON.stringify({
      _tag: "Exit",
      requestId: rpc.id,
      exit: {
        _tag: "Failure",
        cause: [{ _tag: "Fail", error: { _tag: "ApplicationError", kind, message } }],
      },
    }) + "\n",
});

test("Personal workspace shows committed work and sends typed input", async ({ page }) => {
  const data = personalFixture();
  const { writes, errors } = await setup(page, data);
  await expect(page).toHaveTitle("Aster · Workspace");
  await expect(page.locator("vite-error-overlay")).toHaveCount(0);
  await expect(
    page.getByRole("main").getByText("Help me prepare the release", { exact: true }),
  ).toBeVisible();
  await expect(page.getByRole("complementary", { name: "Related work" })).toContainText(
    "Provider temporarily unavailable",
  );
  await page.screenshot({ path: "/tmp/aster-personal-desktop.png" });
  await page.getByLabel("Message Personal Agent").fill("Check the release blockers");
  await page.getByRole("button", { name: "Send message", exact: true }).click();
  await expect(
    page.getByRole("main").getByText("Check the release blockers", { exact: true }),
  ).toBeVisible();
  expect(writes[0]).toMatchObject({
    tag: "SendPersonalMessage",
    body: { expectedRevision: 4, text: "Check the release blockers" },
  });
  expect(writes[0].body.requestId).toBeTruthy();
  await expect(page.getByLabel("Message Personal Agent")).toHaveValue("");
  await page.getByRole("button", { name: "Retry input 1" }).click();
  await expect(
    page.getByRole("complementary", { name: "Related work" }).getByText("Running", { exact: true }),
  ).toBeVisible();
  await expect(page.getByRole("button", { name: "Retry input 1" })).toHaveCount(0);
  expect(writes[1]).toMatchObject({
    tag: "RetryPersonalInput",
    body: { inputRequestId: "input-1", expectedRevision: 5 },
  });
  await page.getByRole("button", { name: "Reconcile delivery" }).click();
  await expect(page.getByText("Accepted at target revision 4")).toBeVisible();
  expect(writes[2]).toMatchObject({
    tag: "SendPersonalGoalMessage",
    body: {
      requestId: "delivery-1",
      causationId: "input-1",
      goalSlug: "engine",
      goalRevision: 3,
      text: "Prioritize the release checklist",
    },
  });
  expect(errors).toEqual([]);
});

test("uncertain Personal submission retains identity across navigation and reconnect", async ({
  page,
}) => {
  const data = personalFixture();
  const { writes, errors } = await setup(page, data);
  let failed;
  await page.route("**/api/rpc{,/}", async (route) => {
    const rpc = JSON.parse(route.request().postData().trim());
    if (rpc.tag !== "SendPersonalMessage" || failed) return route.fallback();
    failed = rpc.payload;
    await route.fulfill(rpcFailure(rpc, "unavailable", "Submission outcome unknown"));
  });
  await page.getByLabel("Message Personal Agent").fill("Keep this exact request");
  await page.getByRole("button", { name: "Send message", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("Submission outcome unknown");
  await expect(page.getByLabel("Message Personal Agent")).toBeDisabled();
  expect(writes).toHaveLength(0);
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /Monitor Knowledge Engine/ })
    .click();
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /Personal Agent/ })
    .click();
  data.contexts[0].revision = 9;
  await page.evaluate(() => window.testEvents.emit("ready"));
  await expect(page.getByText("Revision 9", { exact: true })).toBeVisible();
  await expect(page.getByLabel("Message Personal Agent")).toHaveValue("Keep this exact request");
  await page.getByRole("button", { name: "Retry submission" }).click();
  await expect(page.getByLabel("Message Personal Agent")).toHaveValue("");
  expect(writes[0].body).toEqual(failed);
  expect(errors).toEqual([]);
});

test("Personal revision conflict refreshes before a new explicit submission", async ({ page }) => {
  const data = personalFixture();
  const { writes, errors } = await setup(page, data);
  let rejected;
  await page.route("**/api/rpc{,/}", async (route) => {
    const rpc = JSON.parse(route.request().postData().trim());
    if (rpc.tag !== "SendPersonalMessage" || rejected) return route.fallback();
    rejected = rpc.payload;
    data.contexts[0].revision = 8;
    await route.fulfill(rpcFailure(rpc, "conflict", "Personal Context revision changed"));
  });
  await page.getByLabel("Message Personal Agent").fill("Review current priorities");
  await page.getByRole("button", { name: "Send message", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("Personal Context revision changed");
  await expect(page.getByText("Revision 8", { exact: true })).toBeVisible();
  await expect(page.getByLabel("Message Personal Agent")).toBeEnabled();
  expect(writes).toHaveLength(0);
  await page.getByRole("button", { name: "Send message", exact: true }).click();
  await expect(page.getByLabel("Message Personal Agent")).toHaveValue("");
  expect(writes[0].body.expectedRevision).toBe(8);
  expect(writes[0].body.requestId).not.toBe(rejected.requestId);
  expect(errors).toEqual([]);
});

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
  await expect(page.getByRole("heading", { name: "Personal Agent", exact: true })).toBeVisible();
  await page.goForward();
  await expect(page.getByRole("main")).toContainText("Core workflow integration is complete");
  await page.reload();
  await expect(page.getByRole("main")).toContainText("Core workflow integration is complete");
  data.contexts = data.contexts.filter((context) => context.path !== "/lark/im/chats/chat-1");
  await page.evaluate(() => window.testEvents.emit("ready"));
  await expect(page.getByRole("heading", { name: "Context unavailable" })).toBeVisible();
  expect(errors).toEqual([]);
});

test("Personal mobile layout exposes navigation, messages and related work", async ({ page }) => {
  const { errors } = await setup(page, personalFixture());
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.getByLabel("Message Personal Agent")).toBeVisible();
  await expect(page.getByRole("complementary", { name: "Related work" })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(
    true,
  );
  await page.screenshot({ path: "/tmp/aster-personal-mobile.png", fullPage: true });
  await page.getByRole("button", { name: "Choose context" }).click();
  await expect(page.getByRole("navigation", { name: "Contexts" })).toBeVisible();
  await page.getByRole("button", { name: "Close contexts" }).click();
  await expect(page.getByRole("navigation", { name: "Contexts" })).toBeHidden();
  expect(errors).toEqual([]);
});

test("unsupported Personal data disables input and native Pi frames stay out of generic messages", async ({
  page,
}) => {
  const data = personalFixture();
  data.contexts[0].state.pendingRequestIds = "invalid";
  const { errors } = await setup(page, data);
  await expect(page.getByRole("alert")).toContainText("Personal Context contains unsupported data");
  await expect(page.getByLabel("Message Personal Agent")).toBeDisabled();
  const chat = data.contexts.find((context) => context.path === "/lark/im/chats/chat-1");
  chat.messages.push(
    { kind: "pi.native", text: "Native implementation frame" },
    { role: "system", content: "Internal system prompt" },
    { role: "toolResult", content: "Internal tool response" },
  );
  await page.evaluate(() => window.testEvents.emit("ready"));
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /\/lark\/im\/chats\/chat-1/ })
    .click();
  await expect(page.getByRole("main")).not.toContainText("Native implementation frame");
  await expect(page.getByRole("main")).not.toContainText("Internal system prompt");
  await expect(page.getByRole("main")).not.toContainText("Internal tool response");
  expect(errors).toEqual([]);
});

test("Personal Signal delivery shows its command and reconciles with the same request identity", async ({
  page,
}) => {
  const data = personalFixture();
  const personal = data.contexts.find((item) => item.path === "/personal");
  personal.state.outbox = [
    {
      input: {
        operation: "createSignal",
        requestId: "signal-delivery-1",
        causationId: "input-1",
        source: "/personal",
        target: "/signals/personal--release",
        expectedRevision: 0,
        createdAt: at,
        definition: {
          when: "Release updates",
          task: "Inspect release blockers",
          agent: "test",
          schedule: { type: "cron", expression: "0 9 * * *", timeZone: "Asia/Shanghai" },
        },
        active: true,
      },
      acceptedRevision: 4,
      attempts: 1,
      status: "unknown",
      error: "Acknowledgement missing",
    },
  ];
  const { writes } = await setup(page, data);
  await expect(page.getByText("Create Signal: Inspect release blockers")).toBeVisible();
  await expect(page.getByText("Delivery attempts: 1")).toBeVisible();
  await expect(page.getByText(/Confirmation required for each Run/)).toBeVisible();
  await page.getByRole("button", { name: "Reconcile delivery" }).click();
  await expect(page.getByText("Accepted at target revision 1")).toBeVisible();
  expect(writes.at(-1)).toMatchObject({
    tag: "ApplyPersonalSignal",
    body: {
      operation: "createSignal",
      requestId: "signal-delivery-1",
      signalSlug: "personal--release",
      signalRevision: 0,
      expectedRevision: 4,
    },
  });
});

test("Personal approval preserves an uncertain decision across navigation and reconciles its original identity", async ({
  page,
}) => {
  const data = personalFixture();
  data.contexts.find((item) => item.path === "/approvals").revision = 7;
  const { writes, errors } = await setup(page, data);
  let frozen;
  await page.route("**/api/rpc{,/}", async (route) => {
    const rpc = JSON.parse(route.request().postData().trim());
    if (rpc.tag !== "RespondPersonalApproval" || frozen) return route.fallback();
    frozen = rpc.payload;
    return route.fulfill(rpcFailure(rpc, "unavailable", "Approval admission is uncertain"));
  });
  const openApprovals = () =>
    page
      .getByRole("navigation")
      .getByRole("button", { name: /Task approval queue/ })
      .click();
  await openApprovals();
  await page.getByRole("button", { name: "Approve execution", exact: true }).click();
  await expect(page.getByRole("button", { name: "Reconcile saved decision" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Reject", exact: true })).toHaveCount(0);
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /Personal Agent/ })
    .click();
  await openApprovals();
  data.contexts.find((item) => item.path === "/approvals").revision = 8;
  await page.evaluate(() => window.testEvents.emit("ready"));
  await page.getByRole("button", { name: "Reconcile saved decision" }).click();
  await expect(page.getByRole("button", { name: "Reconcile saved decision" })).toHaveCount(0);
  expect(writes.at(-1)).toMatchObject({ tag: "RespondPersonalApproval", body: frozen });
  expect(frozen).toMatchObject({
    expectedRevision: 4,
    approvalsRevision: 7,
    approvalId: "approval-1",
    response: { decision: "approve" },
  });
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /Personal Agent/ })
    .click();
  await expect(page.getByText("Approval approval-1: approve", { exact: true })).toBeVisible();
  expect(errors).toEqual([]);
});

test("Personal approval delivery reconciles a persisted unknown response without replacing its payload", async ({
  page,
}) => {
  const data = personalFixture();
  const personal = data.contexts.find((item) => item.path === "/personal");
  personal.state.outbox = [
    {
      input: {
        operation: "respondApproval",
        requestId: "decision-1",
        causationId: "user-choice",
        source: "/personal",
        target: "/approvals",
        expectedRevision: 7,
        createdAt: at,
        approvalId: "approval-1",
        response: { decision: "reject" },
      },
      acceptedRevision: 4,
      attempts: 1,
      status: "unknown",
      error: "Acknowledgement missing",
    },
  ];
  const { writes } = await setup(page, data);
  await expect(page.getByText("Approval approval-1: reject")).toBeVisible();
  await page.getByRole("button", { name: "Reconcile delivery" }).click();
  await expect(page.getByText("Accepted at target revision 8")).toBeVisible();
  expect(writes.at(-1)).toMatchObject({
    tag: "RespondPersonalApproval",
    body: {
      requestId: "decision-1",
      causationId: "user-choice",
      expectedRevision: 4,
      approvalsRevision: 7,
      approvalId: "approval-1",
      response: { decision: "reject" },
    },
  });
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
        runPath: "/signals/progress/runs/run-1",
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
  expect(reads.filter((tag) => tag === "InspectPersonalDelegation").length).toBeGreaterThan(1);
  expect(errors).toEqual([]);
});

test("Personal one-time Task reconciles its frozen payload and presents the admitted Run", async ({
  page,
}) => {
  const data = personalFixture();
  const personal = data.contexts.find((item) => item.path === "/personal");
  const requestId = "task-delivery-1";
  const path = `/runs/personal--${createHash("sha256").update(requestId).digest("hex")}`;
  const task = {
    instructions: "Draft the release summary",
    input: [{ content: "All release blockers are resolved.", sources: ["/goals/engine"] }],
  };
  personal.state.outbox = [
    {
      input: {
        operation: "startTask",
        requestId,
        causationId: "input-1",
        source: "/personal",
        target: path,
        expectedRevision: 0,
        createdAt: at,
        agent: "test",
        task,
      },
      acceptedRevision: 4,
      attempts: 1,
      status: "unknown",
      error: "Acknowledgement missing",
    },
  ];
  const run = {
    path,
    description: "Task: Draft the release summary",
    revision: 2,
    state: { status: "awaiting-confirmation", task, sourcePath: "/personal" },
    messages: [
      { type: "Triggered", at },
      { type: "ConfirmationRequested", at },
    ],
  };
  data.contexts.push(run);
  const queue = data.contexts.find((item) => item.path === "/approvals");
  queue.state.entries.push({
    id: "personal-task-confirm",
    target: `/user${path}`,
    contextPath: path,
    kind: "confirmation",
    status: "pending",
    request: {
      id: "personal-task-confirm",
      kind: "approval",
      prompt: "Execute this release summary Task?",
    },
  });
  const { writes, errors } = await setup(page, data);
  await expect(
    page.getByText("One-time Task · Confirmation required before execution"),
  ).toBeVisible();
  await page.getByRole("button", { name: "Reconcile delivery" }).click();
  await expect(page.getByText("Accepted at target revision 1")).toBeVisible();
  expect(writes.at(-1)).toEqual({
    tag: "StartPersonalTask",
    body: { requestId, causationId: "input-1", expectedRevision: 4, agent: "test", task },
  });
  await page.getByRole("button", { name: "View Task Run" }).click();
  const details = page.getByRole("region", { name: "Task Run", exact: true });
  await expect(details.getByText(task.instructions, { exact: true })).toBeVisible();
  await expect(details.getByText(task.input[0].content)).toBeVisible();
  await expect(details.getByText("Task preparation: Completed")).toBeVisible();
  await expect(details.getByText("Confirm / Auto: Current stage")).toBeVisible();
  await expect(page.getByText("Execute this release summary Task?")).toBeVisible();
  await expect(page.getByRole("alert")).toHaveCount(0);
  run.state.status = "completed";
  run.state.outcomeText = "Release summary saved for review.";
  run.revision = 5;
  run.messages.push({ type: "Completed", at, text: run.state.outcomeText });
  queue.state.entries.at(-1).status = "acknowledged";
  await page.evaluate(() => window.testEvents.emit("ready"));
  await expect(details.getByText("Release summary saved for review.")).toBeVisible();
  expect(writes).toHaveLength(1);
  expect(errors).toEqual([]);
});

test("Run resumption preserves uncertain admission across navigation and reconnect", async ({
  page,
}) => {
  const data = personalFixture();
  const personal = data.contexts.find((item) => item.path === "/personal");
  personal.state.outbox = [];
  const run = data.contexts.find((item) => item.path === "/signals/progress/runs/run-1");
  run.revision = 7;
  run.state.status = "failed";
  run.state.task = { instructions: "Continue original work", input: [] };
  const { writes, errors } = await setup(page, data);
  let frozen;
  await page.route("**/api/rpc{,/}", async (route) => {
    const rpc = JSON.parse(route.request().postData().trim());
    if (rpc.tag !== "ResumePersonalRun" || frozen) return route.fallback();
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
    .getByRole("button", { name: /progress.*run-1|run-1/i })
    .first()
    .click();
  await page.getByRole("button", { name: "Resume execution", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("Resume acknowledgement missing");
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /Personal Agent/ })
    .click();
  personal.revision = 10;
  run.revision = 9;
  await page.evaluate(() => window.testEvents.emit("ready"));
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /progress.*run-1|run-1/i })
    .first()
    .click();
  await page.getByRole("button", { name: "Reconcile resumption", exact: true }).click();
  await expect(page.getByText("Resumption delivered", { exact: true })).toBeVisible();
  expect(writes.at(-1)).toEqual({ tag: "ResumePersonalRun", body: frozen });
  expect(frozen).toMatchObject({ expectedRevision: 4, runRevision: 7, runPath: run.path });
  expect(errors).toEqual([]);
});

test("Personal approval request reconciles its original demand and opens the still-pending approval", async ({
  page,
}) => {
  const data = personalFixture();
  const path = "/signals/progress/runs/run-1";
  const id = `${path}:confirm`;
  const personal = data.contexts.find((item) => item.path === "/personal");
  personal.state.outbox = [
    {
      input: {
        operation: "requestApproval",
        requestId: "approval-request-1",
        causationId: "input-1",
        source: "/personal",
        target: "/approvals",
        expectedRevision: 7,
        contextPath: path,
        contextRevision: 6,
        approvalId: id,
        createdAt: at,
      },
      acceptedRevision: 4,
      attempts: 1,
      status: "unknown",
      error: "Request receipt missing",
    },
  ];
  data.contexts
    .find((item) => item.path === "/approvals")
    .state.entries.push({
      id,
      target: "/user/signals/progress/~cnVucy9ydW4tMQ",
      contextPath: path,
      kind: "confirmation",
      status: "pending",
      request: { id, kind: "approval", prompt: "Approve this exact prepared Task?" },
    });
  const { writes, errors } = await setup(page, data);
  await expect(page.getByText(`Request approval: ${id}`, { exact: true })).toBeVisible();
  await expect(page.getByText(`For ${path} · Source revision 6`, { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Reconcile delivery", exact: true }).click();
  await expect(page.getByText("Accepted at target revision 8")).toBeVisible();
  expect(writes.at(-1)).toEqual({
    tag: "RequestPersonalApproval",
    body: {
      requestId: "approval-request-1",
      causationId: "input-1",
      expectedRevision: 4,
      approvalId: id,
      approvalsRevision: 7,
      contextPath: path,
      contextRevision: 6,
    },
  });
  await page.getByRole("button", { name: "View requested approval", exact: true }).click();
  await expect(page.getByText("Approve this exact prepared Task?", { exact: true })).toBeVisible();
  expect(writes).toHaveLength(1);
  expect(data.contexts.find((item) => item.path === "/approvals").state.entries.at(-1).status).toBe(
    "pending",
  );
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

test("Personal business progress links to its source and preserves display-only outcomes after reconnect", async ({
  page,
}) => {
  const data = personalFixture();
  const personal = data.contexts.find((item) => item.path === "/personal");
  personal.state.runs = [];
  personal.state.pendingRequestIds = [];
  personal.state.outbox = [];
  const source = "/signals/review/runs/result";
  data.contexts.push({
    path: source,
    description: "Release review result",
    revision: 4,
    state: { status: "blocked", outcomeText: "Additional evidence is needed" },
    messages: [],
  });
  const notification = {
    requestId: "result-one",
    causationId: "input-1",
    source,
    target: "/personal",
    revision: 4,
    createdAt: at,
    causal: { rootRequestId: "input-1", remainingAgentTurns: 0 },
    kind: "RunResult",
    text: "The release review needs additional evidence.",
  };
  personal.messages.push({
    requestId: notification.requestId,
    causationId: notification.causationId,
    source,
    target: "/personal",
    revision: 4,
    sequence: 2,
    createdAt: at,
    causal: notification.causal,
    payload: {
      _tag: "ProgressEvent",
      text: notification.text,
      notification,
      processing: "display-only",
    },
  });
  const { writes, errors } = await setup(page, data);
  const progress = page.locator(".message").filter({ hasText: notification.text });
  await expect(progress.getByText("Progress", { exact: true })).toBeVisible();
  await expect(
    progress.getByText("Saved for you. Automatic follow-up has reached its limit."),
  ).toBeVisible();
  await progress.getByRole("button", { name: source, exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Release review result", exact: true }),
  ).toBeVisible();
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /Personal/ })
    .click();
  await page.evaluate(() => window.testEvents.emit("ready"));
  await expect(page.locator(".message").filter({ hasText: notification.text })).toHaveCount(1);
  expect(writes).toEqual([]);
  expect(errors).toEqual([]);
});

test("Timeline refreshes mutable older groups after reconnect and keeps pending notes distinct", async ({
  page,
}) => {
  const data = fixture();
  const template = data.timelines.engine.groups[0];
  data.timelines.engine.groups = Array.from({ length: 65 }, (_, i) => ({
    ...template,
    evaluationId: `evaluation-${i + 1}`,
    ordinal: i + 1,
    conclusion: { text: `Conclusion ${i + 1}`, evidence: [], applied: true },
    inputs: [{ ...template.inputs[0], inputId: `input-${i + 1}` }],
    agentRun: { sessionId: "engine", requestId: `evaluation-${i + 1}` },
  }));
  const older = data.timelines.engine.groups[10];
  older.status = "partially_applied";
  older.outputs = [
    {
      id: "delivery-11",
      kind: "signal",
      target: "/signals/progress",
      operation: "create",
      title: "Watch integration",
      status: "unknown",
    },
  ];
  const { errors } = await setup(page, data);
  await expect(page.locator(".evaluation-card")).toHaveCount(30);
  await page.getByRole("button", { name: "Load earlier records" }).click();
  await expect(page.locator(".evaluation-card")).toHaveCount(60);
  await expect(page.locator("#evaluation-evaluation-11")).toContainText("create · unknown");
  older.status = "completed";
  older.outputs[0].status = "applied";
  data.timelines.engine.pendingInputs.push({
    inputId: "pending-note",
    goalSlug: "engine",
    ordinal: 66,
    receivedAt: at,
    payload: { _tag: "UserInput", text: "Please verify the release date" },
  });
  await page.evaluate(() => window.testEvents.emit("ready"));
  await expect(page.locator("#evaluation-evaluation-11")).toContainText("create · applied");
  await expect(page.locator(".timeline-pending")).toContainText("Please verify the release date");
  await expect(page.locator(".evaluation-card")).toHaveCount(60);
  await page.getByRole("button", { name: "Load earlier records" }).click();
  await expect(page.locator(".evaluation-card")).toHaveCount(65);
  await expect(page.getByRole("button", { name: "Load earlier records" })).toHaveCount(0);
  await page.getByRole("tab", { name: "Notes", exact: true }).click();
  await expect(page.locator(".timeline-pending")).toContainText("Please verify the release date");
  await expect(page.locator(".evaluation-conclusion")).toHaveCount(0);
  expect(errors).toEqual([]);
});

test("Goal Signal retry keeps its authorization identity across navigation and an unknown acknowledgement", async ({
  page,
}) => {
  const data = designFixture();
  const group = data.timelines.engine.groups[1];
  group.status = "partially_applied";
  group.outputs[0].status = "unknown";
  group.outputs[0].attempts = 3;
  const { errors } = await setup(page, data);
  const requests = [];
  await page.route("**/api/rpc{,/}", async (route) => {
    const rpc = JSON.parse(route.request().postData().trim());
    if (rpc.tag !== "RetryGoalSignal") return route.fallback();
    requests.push(rpc.payload);
    const first = requests.length === 1;
    if (!first) {
      group.outputs[0].status = "delivered";
      group.outputs[0].attempts = 4;
      group.status = "completed";
    }
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
                      message: "Retry acknowledgement missing",
                    },
                  },
                ],
              }
            : { _tag: "Success", value: { requestId: rpc.payload.requestId, revision: 42 } },
        }) + "\n",
    });
  });
  await page.getByRole("button", { name: "Retry delivery", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("Retry acknowledgement missing");
  await page.screenshot({ path: "/tmp/aster-goal-signal-retry.png" });
  await page.getByRole("tab", { name: "Notes", exact: true }).click();
  await page.getByRole("tab", { name: "Timeline", exact: true }).click();
  await page.evaluate(() => window.testEvents.emit("ready"));
  await expect(page.getByRole("button", { name: "Check retry receipt" })).toBeVisible();
  expect(requests).toHaveLength(1);
  await page.getByRole("button", { name: "Check retry receipt" }).click();
  await expect(page.locator("#evaluation-trip-2")).toContainText("create · delivered");
  expect(requests).toHaveLength(2);
  expect(requests[1]).toEqual(requests[0]);
  expect(requests[0].operationId).toBe("price-watch");
  expect(requests[0].expectedAttempts).toBe(3);
  await expect(page.getByRole("button", { name: "Check retry receipt" })).toHaveCount(0);
  expect(errors).toEqual([]);
});

for (const owner of ["system-one", "notifications"]) {
  test(`${owner} recovery preserves authorization after navigation and reconnect`, async ({
    page,
  }) => {
    const data = fixture();
    const title =
      owner === "system-one" ? "Context reaction processing" : "Business notification delivery";
    const entry = {
      id: "work-1",
      kind: owner === "system-one" ? "screening" : "notification",
      source: "/lark/im/chats/chat-1",
      target: owner === "system-one" ? "/system-one" : "/personal",
      status: owner === "system-one" ? "failed" : "unknown",
      attempts: 3,
      error: "Previous attempt failed",
    };
    data.processing = { [owner]: { owner, revision: 10, entries: [entry] } };
    data.contexts.push({
      path: `/${owner}`,
      revision: 10,
      description: title,
      state: {},
      messages: [],
    });
    const { errors } = await setup(page, data);
    const requests = [];
    await page.route("**/api/rpc{,/}", async (route) => {
      const rpc = JSON.parse(route.request().postData().trim());
      if (rpc.tag !== "RecoverProcessing") return route.fallback();
      requests.push(rpc.payload);
      const first = requests.length === 1;
      if (!first) {
        entry.status = owner === "system-one" ? "completed" : "delivered";
        entry.error = undefined;
        entry.attempts = 4;
        data.processing[owner].revision = 12;
      }
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
                        message: "Recovery acknowledgement missing",
                      },
                    },
                  ],
                }
              : { _tag: "Success", value: { requestId: rpc.payload.requestId, revision: 11 } },
          }) + "\n",
      });
    });
    const open = () =>
      page
        .getByRole("navigation")
        .getByRole("button", { name: new RegExp(title) })
        .click();
    await open();
    await page
      .getByRole("button", {
        name: owner === "system-one" ? "Retry screening" : "Retry delivery",
        exact: true,
      })
      .click();
    await expect(page.getByRole("alert")).toContainText("Recovery acknowledgement missing");
    await page
      .getByRole("navigation")
      .getByRole("button", { name: /Monitor Knowledge Engine/ })
      .click();
    await open();
    await page.evaluate(() => window.testEvents.emit("ready"));
    await expect(page.getByRole("button", { name: "Check recovery receipt" })).toBeVisible();
    await page.screenshot({ path: `/tmp/aster-${owner}-pending-desktop.png` });
    await page.setViewportSize({ width: 390, height: 844 });
    await expect(page.getByRole("button", { name: "Check recovery receipt" })).toBeVisible();
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
    ).toBe(true);
    await expect(page.locator(".breadcrumbs")).toHaveCount(1);
    await page.evaluate(
      () => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))),
    );
    await page.screenshot({ path: `/tmp/aster-${owner}-pending-mobile.png` });
    expect(requests).toHaveLength(1);
    await page.getByRole("button", { name: "Check recovery receipt" }).click();
    await expect(page.getByRole("region", { name: "Processing recovery" })).toContainText(
      owner === "system-one" ? "Screening · completed" : "Delivery · delivered",
    );
    expect(requests).toHaveLength(2);
    expect(requests[0]).toEqual(requests[1]);
    expect(requests[0].expectedRevision).toBe(10);
    await expect(page.getByRole("button", { name: "Check recovery receipt" })).toHaveCount(0);
    await page.screenshot({ path: `/tmp/aster-${owner}-recovery.png` });
    expect(errors).toEqual([]);
  });
}

test("Run publication shows separate approval and retained unknown outcome after reconnect", async ({
  page,
}) => {
  const data = personalFixture();
  const source = "/signals/report/runs/one";
  const operation = {
    request: {
      requestId: "publish-one",
      source,
      signalPath: "/signals/report",
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
  expect(requests[0].turnId).toBe(group.evaluationId);
  expect(errors).toEqual([]);
});
