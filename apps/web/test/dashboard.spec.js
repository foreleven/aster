import { test, expect } from "@playwright/test";
import { fixture, at, taskPath } from "./fixtures.js";
import { setup, failRpc } from "./setup.js";

const composer = (page) => page.getByRole("textbox", { name: "Message your assistant" });
const navigate = async (page, name) =>
  page
    .getByRole("navigation", { name: "Workspace" })
    .getByRole("button", { name, exact: true })
    .click();

test("assistant uses shadcn conversation components and accepts consecutive inputs", async ({
  page,
}) => {
  const { data, reads, writes, errors } = await setup(page);
  await expect(page.getByRole("heading", { name: "A little less to carry." })).toBeVisible();
  await page.evaluate(() => document.fonts.ready);
  await page.screenshot({ path: "/tmp/aster-shadcn-home.png", animations: "disabled" });
  await page.getByRole("button", { name: "Catch me up" }).click();
  await expect(composer(page)).toHaveValue(/What changed across my goals/);
  expect(writes).toEqual([]);
  await composer(page).fill("What should I focus on?");
  await composer(page).press("Enter");
  await expect(page.locator('[data-slot="bubble"]')).toHaveCount(1);
  await expect(composer(page)).toBeFocused();
  await composer(page).fill("Also review my release.");
  await composer(page).press("Enter");
  await expect.poll(() => writes.length).toBe(2);
  expect(writes[0].body.requestId).not.toBe(writes[1].body.requestId);
  data.timelines.personal.push({
    id: 3,
    role: "assistant",
    text: "Let’s start with **release verification**.",
    at,
  });
  await page.evaluate(() => window.testEvents.emit("ready"));
  await expect(page.locator('[data-slot="bubble"] strong')).toHaveText("release verification");
  await page.screenshot({ path: "/tmp/aster-shadcn-conversation.png", animations: "disabled" });
  expect(reads).not.toContain("InspectRuntime");
  await expect(page.getByText("Awaiting a reply", { exact: true })).toHaveCount(0);
  await expect(page.getByRole("button", { name: /End Goal|Notes/ })).toHaveCount(0);
  expect(errors).toEqual([]);
});

test("Activity and Tasks open the same current Task details and Goal tasks are authoritative", async ({
  page,
}) => {
  const { data, reads, errors } = await setup(page);
  data.tasks[taskPath].result = "Release evidence is ready.";
  await navigate(page, "Knowledge Engine");
  const activity = page.getByRole("complementary", { name: "Goal details" });
  await expect(activity).toBeVisible();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(composer(page)).toBeInViewport();
  await expect(activity.getByRole("tab", { name: "Activity", exact: true })).toHaveAttribute(
    "aria-selected",
    "true",
  );
  await expect(activity.getByText("Validation starts this week.", { exact: true })).toHaveCount(0);
  await activity.getByLabel("Which environment?").fill("Test");
  await activity.getByRole("tab", { name: "Summary", exact: true }).click();
  await expect(activity.getByRole("tabpanel", { name: "Summary" })).toHaveText(
    "Validation starts this week.",
  );
  await expect(activity.getByRole("heading", { name: "Tasks · 1" })).not.toBeVisible();
  await page.screenshot({ path: "/tmp/aster-goal-summary.png", animations: "disabled" });
  await activity.getByRole("tab", { name: "Summary", exact: true }).press("ArrowLeft");
  await expect(activity.getByRole("tab", { name: "Activity", exact: true })).toBeFocused();
  await expect(activity.getByLabel("Which environment?")).toHaveValue("Test");
  await expect(activity.getByRole("heading", { name: "Tasks · 1" })).toBeVisible();
  await page.screenshot({ path: "/tmp/aster-shadcn-activity.png", animations: "disabled" });
  await activity.getByRole("button", { name: "Review the release", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Instructions" })).toBeVisible();
  await expect(page.getByText("Release evidence is ready.", { exact: true })).toBeVisible();
  await page.getByRole("tab", { name: "Execution records" }).click();
  await expect(page.getByText("Read public release context", { exact: true })).toBeVisible();
  await navigate(page, "Tasks");
  await page.getByRole("button", { name: "Review the release", exact: true }).click();
  await expect(page.getByText("Release evidence is ready.", { exact: true })).toBeVisible();
  expect(reads).toContain("InspectTask");
  expect(errors).toEqual([]);
});

test("collections use domain filters and exclude deleted signals and system records", async ({
  page,
}) => {
  const { errors } = await setup(page);
  await navigate(page, "Following");
  await expect(page.getByRole("button", { name: "Removed reminder", exact: true })).toHaveCount(0);
  await expect(page.getByRole("radio", { name: "needs input" })).toHaveCount(0);
  await page.getByRole("radio", { name: "paused", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "Watch release changes", exact: true }),
  ).toBeVisible();
  await expect(page.getByRole("button", { name: "Daily release check", exact: true })).toHaveCount(
    0,
  );
  await page.getByRole("radio", { name: "active", exact: true }).click();
  await page.getByRole("button", { name: "Daily release check", exact: true }).click();
  await expect(page.getByText("0 9 * * * · Asia/Shanghai", { exact: true })).toBeVisible();
  await expect(page.getByRole("heading", { name: "What it does" })).toBeVisible();
  await navigate(page, "Sources");
  await expect(page.getByRole("button", { name: "Context matching", exact: true })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Approvals", exact: true })).toHaveCount(0);
  await page.getByRole("button", { name: "Release notes", exact: true }).click();
  await expect(
    page.getByText("Integration is complete. Validation starts this week.", { exact: true }),
  ).toBeVisible();
  await page.goBack();
  await expect(page).toHaveURL(/view=sources/);
  await page.reload();
  await expect(page.getByRole("heading", { name: "Sources", exact: true })).toBeVisible();
  expect(errors).toEqual([]);
});

test("approval decisions and answers are submitted through the public API", async ({ page }) => {
  const { writes, errors } = await setup(page);
  await navigate(page, "Needs you");
  await page.getByRole("button", { name: "Approve execution", exact: true }).click();
  await page.getByLabel("Which environment?").fill("Test");
  await page.getByRole("button", { name: "Submit response" }).click();
  await expect.poll(() => writes.length).toBe(2);
  expect(writes[0].body.response).toEqual({ decision: "approve" });
  expect(writes[1].body.response).toEqual({ answers: { environment: ["Test"] } });
  expect(errors).toEqual([]);
});

test("unknown message acceptance preserves its identity and draft across navigation", async ({
  page,
}) => {
  const { errors } = await setup(page);
  const attempts = [];
  await page.route("**/api/rpc{,/}", async (route) => {
    const rpc = JSON.parse(route.request().postData().trim());
    if (rpc.tag !== "SendGoalMessage") return route.fallback();
    attempts.push(rpc.payload);
    await failRpc(route, rpc);
  });
  await composer(page).fill("Keep the original message");
  await composer(page).press("Enter");
  await expect(page.getByRole("alert")).toContainText("Acknowledgement missing");
  await navigate(page, "Knowledge Engine");
  await navigate(page, "Your assistant");
  await expect(composer(page)).toHaveValue("Keep the original message");
  await page.evaluate(() => window.testEvents.emit("ready"));
  expect(attempts).toHaveLength(1);
  await page.getByRole("button", { name: "Send", exact: true }).click();
  await expect.poll(() => attempts.length).toBe(2);
  expect(attempts[1]).toEqual(attempts[0]);
  expect(errors).toEqual([]);
});

for (const [tag, button] of [
  ["CheckTask", "Check original execution"],
  ["RetryTask", "Retry failed execution"],
]) {
  test(`${tag} preserves the original revision through uncertain admission`, async ({ page }) => {
    const data = fixture();
    data.contexts.find((context) => context.path === taskPath).state.status = "failed";
    const { writes, errors } = await setup(page, data);
    let frozen;
    await page.route("**/api/rpc{,/}", async (route) => {
      const rpc = JSON.parse(route.request().postData().trim());
      if (rpc.tag !== tag || frozen) return route.fallback();
      frozen = rpc.payload;
      await failRpc(route, rpc);
    });
    await navigate(page, "Tasks");
    await page.getByRole("button", { name: "Review the release", exact: true }).click();
    await page.getByRole("button", { name: button, exact: true }).click();
    await expect(page.getByRole("alert")).toContainText("Acknowledgement missing");
    await navigate(page, "Your assistant");
    data.contexts.find((context) => context.path === taskPath).revision = 9;
    await page.evaluate(() => window.testEvents.emit("ready"));
    await navigate(page, "Tasks");
    await page.getByRole("button", { name: "Review the release", exact: true }).click();
    await page.getByRole("button", { name: "Check request receipt", exact: true }).click();
    await expect.poll(() => writes.length).toBe(1);
    expect(writes[0]).toEqual({ tag, body: frozen });
    expect(frozen.expectedRevision).toBe(1);
    expect(errors).toEqual([]);
  });
}

test("unknown approval keeps the original answer after leaving the page", async ({ page }) => {
  const { writes, errors } = await setup(page);
  const attempts = [];
  await page.route("**/api/rpc{,/}", async (route) => {
    const rpc = JSON.parse(route.request().postData().trim());
    if (rpc.tag !== "RespondToApproval") return route.fallback();
    attempts.push(rpc.payload);
    if (attempts.length === 1) return failRpc(route, rpc);
    return route.fallback();
  });
  await navigate(page, "Needs you");
  await page.getByRole("button", { name: "Approve execution", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("Acknowledgement missing");
  await navigate(page, "Your assistant");
  await page.evaluate(() => window.testEvents.emit("ready"));
  await navigate(page, "Needs you");
  await page.getByRole("button", { name: "Reconcile saved decision" }).click();
  await expect.poll(() => writes.length).toBe(1);
  expect(attempts[1]).toEqual(attempts[0]);
  expect(errors).toEqual([]);
});

test("history pagination retains loaded messages across invalidation and uses MessageScroller", async ({
  page,
}) => {
  const data = fixture();
  data.timelines.personal = Array.from({ length: 65 }, (_, i) => ({
    id: i + 1,
    role: "assistant",
    text: `Response ${i + 1}`,
    at,
  }));
  const { errors } = await setup(page, data);
  await expect(page.locator('[data-slot="message"]')).toHaveCount(30);
  await page.getByRole("button", { name: "Load earlier messages" }).click();
  await expect(page.locator('[data-slot="message"]')).toHaveCount(60);
  data.timelines.personal.push({ id: 66, role: "assistant", text: "Latest update", at });
  await page.evaluate(() => window.testEvents.emit("ready"));
  await expect(page.locator('[data-slot="message"]')).toHaveCount(61);
  await expect(page.getByText("Response 6", { exact: true })).toBeAttached();
  await page.getByRole("button", { name: "Load earlier messages" }).click();
  await expect(page.locator('[data-slot="message"]')).toHaveCount(66);
  expect(errors).toEqual([]);
});

test("a failed Goal turn exposes retry without confusing it with task execution", async ({
  page,
}) => {
  const data = fixture();
  data.contexts[0].state.retryableInputId = "failed-turn";
  data.contexts[0].state.lastError = "Provider failed";
  const { writes, errors } = await setup(page, data);
  await expect(page.getByRole("alert")).toContainText("Provider failed");
  await page.getByRole("button", { name: "Retry turn", exact: true }).click();
  await expect.poll(() => writes.length).toBe(1);
  expect(writes[0].body.turnId).toBe("failed-turn");
  expect(errors).toEqual([]);
});

test("restricted and missing contexts remain explicit", async ({ page }) => {
  const data = fixture();
  data.contexts.push({
    path: "/private",
    description: "Private record",
    revision: 9,
    state: {},
    messages: [],
    projection: { visibility: "restricted", reason: "missing-policy" },
  });
  const { errors } = await setup(page, data);
  await page.getByLabel("Find context").fill("Private record");
  await page.getByRole("button", { name: "Private record", exact: true }).click();
  await expect(
    page.getByText("Only this record’s name, path and revision are available."),
  ).toBeVisible();
  await expect(page.getByText("Public context data", { exact: true })).toHaveCount(0);
  data.contexts = data.contexts.filter((context) => context.path !== "/private");
  await page.evaluate(() => window.testEvents.emit("ready"));
  await expect(page.getByText("Context unavailable", { exact: true })).toBeVisible();
  expect(errors).toEqual([]);
});

test("mobile sidebar, composer and activity remain keyboard accessible without overflow", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.emulateMedia({ reducedMotion: "reduce" });
  const { errors } = await setup(page);
  await expect(composer(page)).toBeInViewport();
  await page.screenshot({ path: "/tmp/aster-shadcn-mobile.png", animations: "disabled" });
  await page.getByRole("button", { name: "Toggle Sidebar" }).click();
  await expect(page.getByRole("dialog")).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await page.getByRole("button", { name: "Activity", exact: true }).click();
  const activity = page.getByRole("dialog");
  await expect(activity).toBeVisible();
  await activity.getByRole("tab", { name: "Summary", exact: true }).click();
  await expect(activity.getByRole("tabpanel", { name: "Summary" })).toHaveText("No summary yet.");
  await page.screenshot({ path: "/tmp/aster-goal-mobile-summary.png", animations: "disabled" });
  await page.keyboard.press("Escape");
  await expect(page.getByRole("button", { name: "Activity", exact: true })).toBeFocused();
  await expect(composer(page)).toBeInViewport();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  expect(errors).toEqual([]);
});

test("read subscription reconnects and refreshes without resending commands", async ({ page }) => {
  const { reads, writes, errors } = await setup(page);
  const before = reads.filter((tag) => tag === "ListContexts").length;
  await page.evaluate(() => window.testEvents.disconnect());
  await expect.poll(() => page.evaluate(() => window.testEvents.opened)).toBe(2);
  await expect
    .poll(() => reads.filter((tag) => tag === "ListContexts").length)
    .toBeGreaterThan(before);
  expect(writes).toEqual([]);
  expect(errors).toEqual([]);
});

test("approval query failures remain visible instead of appearing empty", async ({ page }) => {
  await setup(page);
  await page.route("**/api/rpc{,/}", async (route) => {
    const rpc = JSON.parse(route.request().postData().trim());
    if (rpc.tag !== "ListApprovals") return route.fallback();
    return failRpc(route, rpc, "Approvals unavailable");
  });
  await page.evaluate(() => window.testEvents.emit("ready"));
  await navigate(page, "Needs you");
  await expect(page.getByRole("alert")).toContainText("Approvals unavailable");
  await expect(page.getByText("Nothing needs your input")).toHaveCount(0);
});

test("context references and multiline drafts stay editable until explicitly sent", async ({
  page,
}) => {
  const { writes, errors } = await setup(page);
  await composer(page).fill("Use this context:");
  await composer(page).press("Shift+Enter");
  expect(writes).toEqual([]);
  await page.getByRole("button", { name: "Add context", exact: true }).click();
  const picker = page.getByRole("dialog", { name: "Add context" });
  await expect(picker.getByRole("button", { name: "Personal assistant", exact: true })).toHaveCount(
    0,
  );
  await picker.getByRole("button", { name: "Release notes", exact: true }).click();
  await expect(composer(page)).toHaveValue("Use this context:\n\n/sources/release");
  expect(writes).toEqual([]);
  await composer(page).press("Enter");
  await expect.poll(() => writes.length).toBe(1);
  expect(writes[0].body.text).toBe("Use this context:\n\n/sources/release");
  expect(errors).toEqual([]);
});

test("source messages and email bodies are readable while unfamiliar records stay expandable", async ({
  page,
}) => {
  const data = fixture();
  data.contexts
    .find((context) => context.path === "/sources/release")
    .messages.push(
      { content: "The release is approved.", sender: { name: "Morgan" }, at },
      { content: "Removed content", deleted: true },
      { metrics: { builds: 3 } },
    );
  data.contexts.push({
    path: "/lark/mail/inbox/one",
    description: "Email",
    revision: 1,
    messages: [],
    projection: { visibility: "public" },
    state: {
      subject: "Release sign-off",
      from: "morgan@example.com",
      bodyPlainText: "Please verify the release tomorrow.",
    },
  });
  const { errors } = await setup(page, data);
  await navigate(page, "Sources");
  await page.getByRole("button", { name: "Release notes", exact: true }).click();
  await expect(page.getByText("Frontend checks are ready.", { exact: true })).toBeVisible();
  await expect(page.getByText("The release is approved.", { exact: true })).toBeVisible();
  await expect(page.getByText("Morgan", { exact: true })).toBeVisible();
  await expect(page.getByText("Removed content", { exact: true })).toHaveCount(0);
  await page.getByText("Structured record", { exact: true }).click();
  await expect(page.locator("pre").filter({ hasText: '"builds": 3' })).toBeVisible();
  await navigate(page, "Sources");
  await page.getByRole("button", { name: "Release sign-off", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Release sign-off", exact: true })).toBeVisible();
  await expect(
    page.getByText("Please verify the release tomorrow.", { exact: true }),
  ).toBeVisible();
  expect(errors).toEqual([]);
});

test("malformed display fields affect only their own record", async ({ page }) => {
  const data = fixture();
  data.contexts.find((context) => context.path === "/sources/release").state.summary = 42;
  const { errors } = await setup(page, data);
  await navigate(page, "Sources");
  await page.getByRole("button", { name: "Release notes", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText(
    "Cannot display the public fields of /sources/release",
  );
  await navigate(page, "Your assistant");
  await expect(composer(page)).toBeEnabled();
  expect(errors).toEqual([]);
});

test("processing inspection keeps negative matches and explicit recovery", async ({ page }) => {
  const { data, writes, errors } = await setup(page);
  await page.getByLabel("Find context").fill("Context matching");
  await page.getByRole("button", { name: "Context matching", exact: true }).click();
  await expect(page.getByRole("list", { name: "Target matching results" })).toContainText(
    "NotMatched: Already handled",
  );
  data.processing.entries[0].status = "failed";
  data.processing.entries[0].error = "Decision unavailable";
  await page.evaluate(() => window.testEvents.emit("ready"));
  await page.getByRole("button", { name: "Retry screening", exact: true }).click();
  await expect.poll(() => writes.length).toBe(1);
  expect(writes[0].body._tag).toBe("RetryScreening");
  expect(writes[0].body.workId).toBe("evidence");
  expect(errors).toEqual([]);
});

test("long titles and approval content fit mobile sheets and lists", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const data = fixture();
  data.contexts.find((context) => context.path === taskPath).description = "Verification".repeat(
    25,
  );
  const { errors } = await setup(page, data);
  await page.getByRole("button", { name: "Toggle Sidebar" }).click();
  await navigate(page, "Knowledge Engine");
  await page.getByRole("button", { name: "Activity", exact: true }).click();
  const dialog = page.getByRole("dialog");
  await expect(page.getByLabel("Which environment?")).toHaveCount(1);
  await expect(
    dialog.getByRole("button", { name: "Approve execution", exact: true }),
  ).toBeVisible();
  expect(await dialog.evaluate((element) => element.scrollWidth <= element.clientWidth)).toBe(true);
  await page.screenshot({ path: "/tmp/aster-shadcn-mobile-activity.png", animations: "disabled" });
  expect(errors).toEqual([]);
});
