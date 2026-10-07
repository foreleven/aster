import { expect } from "@playwright/test";
import { at, fixture } from "./fixtures.js";
export async function setup(page, data = fixture()) {
  const errors = [],
    reads = [],
    writes = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("console", (message) => {
    if (["error", "warning"].includes(message.type())) errors.push(message.text());
  });
  // Mock only the streaming RPC; the real-server case exercises the native HTTP path.
  await page.addInitScript(() => {
    const nativeFetch = window.fetch.bind(window);
    window.testEvents = { opened: 0, closed: 0 };
    window.fetch = async (input, init) => {
      const request = new Request(input, init);
      if (new URL(request.url).pathname.replace(/\/$/, "") !== "/api/rpc")
        return nativeFetch(input, init);
      const rpc = JSON.parse(await request.clone().text());
      if (rpc.tag !== "SubscribeInvalidations") return nativeFetch(input, init);
      window.testEvents.opened++;
      let closed = false;
      const close = () => {
        if (!closed) {
          closed = true;
          window.testEvents.closed++;
        }
      };
      const body = new ReadableStream({
        start(controller) {
          const encoder = new TextEncoder();
          window.testEvents.emit = (type, data = {}) => {
            if (closed) return;
            const value = type === "ready" ? { _tag: "Invalidate", keys: ["all-queries"] } : data;
            controller.enqueue(
              encoder.encode(
                JSON.stringify({ _tag: "Chunk", requestId: rpc.id, values: [value] }) + "\n",
              ),
            );
          };
          window.testEvents.disconnect = () => {
            close();
            controller.error(new TypeError("Network disconnected"));
          };
          request.signal.addEventListener("abort", () => {
            if (!closed) {
              close();
              controller.error(new DOMException("Aborted", "AbortError"));
            }
          });
          queueMicrotask(() => window.testEvents.emit("ready"));
        },
        cancel() {
          close();
        },
      });
      return new Response(body, { headers: { "content-type": "application/ndjson" } });
    };
  });

  await page.route("**/api/rpc{,/}", async (route) => {
    const rpc = JSON.parse(route.request().postData().trim());
    const body = rpc.payload;
    reads.push(rpc.tag);
    let value;
    switch (rpc.tag) {
      case "ListContexts":
        value = data.contexts;
        break;
      case "ListApprovals":
        value = data.approvals;
        break;
      case "InspectTask":
        value = data.tasks[body.path];
        break;
      case "InspectProcessing":
        value = data.processing;
        break;
      case "GetGoalTimeline": {
        const all = data.timelines[body.slug] ?? [];
        const eligible = all.filter(
          (message) => body.before === undefined || message.id < body.before,
        );
        const messages = eligible.slice(-30);
        value = {
          messages,
          total: all.length,
          nextBefore: eligible.length > messages.length ? messages[0].id : null,
        };
        break;
      }
      case "SendGoalMessage": {
        writes.push({ tag: rpc.tag, body });
        const messages = (data.timelines[body.slug] ??= []);
        messages.push({ id: (messages.at(-1)?.id ?? 0) + 1, role: "user", text: body.text, at });
        break;
      }
      case "RespondToApproval":
        writes.push({ tag: rpc.tag, body });
        data.approvals.find((entry) => entry.id === body.id).status = "acknowledged";
        break;
      case "RetryGoalTurn":
        writes.push({ tag: rpc.tag, body });
        delete data.contexts.find((context) => context.path === `/goals/${body.slug}`).state
          .retryableInputId;
        value = { requestId: body.requestId, revision: 2 };
        break;
      case "CheckTask":
      case "RetryTask":
      case "RecoverProcessing":
        writes.push({ tag: rpc.tag, body });
        value = { requestId: body.requestId, revision: 2 };
        break;
      default:
        throw new Error(`Unexpected RPC: ${rpc.tag}`);
    }
    await route.fulfill({
      contentType: "application/ndjson",
      body:
        JSON.stringify({
          _tag: "Exit",
          requestId: rpc.id,
          exit: { _tag: "Success", value: value ?? null },
        }) + "\n",
    });
  });
  await page.goto("/");
  if (data.contexts.length)
    await expect(
      page.getByRole("heading", { name: "Personal assistant", exact: true }),
    ).toBeVisible();
  return { data, errors, reads, writes };
}
export async function failRpc(
  route,
  rpc,
  message = "Acknowledgement missing",
  kind = "unavailable",
) {
  await route.fulfill({
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
}
