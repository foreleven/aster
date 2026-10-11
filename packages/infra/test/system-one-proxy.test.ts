import assert from "node:assert/strict";
import { createServer, type RequestListener } from "node:http";
import type { Socket } from "node:net";
import { test, type TestContext } from "node:test";
import { ConfigProvider, Deferred, Effect, Fiber, Layer, Logger, Redacted } from "effect";
import { choice, SystemOneClient } from "@aster/core";
import { SystemOneTransport } from "../src/system-one/layer.js";
import { resolveSystemOneSettings, type SystemOneConfig } from "../src/system-one/settings.js";
import { http } from "./http-fixtures.js";

const request = { state: "Evidence", questions: { intent: choice("Choose", { yes: "Yes" }) } };
const config = { url: "http://upstream.invalid?ak=query", model: "test", apiKey: "bearer" };
const transport = (settings: SystemOneConfig) =>
  SystemOneTransport.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        ConfigProvider.layer(ConfigProvider.fromUnknown({ config: { "system-one": settings } })),
        http(async () => {
          assert.fail("Explicit proxy must bypass the host HTTP client");
        }),
      ),
    ),
  );

// Native HTTP callbacks are the test transport boundary; no real upstream is contacted.
const localProxy = async (t: TestContext, listener?: RequestListener) => {
  const server = createServer(listener);
  const sockets = new Set<Socket>();
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
  });
  t.after(async () => {
    for (const socket of sockets) socket.destroy();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  return { server, url: `http://127.0.0.1:${address.port}` };
};

test("proxy settings resolve exact references and redact credentials", async () => {
  const proxy = "https://user:PRIVATE_PROXY_PASSWORD@proxy.test:8443";
  for (const value of [proxy, "${PROXY}"]) {
    const settings = await Effect.runPromise(
      resolveSystemOneSettings(
        { ...config, proxy: value },
        ConfigProvider.fromUnknown({ secrets: { PROXY: proxy } }),
      ),
    );
    assert.ok(settings.proxy);
    assert.equal(Redacted.value(settings.proxy), `${proxy}/`);
    assert.doesNotMatch(JSON.stringify(settings), /PRIVATE_PROXY_PASSWORD/);
  }
  for (const proxy of [
    "",
    "${MISSING}",
    "not a URL PRIVATE_PROXY_PASSWORD",
    "socks5://proxy.test:1080",
    "http://proxy.test/path",
    "http://proxy.test?password=PRIVATE_PROXY_PASSWORD",
    "http://proxy.test#fragment",
  ]) {
    const error = await Effect.runPromise(
      resolveSystemOneSettings({ ...config, proxy }, ConfigProvider.fromUnknown({})).pipe(
        Effect.flip,
      ),
    );
    assert.doesNotMatch(JSON.stringify(error), /PRIVATE_PROXY_PASSWORD/);
  }
});

test("System One POST uses the proxy, preserves protocol and separates proxy authentication", async (t) => {
  const logs: unknown[] = [];
  const proxy = await localProxy(t, (incoming, response) => {
    const chunks: Buffer[] = [];
    incoming.on("data", (chunk: Buffer) => chunks.push(chunk));
    incoming.on("end", () => {
      assert.equal(incoming.method, "POST");
      assert.equal(incoming.url, "http://upstream.invalid/v1/systemone?ak=query");
      assert.equal(incoming.headers.authorization, "Bearer bearer");
      assert.equal(
        incoming.headers["proxy-authorization"],
        `Basic ${Buffer.from("user:PRIVATE_PROXY_PASSWORD").toString("base64")}`,
      );
      assert.deepEqual(JSON.parse(Buffer.concat(chunks).toString()), { ...request, model: "test" });
      response.setHeader("Content-Type", "application/json");
      response.end(JSON.stringify({ answers: { intent: { type: "choice", choice: "yes" } } }));
    });
  });
  const url = new URL(proxy.url);
  url.username = "user";
  url.password = "PRIVATE_PROXY_PASSWORD";
  await Effect.runPromise(
    SystemOneClient.use((client) => client.systemOne(request)).pipe(
      Effect.tap((answer) => Effect.sync(() => assert.equal(answer.answers.intent?.choice, "yes"))),
      Effect.provide(
        Layer.mergeAll(
          transport({ ...config, proxy: url.toString() }),
          Logger.layer([Logger.make((entry) => logs.push(entry.message))]),
        ),
      ),
    ),
  );
  assert.doesNotMatch(JSON.stringify(logs), /PRIVATE_PROXY_PASSWORD|Bearer bearer|ak=query/);
});

test("HTTPS requests use CONNECT and release a pending tunnel on interruption and Layer shutdown", async (t) => {
  const entered = Deferred.makeUnsafe<void>();
  const closed = Deferred.makeUnsafe<void>();
  let attempts = 0;
  const proxy = await localProxy(t);
  proxy.server.on("connect", (incoming, socket) => {
    attempts++;
    assert.equal(incoming.url, "upstream.invalid:443");
    socket.on("close", () => Deferred.doneUnsafe(closed, Effect.void));
    // Upgraded server sockets are half-open; mirror the peer's FIN to finish closing.
    socket.on("end", () => socket.end());
    socket.resume();
    Deferred.doneUnsafe(entered, Effect.void);
  });
  await Effect.runPromise(
    SystemOneClient.use((client) =>
      Effect.gen(function* () {
        const fiber = yield* client.systemOne(request).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* Fiber.interrupt(fiber);
      }),
    ).pipe(
      Effect.scoped,
      Effect.provide(transport({ ...config, url: "https://upstream.invalid", proxy: proxy.url })),
      Effect.timeout("5 seconds"),
    ),
  );
  await Effect.runPromise(Deferred.await(closed).pipe(Effect.timeout("5 seconds")));
  assert.equal(attempts, 1);
});
