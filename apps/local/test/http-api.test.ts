import assert from "node:assert/strict";
import { test } from "node:test";
import { once } from "node:events";
import { Server } from "node:http";
import { createConnection } from "node:net";
import { Deferred, Effect, Option } from "effect";
import { NodeHttpServerRequest } from "@effect/platform-node";
import { HttpServerRequest } from "effect/http";
import { makeContextRegistry } from "@aster/core/testing";
import { startTestHttp, rpcRequest } from "./api-fixtures.js";

test(
  "HTTP shutdown closes requests arriving after handler removal",
  { timeout: 5000 },
  async () => {
    const registry = await Effect.runPromise(makeContextRegistry());
    const started = await Effect.runPromise(Deferred.make<Server>());
    const finalizing = await Effect.runPromise(Deferred.make<void>());
    const release = await Effect.runPromise(Deferred.make<void>());
    const api = await startTestHttp({
      port: 0,
      registry,
      inspect: Effect.gen(function* () {
        const request = yield* Effect.serviceOption(HttpServerRequest.HttpServerRequest);
        assert.ok(Option.isSome(request));
        const { socket } = NodeHttpServerRequest.toIncomingMessage(request.value);
        assert.ok("server" in socket && socket.server instanceof Server);
        yield* Deferred.succeed(started, socket.server);
        return yield* Effect.never;
      }).pipe(
        Effect.ensuring(
          Deferred.succeed(finalizing, undefined).pipe(Effect.andThen(Deferred.await(release))),
        ),
      ),
    });
    const abort = new AbortController();
    const response = fetch(`${api.url}/api/rpc`, {
      ...rpcRequest("InspectRuntime"),
      signal: abort.signal,
    })
      .then((r) => r.text())
      .catch(() => undefined);
    const server = await Effect.runPromise(Deferred.await(started));
    const socket = createConnection({ host: "127.0.0.1", port: Number(new URL(api.url).port) });
    let closing: Promise<void> | undefined;
    try {
      await once(socket, "connect");
      closing = api.close();
      await Effect.runPromise(Deferred.await(finalizing));
      assert.equal(server.listenerCount("request"), 0);
      // The listener is still open while admitted request finalizers drain. A browser
      // reconnect can reach this socket after the Effect request handler is removed.
      const incoming = once(server, "request");
      socket.write("GET /api/rpc HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
      await incoming;
      const closed = once(server, "close", { signal: AbortSignal.timeout(1000) });
      await Effect.runPromise(Deferred.succeed(release, undefined));
      await closed;
      await closing;
    } finally {
      socket.destroy();
      abort.abort();
      await Effect.runPromise(Deferred.succeed(release, undefined));
      await (closing ?? api.close());
      await response;
    }
  },
);

test(
  "HTTP shutdown interrupts active application Effects and waits for their finalizers",
  { timeout: 3000 },
  async () => {
    const registry = await Effect.runPromise(makeContextRegistry());
    const started = Promise.withResolvers<void>();
    let released = false;
    const api = await startTestHttp({
      port: 0,
      registry,
      inspect: Effect.sync(() => started.resolve()).pipe(
        Effect.andThen(Effect.never),
        Effect.ensuring(
          Effect.sleep(20).pipe(
            Effect.andThen(
              Effect.sync(() => {
                released = true;
              }),
            ),
          ),
        ),
      ),
    });
    const response = fetch(`${api.url}/api/rpc`, rpcRequest("InspectRuntime"))
      .then((r) => r.text())
      .catch(() => undefined);
    try {
      await started.promise;
    } finally {
      await api.close();
    }
    assert.equal(released, true);
    await response;
  },
);
