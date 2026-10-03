import assert from "node:assert/strict";
import { createServer } from "node:net";
import { test } from "node:test";
import { ConfigProvider, Effect, Layer, Redacted } from "effect";
import { mailFetcherLayer, MailFetcher, MailIntegration, MailSettings } from "../src/index.js";

test("mail settings decode multiple mailboxes and redact passwords", async () => {
  const provider = ConfigProvider.fromUnknown({
    contexts: {
      "/mail": {
        config: {
          mailboxes: [
            { id: "work", host: "imap.example.com", username: "alice", password: "secret" },
            {
              id: "personal",
              protocol: "pop3",
              host: "pop.example.net",
              username: "alice",
              password: "secret-2",
              secure: false,
            },
          ],
        },
      },
    },
  });
  const settings = await Effect.runPromise(
    Effect.gen(function* () {
      return yield* MailSettings;
    }).pipe(Effect.provide(MailSettings.layer.pipe(Layer.provide(ConfigProvider.layer(provider))))),
  );
  assert.equal(settings.mailboxes.length, 2);
  assert.equal(Redacted.value(settings.mailboxes[0]!.password), "secret");
  assert.equal(settings.mailboxes[1]!.secure, false);
  assert.equal(settings.mailboxes[1]!.protocol, "pop3");
});

test("MailIntegration.layer provides configured fetcher without opening connections", async () => {
  const provider = ConfigProvider.fromUnknown({
    contexts: {
      "/mail": {
        config: {
          mailboxes: [
            { id: "work", host: "imap.example.com", username: "alice", password: "secret" },
          ],
        },
      },
    },
  });
  const fetcher = await Effect.runPromise(
    Effect.gen(function* () {
      return yield* MailFetcher;
    }).pipe(
      Effect.provide(MailIntegration.layer.pipe(Layer.provide(ConfigProvider.layer(provider)))),
    ),
  );
  assert.equal(typeof fetcher.pull, "function");
  assert.equal(typeof fetcher.pullAll, "function");
});

test("POP3 mailboxes use node-pop3 and normalize retrieved messages", async () => {
  const server = createServer((socket) => {
    socket.write("+OK ready\r\n");
    let buffer = "";
    socket.on("data", (chunk) => {
      buffer += chunk.toString();
      while (buffer.includes("\r\n")) {
        const end = buffer.indexOf("\r\n");
        const command = buffer.slice(0, end);
        buffer = buffer.slice(end + 2);
        if (command.startsWith("USER ")) socket.write("+OK user\r\n");
        else if (command.startsWith("PASS ")) socket.write("+OK pass\r\n");
        else if (command === "STAT") socket.write("+OK 2 100\r\n");
        else if (command === "RETR 1")
          socket.write(
            "+OK\r\nFrom: sender@example.com\r\nTo: alice@example.com\r\nSubject: First\r\n\r\nHello one\r\n.\r\n",
          );
        else if (command === "RETR 2")
          socket.write(
            "+OK\r\nFrom: sender@example.com\r\nTo: alice@example.com\r\nSubject: Second\r\n\r\nHello two\r\n.\r\n",
          );
        else if (command === "QUIT") {
          socket.write("+OK bye\r\n");
          socket.end();
        }
      }
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  try {
    const messages = await Effect.runPromise(
      Effect.gen(function* () {
        const fetcher = yield* MailFetcher;
        return yield* fetcher.pull({
          id: "test-pop3",
          protocol: "pop3",
          host: "127.0.0.1",
          port: address.port,
          secure: false,
          username: "alice",
          password: Redacted.make("secret"),
        });
      }).pipe(Effect.provide(mailFetcherLayer([]))),
    );
    assert.deepEqual(
      messages.map(({ subject, text }) => ({ subject, text })),
      [
        { subject: "First", text: "Hello one" },
        { subject: "Second", text: "Hello two" },
      ],
    );
  } finally {
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
});
