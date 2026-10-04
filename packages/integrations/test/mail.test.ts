import assert from "node:assert/strict";
import { createServer } from "node:net";
import { test } from "node:test";
import { ConfigProvider, Effect, Fiber, Layer, Redacted } from "effect";
import {
  mailFetcherLayer,
  MailFetcher,
  MailIntegration,
  MailSettings,
  MailFetchError,
  mailFailureDetails,
} from "../src/index.js";

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

test("MailIntegration.services provides configured fetcher without opening connections", async () => {
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
      Effect.provide(MailIntegration.services.pipe(Layer.provide(ConfigProvider.layer(provider)))),
    ),
  );
  assert.equal(typeof fetcher.pull, "function");
  assert.equal(typeof fetcher.pullAll, "function");
});

test("POP3 mailboxes normalize messages with stable UIDL identities across renumbering", async () => {
  let deletedFirst = false;
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
        else if (command === "UIDL")
          socket.write(
            deletedFirst
              ? "+OK\r\n1 stable-two\r\n.\r\n"
              : "+OK\r\n1 stable-one\r\n2 stable-two\r\n.\r\n",
          );
        else if (command === "STAT") socket.write(deletedFirst ? "+OK 1 50\r\n" : "+OK 2 100\r\n");
        else if (command === "RETR 1" && !deletedFirst)
          socket.write(
            "+OK\r\nFrom: sender@example.com\r\nTo: alice@example.com\r\nSubject: First\r\n\r\nHello one\r\n.\r\n",
          );
        else if (command === "RETR 2" || (command === "RETR 1" && deletedFirst))
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
    const pull = () =>
      Effect.runPromise(
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
    const messages = await pull();
    deletedFirst = true;
    const remaining = await pull();
    assert.equal(remaining[0]?.id, messages[1]?.id);
    assert.equal(remaining[0]?.id, "pop3:stable-two");
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

test("IMAP polls the latest messages and scopes stable UIDs to UIDVALIDITY", async () => {
  const requests: string[] = [];
  let validity = 12;
  let rejectLogin = false;
  const server = createServer((socket) => {
    socket.write("* OK [CAPABILITY IMAP4rev1] ready\r\n");
    let buffer = "";
    socket.on("data", (chunk) => {
      buffer += chunk.toString();
      while (buffer.includes("\r\n")) {
        const end = buffer.indexOf("\r\n");
        const line = buffer.slice(0, end);
        buffer = buffer.slice(end + 2);
        const [tag, command] = line.split(" ");
        requests.push(line);
        if (command === "CAPABILITY") socket.write(`* CAPABILITY IMAP4rev1\r\n${tag} OK done\r\n`);
        else if (command === "LOGIN")
          socket.write(
            rejectLogin
              ? `${tag} NO [AUTHENTICATIONFAILED] secret server response\r\n`
              : `${tag} OK authenticated\r\n`,
          );
        else if (command === "LIST") socket.write(`* LIST () "/" "INBOX"\r\n${tag} OK done\r\n`);
        else if (command === "SELECT")
          socket.write(
            `* FLAGS (\\Seen)\r\n* 100 EXISTS\r\n* OK [UIDVALIDITY ${validity}] valid\r\n* OK [UIDNEXT 101] next\r\n${tag} OK [READ-WRITE] selected\r\n`,
          );
        else if (command === "FETCH") {
          for (const seq of [99, 100]) {
            const source = `From: sender@example.com\r\nTo: alice@example.com\r\nSubject: Message ${seq}\r\n\r\nBody ${seq}\r\n`;
            socket.write(
              `* ${seq} FETCH (UID ${seq} BODY[] {${Buffer.byteLength(source)}}\r\n${source})\r\n`,
            );
          }
          socket.write(`${tag} OK fetched\r\n`);
        } else socket.write(`${tag} BAD unsupported test command\r\n`);
      }
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  try {
    const pull = () =>
      Effect.runPromise(
        Effect.gen(function* () {
          return yield* (yield* MailFetcher).pull({
            id: "work",
            host: "127.0.0.1",
            port: address.port,
            secure: false,
            username: "alice",
            password: Redacted.make("secret"),
            maxMessages: 2,
          });
        }).pipe(Effect.provide(mailFetcherLayer([])), Effect.timeout("5 seconds")),
      );
    const first = await pull();
    assert.deepEqual(
      first.map((email) => email.id),
      ["imap:INBOX:12:99", "imap:INBOX:12:100"],
    );
    assert.deepEqual(
      first.map((email) => email.subject),
      ["Message 99", "Message 100"],
    );
    assert.ok(requests.some((line) => / FETCH 99:100 /.test(line)));
    assert.deepEqual(
      (await pull()).map((email) => email.id),
      first.map((email) => email.id),
    );
    validity = 13;
    assert.notEqual((await pull())[0]?.id, first[0]?.id);
    rejectLogin = true;
    await assert.rejects(pull(), (error: unknown) => {
      assert.ok(error instanceof MailFetchError);
      assert.deepEqual(error.details, {
        stage: "authenticate",
        reason: "authentication",
        code: "AUTHENTICATIONFAILED",
        responseStatus: "NO",
      });
      assert.ok(!JSON.stringify(error.details).includes("secret"));
      return true;
    });
    rejectLogin = false;
    assert.equal((await pull()).length, 2);
  } finally {
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
});

for (const protocol of ["imap", "pop3"] as const)
  test(`${protocol} interruption closes a connection waiting for its greeting`, async () => {
    const accepted = Promise.withResolvers<void>();
    const closed = Promise.withResolvers<void>();
    const server = createServer((socket) => {
      socket.on("close", () => closed.resolve());
      accepted.resolve();
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    assert.ok(address && typeof address !== "string");
    const fiber = Effect.runFork(
      Effect.gen(function* () {
        return yield* (yield* MailFetcher).pull({
          id: "test",
          protocol,
          host: "127.0.0.1",
          port: address.port,
          secure: false,
          username: "u",
          password: Redacted.make("p"),
        });
      }).pipe(Effect.provide(mailFetcherLayer([]))),
    );
    try {
      await accepted.promise;
      await Effect.runPromise(Fiber.interrupt(fiber).pipe(Effect.timeout("5 seconds")));
      await Effect.runPromise(
        Effect.promise(() => closed.promise).pipe(Effect.timeout("5 seconds")),
      );
    } finally {
      await Effect.runPromise(Fiber.interrupt(fiber));
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    }
  });

test("mail diagnostics retain network and server causes wrapped as authentication failures", () => {
  for (const [code, reason] of [
    ["ECONNRESET", "network"],
    ["UNAVAILABLE", "server"],
  ]) {
    const details = mailFailureDetails(
      Object.assign(new Error("secret password and message body"), {
        code,
        authenticationFailed: true,
        responseText: "secret raw reply",
        executedCommand: "LOGIN secret",
      }),
      "connect",
    );
    assert.deepEqual(details, { stage: "authenticate", reason, code });
    assert.ok(!JSON.stringify(details).includes("secret"));
  }
  assert.deepEqual(
    mailFailureDetails(
      { code: "secret", responseStatus: "secret", responseText: "secret" },
      "fetch",
    ),
    { stage: "fetch", reason: "protocol" },
  );
});
