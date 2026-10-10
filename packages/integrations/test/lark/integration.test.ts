import assert from "node:assert/strict";
import { test } from "node:test";
import { AgentRunner } from "@aster/agent/agent";
import { Models } from "@aster/agent";
import { ProcessEnvironment, SystemOneClient } from "@aster/core";
import { Context, Effect, Layer, Option } from "effect";
import { ChildProcessSpawner } from "effect/process";
import {
  LarkIntegration,
  LarkAccountClient,
  LarkImClient,
  LarkMailClient,
} from "../../src/lark/index.js";
import { ChatSummarizer } from "../../src/lark/im/summary/summarizer.js";
import { SummaryAdmission } from "../../src/lark/im/summary/admission.js";
import { installLark } from "./fixtures.js";
import { larkSettings } from "../../src/lark/testing.js";
import { ConfigProvider } from "effect";

test("absent Mail/IM acquire no branch services and execute no process or model", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const services = yield* Layer.build(LarkIntegration.services).pipe(
          Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({})),
          Effect.provideService(ProcessEnvironment, { values: {}, privateKeys: [] }),
          Effect.provideService(
            ChildProcessSpawner.ChildProcessSpawner,
            ChildProcessSpawner.make(() => Effect.die("Unexpected process acquisition")),
          ),
          Effect.provideService(SystemOneClient, {
            configured: false,
            systemOne: () => Effect.die("Unexpected gate acquisition"),
          }),
          Effect.provideService(
            AgentRunner,
            AgentRunner.make(() => Effect.die("Unexpected model execution")),
          ),
          Effect.provide(Models.layer([])),
        );
        assert.ok(Option.isSome(Context.getOption(services, LarkAccountClient)));
        assert.ok(Option.isNone(Context.getOption(services, LarkMailClient)));
        assert.ok(Option.isNone(Context.getOption(services, LarkImClient)));
        assert.ok(Option.isNone(Context.getOption(services, ChatSummarizer)));
        assert.ok(Option.isNone(Context.getOption(services, SummaryAdmission)));
      }),
    ),
  );
});

test("activation completes without business catch-up and disabled capabilities create no Actors or Contexts", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const { registry, system, handle, captures } = yield* installLark({
          settings: larkSettings({}),
          account: { getAccount: () => Effect.never },
        });
        assert.deepEqual(
          registry.backend.directory().map(({ path }) => path),
          ["/lark"],
        );
        assert.deepEqual(
          (yield* system.inspect()).map(({ path }) => path),
          ["/user/lark"],
        );
        assert.equal(
          captures.select({
            path: "/lark/mail",
            revision: 1,
            description: "Mail",
            text: "profile",
          }),
          undefined,
        );
        yield* handle.stop;
      }),
    ).pipe(Effect.timeout("2 seconds")),
  );
});

import { Deferred } from "effect";
import { ImState } from "../../src/lark/im/model.js";
import { MailboxState } from "../../src/lark/mail/model.js";

test("retired polling, handoff and profile completions cannot mutate a fresh Behavior", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        let polls = 0;
        const { system, queries, registry } = yield* installLark({
          im: {
            searchMessages: () =>
              Effect.sync(() => {
                polls++;
              }).pipe(
                Effect.andThen(Deferred.succeed(entered, undefined)),
                Effect.andThen(Effect.never),
              ),
            getChatSettings: () => Effect.succeed([]),
            listMessages: () => Effect.die("Unexpected history"),
          },
          mail: {
            getMailboxProfile: () => Effect.never,
            listIds: () => Effect.never,
            getMessages: () => Effect.die("Unexpected mail handoff"),
            listMessages: () => Effect.die("Unexpected mail query"),
          },
        });
        yield* Deferred.await(entered);
        const im = yield* system.select("/user/lark/im").resolve();
        const root = yield* system.select("/user/lark").resolve();
        const mail = yield* system.select("/user/lark/mail").resolve();
        const result = {
          start: "2026-10-09T16:00:00Z",
          through: "2099-01-01T00:00:00Z",
          caughtUp: true,
          batches: [
            { chat: { id: "retired", name: "Retired", mode: "", description: "" }, messages: [] },
          ],
        };
        yield* im.tell({ _tag: "Poll", generation: "retired" });
        yield* im.tell({
          _tag: "Polled",
          generation: "retired",
          result: { _tag: "Success", value: result },
        });
        yield* im.tell({
          _tag: "HandedOff",
          generation: "retired",
          value: result,
          result: { _tag: "Success", value: undefined },
        });
        yield* mail.tell({
          _tag: "Published",
          generation: "retired",
          window: { start: 0, through: 1000, caughtUp: true },
          result: { _tag: "Success", value: undefined },
        });
        yield* mail.tell({
          _tag: "ProfileLoaded",
          generation: "retired",
          result: { _tag: "Success", value: { address: "retired@example.com", name: "Retired" } },
        });
        yield* root.tell({
          _tag: "AccountLoaded",
          generation: "retired",
          result: {
            _tag: "Success",
            value: { openId: "retired", name: "Retired", email: "", enterpriseEmail: "" },
          },
        });
        assert.deepEqual(
          yield* queries.query({ path: "/lark/im", command: "list_chats", args: {} }),
          { items: [], total: 0 },
        );
        assert.equal((yield* registry.backend.readState("/lark/im", ImState)).through, undefined);
        assert.equal(polls, 1);
        assert.equal(
          (yield* queries
            .query({ path: "/lark/mail", command: "profile", args: {} })
            .pipe(Effect.result))._tag,
          "Failure",
        );
        assert.equal(
          (yield* queries
            .query({ path: "/lark", command: "profile", args: {} })
            .pipe(Effect.result))._tag,
          "Failure",
        );
        assert.equal(
          (yield* registry.backend.readState("/lark/mail", MailboxState)).through,
          undefined,
        );
        assert.equal(
          registry.backend.directory().filter(({ path }) => path.includes("/chats/")).length,
          0,
        );
      }),
    ),
  );
});
