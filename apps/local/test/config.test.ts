import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { parseMailboxProfile, parseAccount, LarkConfig } from "@aster/integrations";
import { parseMemoryConfig, memorySettings } from "@aster/infra";
import { LocalConfig } from "@aster/infra";
import { Config, Effect, Schema } from "effect";
import { signalSettings } from "@aster/core";

const settings = (file: string) =>
  Effect.runPromise(
    Effect.gen(function* () {
      return {
        lark: yield* LarkConfig.pipe(Effect.provide(LarkConfig.layer)),
        memory: yield* memorySettings,
        custom: yield* Config.schema(Schema.optional(Schema.String), [
          "contexts",
          "/custom",
          "children",
          "/anything",
          "private",
        ]),
        systemOne: yield* Config.schema(
          Schema.optional(
            Schema.Struct({ url: Schema.String, model: Schema.String, apiKey: Schema.String }),
          ),
          ["config", "system-one"],
        ),
        signals: yield* signalSettings,
      };
    }).pipe(
      Effect.provide(
        LocalConfig.layer({
          configPath: file,
          envPath: `${file}.env`,
          projectRoot: "/tmp",
          environment: {},
        }),
      ),
    ),
  );
import { agentEnvironment } from "@aster/infra";

test("YAML preserves descriptions and multiline prompts and resolves dataDir beside config", async () => {
  const dir = await mkdtemp(join(tmpdir(), "signals-config-test-"));
  try {
    const file = join(dir, "signals.yaml");
    await writeFile(
      file,
      `
config:
  system-one:
    url: http://localhost:8000/v1/systemone
    model: multilingual
    apiKey: literal-test-key
contexts:
  /lark:
    description: My work account
    config: {profile: test}
    children:
      /mail:
        description: My work mailbox
        config: {mailbox: me}
signals:
  review:
    trigger:
      _tag: Context
      when: |
        A new email requests a review.
        Consider the chat context too.
    task: {_tag: Goal, target: /goals/personal, text: Review}
`,
    );
    const config = await settings(file);
    assert.deepEqual(config.systemOne, {
      url: "http://localhost:8000/v1/systemone",
      model: "multilingual",
      apiKey: "literal-test-key",
    });
    const lark = config.lark;
    const memory = config.memory;
    assert.equal(lark.description, "My work account");
    assert.equal(lark.mail.description, "My work mailbox");
    assert.equal(memory.dataDir, join(homedir(), ".aster/memory"));
    assert.equal(
      parseMemoryConfig({ config: { dataDir: "~/.aster/memory" } }, dir).dataDir,
      join(homedir(), ".aster/memory"),
    );
    assert.equal(
      parseMemoryConfig({ config: { dataDir: "custom/memory" } }, dir).dataDir,
      join(dir, "custom/memory"),
    );
    assert.equal(memory.autoCompress, false);
    const trigger = config.signals[0]!.trigger;
    assert.equal(trigger._tag, "Context");
    if (trigger._tag === "Context") assert.match(trigger.when, /review\.\nConsider/);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("module settings own children, defaults and validation", async () => {
  const dir = await mkdtemp(join(tmpdir(), "signals-root-config-test-"));
  try {
    const file = join(dir, "signals.yaml");
    await writeFile(
      file,
      "contexts:\n  /custom:\n    children:\n      /anything: {private: value}\n",
    );
    const config = await settings(file);
    assert.equal(config.custom, "value");
    assert.deepEqual(config.signals, []);
    assert.equal(config.systemOne, undefined);
    assert.equal(config.lark.mail.mailbox, "me");
    assert.equal(config.memory.description, "My long-term memory");
    // The Lark module validates its own child configuration through the provider.
    await writeFile(
      file,
      "contexts:\n  /lark:\n    children:\n      /mail:\n        config: {pollIntervalMs: bad}\n",
    );
    await assert.rejects(settings(file));
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("Lark profile parsers match actual CLI envelopes and select only public identity fields", () => {
  assert.deepEqual(
    parseMailboxProfile('{"ok":true,"data":{"primary_email_address":"test@example.com"}}'),
    {
      address: "test@example.com",
      name: "",
    },
  );
  assert.deepEqual(
    parseAccount(
      '{"ok":true,"data":{"user":{"name":"Test","open_id":"ou_test","access_token":"private"}}}',
    ),
    {
      openId: "ou_test",
      name: "Test",
      email: "",
      enterpriseEmail: "",
    },
  );
});

test("Agent processes receive none of the project service keys", () => {
  const env = agentEnvironment({
    privateKeys: ["PROJECT_SECRET"],
    values: {
      PROJECT_SECRET: "private",
      ASTER_HTTP_PORT: "3000",
      TYPESAFE_API_KEY: "test",
      LAYA_API_KEY: "test",
      AGENTMEMORY_SECRET: "test",
      PATH: "/bin",
    },
  });
  assert.equal(env.PROJECT_SECRET, undefined);
  assert.equal(env.ASTER_HTTP_PORT, undefined);
  assert.equal(env.TYPESAFE_API_KEY, undefined);
  assert.equal(env.LAYA_API_KEY, undefined);
  assert.equal(env.AGENTMEMORY_SECRET, undefined);
  assert.equal(env.PATH, "/bin");
});
