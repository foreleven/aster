import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { parseMailboxProfile, parseAccount, parseLarkConfig } from "@aster/integrations";
import { parseMemoryConfig } from "@aster/integrations";
import { loadConfig } from "@aster/integrations";
import { agentEnvironment } from "@aster/integrations";

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
    when: |
      A new email requests a review.
      Consider the chat context too.
    task: Review
    agent: test
    mode: confirm
`,
    );
    const config = loadConfig(file);
    assert.deepEqual(config.config["system-one"], {
      url: "http://localhost:8000/v1/systemone",
      model: "multilingual",
      apiKey: "literal-test-key",
    });
    const lark = parseLarkConfig(config.contexts["/lark"]);
    const memory = parseMemoryConfig(config.contexts["/memory"], config.baseDir);
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
    assert.match(config.signals[0]!.when, /review\.\nConsider/);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("loader preserves opaque root configs; integrations own children and defaults", async () => {
  const dir = await mkdtemp(join(tmpdir(), "signals-root-config-test-"));
  try {
    const file = join(dir, "signals.yaml");
    await writeFile(
      file,
      "contexts:\n  /custom:\n    children:\n      /anything: {private: value}\n",
    );
    const config = loadConfig(file);
    assert.deepEqual(config.contexts["/custom"], {
      children: { "/anything": { private: "value" } },
    });
    assert.deepEqual(config.signals, []);
    assert.deepEqual(config.config, {});
    assert.equal(parseLarkConfig(config.contexts["/lark"]).mail.mailbox, "me");
    assert.equal(
      parseMemoryConfig(config.contexts["/memory"], config.baseDir).description,
      "My long-term memory",
    );
    // The central loader does not validate a Lark-owned child's configuration.
    await writeFile(
      file,
      "contexts:\n  /lark:\n    children:\n      /mail:\n        config: {pollIntervalMs: bad}\n",
    );
    const invalid = loadConfig(file);
    assert.throws(() => parseLarkConfig(invalid.contexts["/lark"]));
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
    TYPESAFE_API_KEY: "test",
    LAYA_API_KEY: "test",
    AGENTMEMORY_SECRET: "test",
    PATH: "/bin",
  });
  assert.equal(env.TYPESAFE_API_KEY, undefined);
  assert.equal(env.LAYA_API_KEY, undefined);
  assert.equal(env.AGENTMEMORY_SECRET, undefined);
  assert.equal(env.PATH, "/bin");
});
