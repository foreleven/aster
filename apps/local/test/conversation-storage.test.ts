import assert from "node:assert/strict";
import { mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { Effect, Layer } from "effect";
import { AgentConversations } from "@aster/agent/harness";
import { LocalConfig, storageSettings, withActorStoreLock } from "@aster/infra";
import { localConversationsLayer } from "../src/services.js";

test("conversations resolve beside config under the locked root and reopen without Pi leases", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "aster-conversation-config-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const configPath = join(directory, "config.yaml");
  await writeFile(configPath, JSON.stringify({ config: { durable: { root: "data" } } }));
  const sources = LocalConfig.layer({
    configPath,
    projectRoot: "/unused-project",
    envPath: join(directory, ".env"),
    environment: {},
  });
  const root = (await Effect.runPromise(storageSettings.pipe(Effect.provide(sources)))).root;
  assert.equal(root, join(directory, "data"));
  const conversations = localConversationsLayer.pipe(Layer.provide(sources));
  const entry = await Effect.runPromise(
    withActorStoreLock(
      root,
      Effect.gen(function* () {
        const messages = yield* AgentConversations;
        return yield* messages.append("/goals/a", "one", "goal.input", { text: "Hello" });
      }).pipe(Effect.provide(conversations)),
    ),
  );
  const entries = await Effect.runPromise(
    withActorStoreLock(
      root,
      Effect.gen(function* () {
        return yield* (yield* AgentConversations).read("/goals/a");
      }).pipe(Effect.provide(conversations)),
    ),
  );
  assert.deepEqual(entries, [entry]);
  const owners = await readdir(join(root, "conversations"));
  assert.equal(owners.length, 1);
  const files = await readdir(join(root, "conversations", owners[0]));
  assert.ok(files.length > 0);
  assert.equal(
    files.some((file) => file.startsWith(".aster-owner")),
    false,
  );
});
