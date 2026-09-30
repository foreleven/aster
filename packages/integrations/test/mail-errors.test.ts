import assert from "node:assert/strict";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { Cause, Effect, Exit, Option, Result } from "effect";
import { liveCli, LarkResponseError } from "../src/index.js";

test("mail response errors remain typed failures for polling retries", async () => {
  const dir = await mkdtemp(join(tmpdir(), "aster-mail-error-"));
  const originalPath = process.env.PATH;
  try {
    process.env.PATH = `${dir}:${originalPath}`;
    for (const body of [
      '{"ok":false,"error":{"message":"upstream unavailable"}}',
      "invalid JSON",
      "{}",
    ]) {
      await writeFile(
        join(dir, "lark-cli"),
        `#!/usr/bin/env node\nprocess.stdout.write(${JSON.stringify(body)});\n`,
        { mode: 0o700 },
      );
      const cli = liveCli();
      const operations: Effect.Effect<unknown, Error>[] = [
        cli.getMailboxProfile("me"),
        cli.listRecentIds("me"),
        cli.getMessages("me", ["id"]),
      ];
      for (const operation of operations) {
        const exit = await Effect.runPromiseExit(operation);
        assert.ok(Exit.isFailure(exit));
        assert.ok(Result.isFailure(Cause.findDefect(exit.cause)));
        const error = Cause.findErrorOption(exit.cause);
        assert.ok(Option.isSome(error));
        assert.ok(error.value instanceof LarkResponseError);
      }
    }
  } finally {
    process.env.PATH = originalPath;
    await rm(dir, { recursive: true, force: true });
  }
});
