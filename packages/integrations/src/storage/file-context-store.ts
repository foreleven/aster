import {
  appendFileSync,
  closeSync,
  existsSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readFileSync,
  readdirSync,
  renameSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import type { ContextRecord, ContextStore } from "@aster/core";

/** File commits have a durable intent so the two public files recover as one record. */
export const makeFileContextStore = (root = join(homedir(), ".aster", "actors")): ContextStore => {
  root = resolve(root);
  mkdirSync(root, { recursive: true, mode: 0o700 });
  const directory = (path: string) => {
    if (
      !path.startsWith("/") ||
      path
        .slice(1)
        .split("/")
        .some((part) => !part || part === "." || part === ".." || /[\\\0]/.test(part))
    ) {
      throw new Error(`Invalid persistent Context path: ${path}`);
    }
    return join(root, path.slice(1));
  };
  const atomic = (path: string, contents: string) => {
    const temp = `${path}.${randomUUID()}.tmp`;
    const fd = openSync(temp, "wx", 0o600);
    try {
      writeFileSync(fd, contents);
      fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
    renameSync(temp, path);
  };
  const jsonl = (messages: ReadonlyArray<unknown>) =>
    messages.map((message) => JSON.stringify(message) + "\n").join("");
  const finish = (record: ContextRecord, previous?: ContextRecord) => {
    const dir = directory(record.path);
    const messagePath = join(dir, "messages.jsonl");
    const append =
      previous &&
      existsSync(messagePath) &&
      previous.messages.length <= record.messages.length &&
      previous.messages.every((message, index) =>
        isDeepStrictEqual(message, record.messages[index]),
      );
    if (append) {
      const fd = openSync(messagePath, "a", 0o600);
      try {
        appendFileSync(fd, jsonl(record.messages.slice(previous.messages.length)));
        fsyncSync(fd);
      } finally {
        closeSync(fd);
      }
    } else atomic(messagePath, jsonl(record.messages));
    atomic(
      join(dir, "state.json"),
      JSON.stringify(
        {
          path: record.path,
          description: record.description,
          state: record.state,
          messageCount: record.messages.length,
        },
        null,
        2,
      ) + "\n",
    );
    rmSync(join(dir, ".pending.json"), { force: true });
  };
  const cache = new Map<string, ContextRecord>();
  const loadAll = () => {
    cache.clear();
    const visit = (dir: string) => {
      const entries = readdirSync(dir, { withFileTypes: true });
      const pending = join(dir, ".pending.json");
      if (existsSync(pending)) {
        const record = JSON.parse(readFileSync(pending, "utf8")) as ContextRecord;
        if (directory(record.path) !== dir)
          throw new Error(`Context recovery path mismatch: ${dir}`);
        // Rewrite rather than append: a crash may have committed only part of the previous append.
        finish(record);
      }
      const statePath = join(dir, "state.json");
      if (existsSync(statePath)) {
        const stored = JSON.parse(readFileSync(statePath, "utf8")) as Omit<
          ContextRecord,
          "messages"
        > & { messageCount: number };
        if (directory(stored.path) !== dir) throw new Error(`Context state path mismatch: ${dir}`);
        const raw = readFileSync(join(dir, "messages.jsonl"), "utf8");
        const messages = raw
          .split("\n")
          .filter(Boolean)
          .map((line) => JSON.parse(line) as unknown);
        if (messages.length !== stored.messageCount)
          throw new Error(`Context message count mismatch: ${stored.path}`);
        cache.set(stored.path, {
          path: stored.path,
          description: stored.description,
          state: stored.state,
          messages,
        });
      }
      for (const entry of entries) if (entry.isDirectory()) visit(join(dir, entry.name));
    };
    visit(root);
    return structuredClone([...cache.values()]);
  };
  return {
    loadAll,
    save: (input) => {
      const record = structuredClone(input);
      const dir = directory(record.path);
      mkdirSync(dir, { recursive: true, mode: 0o700 });
      atomic(join(dir, ".pending.json"), JSON.stringify(record));
      finish(record, cache.get(record.path));
      cache.set(record.path, record);
    },
  };
};
