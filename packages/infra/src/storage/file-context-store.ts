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
import { dirname, join, resolve } from "node:path";
import { randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { StoredContext, ContextSnapshot } from "@aster/core";
import { Schema } from "effect";

export interface ContextStore {
  readonly loadAll: () => ReadonlyArray<StoredContext>;
  readonly save: (record: StoredContext) => void;
}

const StateFile = Schema.Struct({
  snapshot: Schema.Struct({
    path: ContextSnapshot.fields.path,
    revision: ContextSnapshot.fields.revision,
    description: ContextSnapshot.fields.description,
    state: ContextSnapshot.fields.state,
  }),
  events: StoredContext.fields.events,
  messageCount: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
});
const decodePending = Schema.decodeUnknownSync(Schema.fromJsonString(StoredContext));
const decodeState = Schema.decodeUnknownSync(Schema.fromJsonString(StateFile));
const decodeMessage = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

// Native fsync is required for the existing synchronous store contract. Persist
// directory entries as well as file contents before acknowledging a commit.
const syncDirectory = (directory: string) => {
  const fd = openSync(directory, "r");
  try {
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
};
const ensureDirectory = (directory: string) => {
  const firstCreated = mkdirSync(directory, { recursive: true, mode: 0o700 });
  if (firstCreated === undefined) return;
  const parent = dirname(firstCreated);
  for (let current = directory; ; current = dirname(current)) {
    syncDirectory(current);
    if (current === parent) break;
  }
};

/** File commits have a durable intent so the two public files recover as one record. */
export const makeFileContextStore = (root = join(homedir(), ".aster", "actors")): ContextStore => {
  root = resolve(root);
  ensureDirectory(root);
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
    syncDirectory(dirname(path));
  };
  const jsonl = (messages: ReadonlyArray<unknown>) =>
    messages.map((message) => JSON.stringify(message) + "\n").join("");
  const finish = (stored: StoredContext, prior?: StoredContext) => {
    const record = stored.snapshot;
    const previous = prior?.snapshot;
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
          snapshot: {
            path: record.path,
            revision: record.revision,
            description: record.description,
            state: record.state,
          },
          events: stored.events,
          messageCount: record.messages.length,
        },
        null,
        2,
      ) + "\n",
    );
    rmSync(join(dir, ".pending.json"), { force: true });
    syncDirectory(dir);
  };
  const cache = new Map<string, StoredContext>();
  const loadAll = () => {
    cache.clear();
    const visit = (dir: string) => {
      const entries = readdirSync(dir, { withFileTypes: true });
      const pending = join(dir, ".pending.json");
      if (existsSync(pending)) {
        const record = decodePending(readFileSync(pending, "utf8"));
        if (directory(record.snapshot.path) !== dir)
          throw new Error(`Context recovery path mismatch: ${dir}`);
        // Rewrite rather than append: a crash may have committed only part of the previous append.
        finish(record);
      }
      const statePath = join(dir, "state.json");
      if (existsSync(statePath)) {
        const stored = decodeState(readFileSync(statePath, "utf8"));
        if (directory(stored.snapshot.path) !== dir)
          throw new Error(`Context state path mismatch: ${dir}`);
        const raw = readFileSync(join(dir, "messages.jsonl"), "utf8");
        const messages = raw
          .split("\n")
          .filter(Boolean)
          .map((line) => decodeMessage(line));
        if (messages.length !== stored.messageCount)
          throw new Error(`Context message count mismatch: ${stored.snapshot.path}`);
        cache.set(
          stored.snapshot.path,
          Schema.decodeUnknownSync(StoredContext)({
            snapshot: { ...stored.snapshot, messages },
            events: stored.events,
          }),
        );
      }
      for (const entry of entries) if (entry.isDirectory()) visit(join(dir, entry.name));
    };
    visit(root);
    return structuredClone([...cache.values()]);
  };
  return {
    loadAll,
    save: (input) => {
      const record = Schema.decodeUnknownSync(StoredContext)(structuredClone(input));
      const dir = directory(record.snapshot.path);
      ensureDirectory(dir);
      atomic(join(dir, ".pending.json"), JSON.stringify(record));
      finish(record, cache.get(record.snapshot.path));
      cache.set(record.snapshot.path, record);
    },
  };
};
