import { createHash, randomUUID } from "node:crypto";
import {
  closeSync,
  existsSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readFileSync,
  readdirSync,
  renameSync,
  writeFileSync,
} from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { Context, Layer, Schema } from "effect";
import type { ImBatch } from "./client.js";
import { ImChat, ImMessage } from "./model.js";
import { ChatSummary } from "./summarizer.js";
import { IM_TIME_ZONE, imDate, imDayStart, nextImDay } from "./dates.js";

export const messageFingerprint = (message: ImMessage) =>
  createHash("sha256").update(JSON.stringify(message)).digest("hex");
export interface SummaryCommit {
  readonly batch: readonly ImMessage[];
  readonly daily: ChatSummary;
  readonly rolling: ChatSummary;
  readonly evaluate: boolean;
  readonly updatedAt: string;
}
export interface ChatDay {
  version: 1;
  date: string;
  chat: ImChat;
  pending: ImMessage[];
  seen: Record<string, { fingerprint: string; at: string }>;
  summary?: ChatSummary;
  updatedAt?: string;
  commit?: SummaryCommit;
  assessment?: { fingerprint: string; needed: boolean };
  stage?: { batch: readonly ImMessage[]; daily?: ChatSummary };
  flush?: boolean;
  lastError?: string;
  retryAt?: string;
}
export interface RetrievalProgress {
  version: 1;
  date: string;
  timeZone: typeof IM_TIME_ZONE;
  /** Successful intervals, including empty search results; gaps remain explicit. */
  intervals: { from: string; through: string }[];
  through?: string;
}
const SummaryCommitSchema = Schema.Struct({
  batch: Schema.Array(ImMessage),
  daily: ChatSummary,
  rolling: ChatSummary,
  evaluate: Schema.Boolean,
  updatedAt: Schema.String,
});
const ChatDaySchema = Schema.Struct({
  version: Schema.Literal(1),
  date: Schema.String,
  chat: ImChat,
  pending: Schema.Array(ImMessage).pipe(Schema.mutable),
  seen: Schema.Record(
    Schema.String,
    Schema.Struct({ fingerprint: Schema.String, at: Schema.String }),
  ),
  summary: Schema.optional(ChatSummary),
  updatedAt: Schema.optional(Schema.String),
  commit: Schema.optional(SummaryCommitSchema),
  assessment: Schema.optional(
    Schema.Struct({ fingerprint: Schema.String, needed: Schema.Boolean }),
  ),
  stage: Schema.optional(
    Schema.Struct({ batch: Schema.Array(ImMessage), daily: Schema.optional(ChatSummary) }),
  ),
  flush: Schema.optional(Schema.Boolean),
  lastError: Schema.optional(Schema.String),
  retryAt: Schema.optional(Schema.String),
});
const RetrievalProgressSchema = Schema.Struct({
  version: Schema.Literal(1),
  date: Schema.String,
  timeZone: Schema.Literal(IM_TIME_ZONE),
  intervals: Schema.Array(Schema.Struct({ from: Schema.String, through: Schema.String })).pipe(
    Schema.mutable,
  ),
  through: Schema.optional(Schema.String),
});

const atomic = (path: string, value: string) => {
  mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
  const temp = `${path}.${randomUUID()}.tmp`;
  const fd = openSync(temp, "wx", 0o600);
  try {
    writeFileSync(fd, value);
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
  renameSync(temp, path);
  const dir = openSync(dirname(path), "r");
  try {
    fsyncSync(dir);
  } finally {
    closeSync(dir);
  }
};
const read = <T>(path: string, schema: Schema.ConstraintDecoder<T>): T | undefined =>
  existsSync(path)
    ? Schema.decodeUnknownSync(schema)(JSON.parse(readFileSync(path, "utf8")))
    : undefined;
const json = (path: string, value: unknown) => atomic(path, JSON.stringify(value, null, 2) + "\n");
const chatKey = (id: string) => {
  if (!/^[a-zA-Z0-9_-]+$/.test(id)) throw new Error("Invalid IM chat ID");
  return id;
};

/** Private ingress journal and summary checkpoints, separate from public Contexts. */
export const makeImStorage = (root = join(homedir(), ".aster", "im")) => {
  const dayDir = (date: string) => {
    imDayStart(date);
    return join(root, date);
  };
  const statePath = (date: string, id: string) =>
    join(dayDir(date), "chats", `${chatKey(id)}.json`);
  const get = (date: string, id: string) => read<ChatDay>(statePath(date, id), ChatDaySchema);
  const save = (day: ChatDay) => json(statePath(day.date, day.chat.id), day);
  const progress = (date: string) =>
    read<RetrievalProgress>(join(dayDir(date), "progress.json"), RetrievalProgressSchema);
  const list = (date: string) => {
    const dir = join(dayDir(date), "chats");
    return existsSync(dir)
      ? readdirSync(dir)
          .filter((name) => name.endsWith(".json"))
          .map((name) => read<ChatDay>(join(dir, name), ChatDaySchema)!)
      : [];
  };
  const ingest = (batch: ImBatch, legacy = false) => {
    const days = new Map<string, ChatDay>();
    for (const message of batch.messages) {
      const date = imDate(message.at);
      const day = days.get(date) ??
        get(date, batch.chat.id) ?? {
          version: 1 as const,
          date,
          chat: batch.chat,
          pending: [],
          seen: {},
        };
      day.chat = batch.chat;
      const fingerprint = messageFingerprint(message);
      const index = day.pending.findIndex((value) => value.id === message.id);
      if (legacy && (index >= 0 || day.seen[message.id])) continue;
      if (index >= 0) day.pending[index] = message;
      else if (day.seen[message.id]?.fingerprint !== fingerprint) day.pending.push(message);
      day.pending.sort((a, b) => Date.parse(a.at) - Date.parse(b.at));
      days.set(date, day);
    }
    for (const day of days.values()) save(day);
    return [...days.keys()];
  };
  const markRetrieved = (from: string, through: string) => {
    const end = Date.parse(through);
    for (let start = Date.parse(from); start < end;) {
      const date = imDate(start);
      const next = Math.min(imDayStart(nextImDay(date)), end);
      const day = progress(date) ?? {
        version: 1 as const,
        date,
        timeZone: IM_TIME_ZONE,
        intervals: [],
      };
      const intervals = [
        ...day.intervals.map((i) => [Date.parse(i.from), Date.parse(i.through)]),
        [start, next],
      ].sort((a, b) => a[0] - b[0]);
      const merged: number[][] = [];
      for (const interval of intervals) {
        const last = merged.at(-1);
        if (last && interval[0] <= last[1]) last[1] = Math.max(last[1], interval[1]);
        else merged.push([...interval]);
      }
      day.intervals = merged.map(([a, b]) => ({
        from: new Date(a).toISOString(),
        through: new Date(b).toISOString(),
      }));
      day.through = day.intervals.at(-1)!.through;
      json(join(dayDir(date), "progress.json"), day);
      start = next;
    }
  };
  const archive = (day: ChatDay, commit: SummaryCommit) => {
    const seen = { ...day.seen };
    for (const message of commit.batch)
      seen[message.id] = { fingerprint: messageFingerprint(message), at: message.at };
    const times = Object.values(seen)
      .map((value) => value.at)
      .sort((a, b) => Date.parse(a) - Date.parse(b));
    const fields = {
      date: day.date,
      timezone: IM_TIME_ZONE,
      chat_id: day.chat.id,
      chat_name: day.chat.name,
      chat_mode: day.chat.mode,
      updated_at: commit.updatedAt,
      first_message_at: times[0],
      last_message_at: times.at(-1),
      message_count: times.length,
    };
    const frontmatter = Object.entries(fields)
      .map(([key, value]) => `${key}: ${JSON.stringify(value)}`)
      .join("\n");
    const references = commit.daily.references.length
      ? "\n\n## Message references\n\n" +
        commit.daily.references
          .map((ref) => `- ${ref.id}${ref.url ? ` — ${ref.url}` : ""}`)
          .join("\n")
      : "";
    atomic(
      join(dayDir(day.date), `${chatKey(day.chat.id)}.md`),
      `---\n${frontmatter}\n---\n\n${commit.daily.text}${references}\n`,
    );
  };
  const finish = (date: string, id: string, commit: SummaryCommit) => {
    const day = get(date, id)!;
    for (const message of commit.batch)
      day.seen[message.id] = { fingerprint: messageFingerprint(message), at: message.at };
    day.pending = day.pending.filter(
      (message) => day.seen[message.id]?.fingerprint !== messageFingerprint(message),
    );
    day.summary = commit.daily;
    day.updatedAt = commit.updatedAt;
    delete day.commit;
    delete day.stage;
    delete day.assessment;
    delete day.lastError;
    delete day.retryAt;
    save(day);
  };
  const lastAgentStart = () =>
    read(
      join(root, "agent-admission.json"),
      Schema.Struct({ lastStart: Schema.Number.check(Schema.isFinite()) }),
    )?.lastStart;
  const saveAgentStart = (lastStart: number) =>
    json(join(root, "agent-admission.json"), { lastStart });
  return {
    lastAgentStart,
    saveAgentStart,
    root,
    get,
    save,
    list,
    ingest,
    progress,
    markRetrieved,
    archive,
    finish,
  };
};
export class ImStorage extends Context.Service<ImStorage, ReturnType<typeof makeImStorage>>()(
  "lark/ImStorage",
) {
  static readonly layer = Layer.sync(ImStorage, () => makeImStorage());
}
