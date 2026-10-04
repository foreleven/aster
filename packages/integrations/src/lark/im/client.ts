import { Context, Effect, Layer } from "effect";
import { LarkConfig } from "../config.js";
import { runLarkCli } from "../shared/cli.js";
import { object, string, parseCliOutput } from "../shared/response.js";
import type { ImChat, ImMessage } from "./model.js";

export const parseImPage = (stdout: string, kind: "messages" | "chats" = "messages") => {
  const data = parseCliOutput(stdout);
  if (!Array.isArray(data[kind])) throw new Error(`Lark IM response has no ${kind}`);
  const next = data.has_more === true ? string(data.page_token) : undefined;
  if (data.has_more === true && !next) throw new Error("Lark IM pagination token missing");
  return { items: data[kind].map(object), next };
};
export const messageTime = (value: unknown): number => {
  const parsed =
    typeof value === "number"
      ? value
      : typeof value === "string" && /^\d+$/.test(value)
        ? Number(value)
        : Date.parse(string(value));
  return Number.isFinite(parsed) && Math.abs(parsed) < 100_000_000_000 ? parsed * 1000 : parsed;
};
export interface ImBatch {
  readonly chat: ImChat;
  readonly messages: readonly ImMessage[];
}

/** Search first, then resolve notification settings only for chats represented in this window. */
export const makeImClient = (run: (args: string[], signal?: AbortSignal) => Promise<string>) => ({
  recent: async (start: string, end: string, signal?: AbortSignal): Promise<ImBatch[]> => {
    const from = Date.parse(start),
      through = Date.parse(end);
    if (!Number.isFinite(from) || !Number.isFinite(through) || from >= through)
      throw new Error("Invalid IM query window");
    const grouped = new Map<string, { chat: ImChat; messages: Map<string, ImMessage> }>();
    const tokens = new Set<string>();
    let token: string | undefined;
    let pages = 0;
    do {
      const page = parseImPage(
        await run(
          [
            "im",
            "+messages-search",
            "--as",
            "user",
            "--query",
            "",
            "--start",
            new Date(Math.floor(from / 1000) * 1000).toISOString().replace(/\.\d{3}Z$/, "Z"),
            "--end",
            new Date(Math.ceil(through / 1000) * 1000).toISOString().replace(/\.\d{3}Z$/, "Z"),
            "--page-size",
            "50",
            "--format",
            "json",
            "--no-reactions",
            ...(token ? ["--page-token", token] : []),
          ],
          signal,
        ),
      );
      for (const raw of page.items) {
        const at = messageTime(raw.create_time);
        if (!Number.isFinite(at)) throw new Error("Lark message has no valid timestamp");
        if (at < from || at >= through) continue;
        const id = string(raw.message_id),
          chatId = string(raw.chat_id);
        if (!id || !chatId) throw new Error("Lark message identity missing");
        const group = grouped.get(chatId) ?? {
          chat: {
            id: chatId,
            name: string(raw.chat_name) || chatId,
            mode: string(raw.chat_type ?? raw.chat_mode),
            description: "",
          },
          messages: new Map<string, ImMessage>(),
        };
        group.messages.set(id, {
          id,
          at: new Date(at).toISOString(),
          content:
            typeof raw.content === "string"
              ? raw.content
              : string(object(raw.content).text) || JSON.stringify(raw.content ?? {}),
          sender: object(raw.sender),
          url: string(raw.message_app_link),
          deleted: raw.deleted === true,
        });
        grouped.set(chatId, group);
      }
      token = page.next;
      if (token && tokens.has(token)) throw new Error("Lark IM repeated pagination token");
      if (token) tokens.add(token);
      if (++pages >= 1000 && token) throw new Error("Lark IM pagination exceeded safe limit");
    } while (token);
    const ids = [...grouped.keys()];
    const muted = new Set<string>();
    // The user-setting endpoint accepts at most ten chat IDs per request.
    for (let index = 0; index < ids.length; index += 10) {
      const batch = ids.slice(index, index + 10);
      const data = parseCliOutput(
        await run(
          [
            "im",
            "chat.user_setting",
            "batch_query",
            "--as",
            "user",
            "--data",
            JSON.stringify({ chat_ids: batch }),
            "--format",
            "json",
          ],
          signal,
        ),
      );
      if (!Array.isArray(data.items)) throw new Error("Lark mute status missing");
      const seen = new Set<string>();
      for (const value of data.items) {
        const item = object(value),
          id = string(item.chat_id);
        if (!batch.includes(id) || seen.has(id) || typeof item.is_muted !== "boolean")
          throw new Error("Invalid Lark mute status");
        seen.add(id);
        if (item.is_muted) muted.add(id);
      }
      if (seen.size !== batch.length) throw new Error("Incomplete Lark mute status");
    }
    return [...grouped.values()]
      .filter((group) => !muted.has(group.chat.id))
      .map((group) => ({
        chat: group.chat,
        messages: [...group.messages.values()].sort((a, b) => a.at.localeCompare(b.at)),
      }));
  },
});
export type ImClient = ReturnType<typeof makeImClient>;

export class ImSearch extends Context.Service<ImSearch, ImClient>()("lark/ImSearch") {
  static readonly layer = Layer.effect(
    ImSearch,
    Effect.gen(function* () {
      const config = yield* LarkConfig;
      const profile = config.profile;
      return makeImClient((args, signal) => runLarkCli(args, profile, signal));
    }),
  );
}
