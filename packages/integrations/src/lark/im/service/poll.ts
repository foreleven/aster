import { Effect } from "effect";
import { ChatPollError } from "../../shared/errors.js";
import type { LarkChatService } from "./chat-service.js";
import { formatDate, dayStart } from "./dates.js";

/** Startup is bounded to today; uninterrupted polling can finish yesterday's tail. */
export const pollChats = Effect.fn("LarkChat.poll")(function* (
  chats: Pick<LarkChatService["Service"], "searchMessages">,
  cursor: string | undefined,
  now: number,
  startup = true,
  sessionStart?: number,
  windowMs = 60 * 60_000,
) {
  if (!Number.isSafeInteger(windowMs) || windowMs <= 0)
    return yield* new ChatPollError({ cause: windowMs, message: "Invalid IM catch-up window" });
  const previous = cursor ? Date.parse(cursor) : NaN;
  const today = dayStart(formatDate(now));
  const valid = Number.isFinite(previous) && previous <= now;
  const lower = valid ? previous - Math.min(60_000, Math.floor(windowMs / 2)) : today;
  const start = new Date(
    Math.max(startup ? today : (sessionStart ?? -Infinity), lower),
  ).toISOString();
  const through = new Date(Math.min(now, Date.parse(start) + windowMs)).toISOString();
  const batches =
    Date.parse(start) < now
      ? yield* chats
          .searchMessages({ start, end: through, excludeMuted: true })
          .pipe(Effect.mapError((cause) => new ChatPollError({ cause, message: cause.message })))
      : [];
  return { start, through, batches, caughtUp: Date.parse(through) >= now };
});
