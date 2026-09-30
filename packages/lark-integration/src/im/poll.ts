import type { ImClient } from "./client.js";
import { imDate, imDayStart } from "./dates.js";
/** Startup is bounded to today; uninterrupted polling can finish yesterday's tail. */
export const pollIm = async (
  client: ImClient,
  cursor: string | undefined,
  now: number,
  signal?: AbortSignal,
  startup = true,
  sessionStart?: number,
  windowMs = 60 * 60_000,
) => {
  if (!Number.isSafeInteger(windowMs) || windowMs <= 0)
    throw new Error("Invalid IM catch-up window");
  const previous = cursor ? Date.parse(cursor) : NaN;
  const today = imDayStart(imDate(now));
  const valid = Number.isFinite(previous) && previous <= now;
  const lower = valid ? previous - Math.min(60_000, Math.floor(windowMs / 2)) : today;
  const start = new Date(
    Math.max(startup ? today : (sessionStart ?? -Infinity), lower),
  ).toISOString();
  const through = new Date(Math.min(now, Date.parse(start) + windowMs)).toISOString();
  const batches = Date.parse(start) < now ? await client.recent(start, through, signal) : [];
  return { start, through, batches, caughtUp: Date.parse(through) >= now };
};
