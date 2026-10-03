import { DateTime, Schema } from "effect";

export const MailWindow = Schema.Struct({
  start: Schema.Number,
  through: Schema.Number,
  caughtUp: Schema.Boolean,
});
export type MailWindow = typeof MailWindow.Type;

export const mailDayStart = (now: number): number =>
  DateTime.makeUnsafe(now).pipe(
    DateTime.setZoneNamedUnsafe("Asia/Shanghai"),
    DateTime.startOf("day"),
    DateTime.toEpochMillis,
  );

/** Replay today on restart; an uninterrupted session can finish yesterday's tail. */
export const mailWindow = (
  cursor: number | undefined,
  now: number,
  sessionStart: number,
): MailWindow => {
  const previous = cursor !== undefined && cursor <= now ? cursor - 60_000 : sessionStart;
  const start = Math.max(sessionStart, previous);
  const through = Math.min(now, start + 60 * 60_000);
  return { start, through, caughtUp: through >= now };
};
