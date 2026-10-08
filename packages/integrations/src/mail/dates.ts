import { DateTime, Option, Schema } from "effect";
import type { MailboxWindow } from "./model.js";
export const MailDate = Schema.String.check(
  Schema.isPattern(/^\d{4}-\d{2}-\d{2}$/),
  Schema.makeFilter((value) => {
    const parsed = DateTime.make(value);
    return Option.isSome(parsed) && DateTime.formatIsoDateUtc(parsed.value) === value;
  }),
);
export const mailDay = (now: DateTime.DateTime, timeZone: string) =>
  DateTime.startOf(DateTime.setZoneNamedUnsafe(now, timeZone), "day");
export const dayWindow = (date: string, timeZone: string): MailboxWindow => {
  const start = DateTime.makeZonedUnsafe(`${date}T00:00:00Z`, {
    timeZone,
    adjustForTimeZone: true,
  });
  return {
    from: DateTime.formatIsoOffset(start),
    through: DateTime.formatIsoOffset(DateTime.add(start, { days: 1 })),
  };
};
export const inWindow = (date: string | undefined, window: MailboxWindow) =>
  date !== undefined &&
  Date.parse(date) >= Date.parse(window.from) &&
  Date.parse(date) < Date.parse(window.through);
